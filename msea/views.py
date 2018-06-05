import datetime
import glob
import hashlib
import json
import os
import re
import subprocess

import requests
from django.conf import settings
from django.db.models import Q
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.gzip import gzip_page

from .forms import GeneForm, StudyForm, VariantForm
from .models import *
from .models import Gene, ReferenceSequence
from .utils import generate_variant_bplot


def msea_home(request):
    form = GeneForm(request.user)
    context = {'form': form}
    return render(request, 'msea/msea_home.html', context)


@gzip_page
def download_tiff(request):
    if request.GET:
        try:
            dataset_es_index_name = request.GET.get('dataset_es_index_name')
            gene = request.GET.get('gene')
            rs_id = request.GET.get('rs_id')
            variant_selected = request.GET.get('variant_selected')
            recurrent_variant_option = request.GET.get(
                'recurrent_variant_option')

        except Exception as e:
            return HttpResponse('Missing parameter')
        # # Define command and arguments
        # # Rscript make.msea.plot.json-v12-2016.R $PWD/NM_138420_ansi_gene.json /tmp/ ansi $PWD/NM_138420_ansi_domain.json
        command = 'Rscript'
        path2script = os.path.join(
            settings.BASE_DIR, 'msea/management/commands/data/make.msea.plot.elasticsearch.R')

        # # Variable number of args in a list

        dataset_obj = Dataset.objects.get(es_index_name=dataset_es_index_name)
        output_folder = os.path.join(settings.BASE_DIR, 'msea/output_tiff/')
        output_filename = "%s_%s_%s_%s.tiff" % (
            gene, rs_id, dataset_es_index_name, variant_selected)
        output_path = os.path.join(output_folder, output_filename)

        args = [gene, rs_id, variant_selected, dataset_obj.es_index_name,
                dataset_obj.es_type_name, dataset_obj.es_host, dataset_obj.es_port, output_folder, 'tiff']

        # # Build subprocess command
        cmd = [command, path2script] + args
        # print(cmd)
        # print(' '.join(cmd))
        # # check_output will run the command and store to result

        subprocess.check_call(cmd)

        image_data = open(output_path, "rb").read()
        return HttpResponse(image_data, content_type="image/tiff")


@gzip_page
def msea_plot(request):
    if request.POST:
        gene_form = GeneForm(request.user, request.POST)
        rs_id = request.POST['rs_id']
        study = request.POST['study']
        variant_form = VariantForm(rs_id, study, request.POST)
        if gene_form.is_valid() and variant_form.is_valid():
            gene_data = gene_form.cleaned_data
            variant_data = variant_form.cleaned_data

            study_short_name = gene_data['study']
            study_obj = Study.objects.get(short_name=study_short_name)
            gene_name = gene_data['search_term']
            gene, rs_id = gene_name.split()
            rs_id = rs_id[1:-1]

            recurrent_variant_option = gene_data['recurrent_variant_option']

            variant_selected = variant_data['variant_choice']
            # print('*'*20, variant_selected)

            command = 'Rscript'
            path2script = os.path.join(
                settings.BASE_DIR, 'msea/management/commands/data/make.msea.plot.elasticsearch.R')

            # # Variable number of args in a list

            if settings.DEBUG:
                output_folder = os.path.join(
                    settings.BASE_DIR, 'static/r_outputs/svg/')
            else:
                output_folder = os.path.join(
                    settings.BASE_DIR, 'static_root/r_outputs/svg/')
            wildcardstring = '%s_%s_*_%s.svg' % (
                gene, rs_id, variant_selected)

            for dataset in Dataset.objects.filter(study=study_obj, es_index_name__contains='_'+recurrent_variant_option):
                args = [gene, rs_id, variant_selected,
                        dataset.es_index_name, dataset.es_type_name, dataset.es_host, dataset.es_port, output_folder, 'svg']

                # Build subprocess command
                cmd = [command, path2script] + args
                print(' '.join(cmd))

                # only create plot if plot doesn't already exist
                filename = '%s%s_%s_%s_%s.svg' % (
                    output_folder, gene, rs_id, dataset.es_type_name, variant_selected)
                if not os.path.isfile(filename):
                    # Run Rscript
                    subprocess.check_call(cmd)

            svg_files = glob.glob(os.path.join(output_folder, wildcardstring))
            wildcardstring = '%s_%s_(\S+)_%s' % (gene, rs_id,
                                                 variant_selected)
            plots = []
            # hacky way to get 'proper' display order for SIM study plots
            for file in sorted(svg_files, key=len):
                # print(file)
                filename = os.path.basename(file)
                tmp = re.search(wildcardstring, filename).groups()[0]
                dataset_obj = Dataset.objects.get(es_index_name=tmp)

                plots.append((gene, rs_id, variant_selected, recurrent_variant_option,
                              dataset_obj.es_index_name, dataset_obj.display_name, study_obj.id, filename))
            # print(plots)
            context = {}
            context['plots'] = plots

            return render(request, 'msea/msea_plot.html', context)


def search_gene_rs_id(request):
    q = request.GET.get('q', None)
    study = request.GET.get('study', None)
    if q and study:
        if len(q) > 0:
            genes = Gene.objects.filter(gene_name__icontains=q)
            reference_sequences = ReferenceSequence.objects.filter(study__short_name=study).filter(
                Q(rs_id__icontains=q) | Q(gene__gene_name__icontains=q))
            results = ['%s (%s)' % (ele.gene.gene_name, ele.rs_id)
                       for ele in reference_sequences]

            json_response = {"data": results}
            return JsonResponse(json_response)
        else:
            json_response = {"errors":
                             [{"status": "400",
                               "detail": "Search string must be greater than 0 character"}
                              ]}
            return JsonResponse(json_response)
    else:

        json_response = {"errors": [{"status": "400",
                                     "detail": "Search string missing"
                                     }
                                    ]}
        return JsonResponse(json_response)


def get_variant_form(request):
    rs_id = request.GET['selected_rs_id']
    study = request.GET['study']
    variant_form = VariantForm(rs_id, study)
    context = {'variant_form': variant_form,
               'rs_id': rs_id}
    # return HttpResponse(attribute_forms)
    return render(request, "msea/get_variant_snippet.html", context)


def get_top100_nes(index, doc_type, es_host, es_port, size=100):
    query_string = """
    {
        "_source": ["nes", "pvalue", "refgene_id", "vset", "gene_name"],
        "sort" : [
            { "nes" : {"order" : "desc"}}
        ],
        "size": %s
    }
    """

    url = "http://%s:%s/%s/%s/_search?pretty" % (
        es_host, es_port, index, doc_type)
    response = requests.get(url, data=query_string % (
        size), headers={'Content-type': 'application/json'})
    tmp = json.loads(response.text)
    tmp = tmp['hits']['hits']
    results = []
    for ele in tmp:
        tmp_source = ele['_source']
        results.append(tmp_source)

    return results


def msea_pvalue(request):

    study_form = StudyForm(request.user)

    context = {}
    context['study_form'] = study_form

    return render(request, 'msea/msea_pvalue.html', context)


def msea_get_sorted_pvalues(request):
    from pprint import pprint

    study_short_name = request.GET.get('selected_study')
    study_obj = Study.objects.get(short_name=study_short_name)

    results = []
    for dataset in Dataset.objects.filter(study=study_obj, es_index_name__contains="_noexpand"):
        result = get_top100_nes(
            dataset.es_index_name, dataset.es_type_name, dataset.es_host, dataset.es_port, size=100)
        results.append((dataset.es_index_name, dataset.display_name, result))

    context = {}
    context['results'] = results
    return render(request, 'msea/msea_get_pvalues.html', context)


@gzip_page
def get_plot(request):
    dataset_es_index_name = request.GET.get('dataset_es_index_name')
    gene = request.GET.get('gene')
    rs_id = request.GET.get('rs_id')
    variant_selected = request.GET.get('variant_selected')

    recurrent_variant_option = 'noexpand'
    command = 'Rscript'
    path2script = os.path.join(
        settings.BASE_DIR, 'msea/management/commands/data/make.msea.plot.elasticsearch.R')

    # # Variable number of args in a list

    dataset_obj = Dataset.objects.get(es_index_name=dataset_es_index_name)
    if settings.DEBUG:
        output_folder = os.path.join(
            settings.BASE_DIR, 'static/r_outputs/svg/')
    else:
        output_folder = os.path.join(
            settings.BASE_DIR, 'static_root/r_outputs/svg/')
    output_filename = "%s_%s_%s_%s.svg" % (
        gene, rs_id, dataset_es_index_name, variant_selected)
    output_path = os.path.join(output_folder, output_filename)

    args = [gene, rs_id, variant_selected,
            dataset_obj.es_index_name, dataset_obj.es_type_name, dataset_obj.es_host, dataset_obj.es_port, output_folder, 'svg']

    # # Build subprocess command
    cmd = [command, path2script] + args
    # print(cmd)
    print(' '.join(cmd))
    # # check_output will run the command and store to result

    # only create plot if plot doesn't already exist
    if not os.path.isfile(output_path):
        # Run Rscript
        subprocess.check_call(cmd)

    context = {}
    context['plot_path'] = output_filename


    context['dataset_display_name'] = dataset_obj.display_name

    # The following are used to generate the 'download as tiff' link
    context['gene'] = gene
    context['rs_id'] = rs_id
    context['variant_selected'] = variant_selected
    context['recurrent_variant_option'] = recurrent_variant_option
    context['dataset_es_index_name'] = dataset_es_index_name

    return render(request, 'msea/msea_get_plot.html', context)
