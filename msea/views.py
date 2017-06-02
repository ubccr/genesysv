from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.db.models import Q

import subprocess
import datetime
import hashlib
import datetime
import os
import re
import requests
import json
import glob

from .models import *
from .utils import generate_variant_bplot
from django.views.decorators.gzip import gzip_page
from .forms import GeneForm, VariantForm
from .models import Gene, ReferenceSequence
from django.conf import settings
# Create your views here.

# args <- commandArgs(trailingOnly = T)
# gene <- args[1]
# rs_id <- args[2]
# vset <- args[3]
# msea_type_name <- args[4]
# es_host <- args[5]
# es_port <- args[6]
# output_folder <- args[7]
# output_type <- args[8]


@gzip_page
def download_svg(request):
    if request.GET:
        print(request.GET)
        try:
            dataset = request.GET.get('dataset')
            gene = request.GET.get('gene')
            rs_id = request.GET.get('rs_id')
            vset = request.GET.get('vset')
            expand = request.GET.get('expand')
        except:
            return HttpResponse('Missing parameter')
        # # Define command and arguments
        # # Rscript make.msea.plot.json-v12-2016.R $PWD/NM_138420_ansi_gene.json /tmp/ ansi $PWD/NM_138420_ansi_domain.json
        command = 'Rscript'
        path2script = os.path.join(settings.BASE_DIR, 'msea/management/commands/data/make.msea.plot.elasticsearch.R')

        # # Variable number of args in a list

        dataset_obj = MseaDataset.objects.get(dataset=dataset)
        output_folder = os.path.join(settings.BASE_DIR, 'msea/output_svg/')
        output_filename = "%s-%s-%s.svg" %(gene, rs_id, vset)
        output_path = os.path.join(output_folder, output_filename)


        args = [gene , rs_id, vset, expand, dataset_obj.es_host, dataset_obj.es_port]

        # # Build subprocess command
        cmd = [command, path2script] + args
        # print(cmd)
        print(' '.join(cmd))
        # # check_output will run the command and store to result

        subprocess.check_call(cmd)

        svg_data = open(output_path,'r')
        response = HttpResponse(svg_data, content_type="image/svg+xml")
        response["Content-Disposition"]= "attachment; filename=%s" %(output_filename)
        return response
        # return HttpResponse('test')

@gzip_page
def msea_home(request):
    form = GeneForm(request.user)
    context = {'form':form}
    return render(request, 'msea/msea_home.html', context)

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

            variants_selected = variant_data['variant_choices']


            command = 'Rscript'
            path2script = os.path.join(settings.BASE_DIR, 'msea/management/commands/data/make.msea.plot.elasticsearch.R')

            # # Variable number of args in a list

            output_folder = os.path.join(settings.BASE_DIR, 'static/r_outputs/svg/')
            wildcardstring = '%s_%s_*_%s_%s.svg' %(gene, rs_id, recurrent_variant_option, variants_selected)



            for dataset in Dataset.objects.filter(study=study_obj):
                args = [gene , rs_id, variants_selected, '%s_%s' %(dataset.short_name, recurrent_variant_option), study_obj.es_host, study_obj.es_port, output_folder, 'SVG']

            # # Build subprocess command
                cmd = [command, path2script] + args
                # print(cmd)
                print(' '.join(cmd))
                # # check_output will run the command and store to result

                ### Run Rscript
                subprocess.check_call(cmd)

            svg_files = glob.glob(os.path.join(output_folder, wildcardstring))
            wildcardstring = '%s_%s_(\S+)_%s_%s' %(gene, rs_id, recurrent_variant_option, variants_selected)
            plots = []
            for file in svg_files:
                filename = os.path.basename(file)
                tmp = re.search(wildcardstring, filename).groups()[0]
                dataset_obj = Dataset.objects.get(study=study_obj, short_name=tmp)

                plots.append((gene, rs_id, variants_selected, filename, dataset_obj.display_name))
            # print(plots)
            context = {}
            context['plots'] = plots


            return render(request, 'msea/msea_plot.html', context)


@gzip_page
def search_gene_rs_id(request):
    q = request.GET.get('q', None)
    study = request.GET.get('study', None)
    if q and study:
        if len(q) > 0:
            genes = Gene.objects.filter(gene_name__icontains=q)
            reference_sequences = ReferenceSequence.objects.filter(study__short_name=study).filter(Q(rs_id__icontains=q) | Q(gene__gene_name__icontains=q))
            results = ['%s (%s)' %(ele.gene.gene_name, ele.rs_id) for ele in reference_sequences]

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

@gzip_page
def get_variant_form(request):
    rs_id = request.GET['selected_rs_id']
    study = request.GET['study']
    variant_form = VariantForm(rs_id, study)
    context = {'variant_form':variant_form,
               'rs_id': rs_id}
    # return HttpResponse(attribute_forms)
    return render(request, "msea/get_variant_snippet.html", context)


def get_top100_nes(index, doc_type, size=100):
    query_string = """
    {
        "_source": ["nes", "pvalue", "refgene_id", "vset", "gene_name"],
        "sort" : [
            { "nes" : {"order" : "desc"}}
        ],
        "size": %s
    }
    """

    url = "http://199.109.195.45:9200/%s/%s/_search?pretty" %(index, doc_type)

    response = requests.get(url, data = query_string %(size))
    tmp = json.loads(response.text)
    tmp = tmp['hits']['hits']
    results = []
    for ele in tmp:
        tmp_source = ele['_source']
        results.append(tmp_source)

    return results

def msea_pvalue(request):
    index = 'msea'
    doc_type = 'sim_sen_noexpand'
    type_name = 'noexpand'
    results = get_top100_nes(index, doc_type, size=100)
    context = {}
    context['results'] = results
    context['index'] = index
    context['doc_type'] = doc_type
    context['type_name'] = type_name
    return render(request, 'msea/msea_pvalue.html', context)



@gzip_page
def get_plot(request):
    params = request.GET
    msea_type_name = params.get('type_name')
    gene = params.get('gene')
    rs_id = params.get('rs_id')
    vset = params.get('vset')

    plot_path = generate_variant_bplot(msea_type_name, gene, rs_id, vset)
    plot_path = os.path.basename(plot_path)
    context = {}
    context['plot_path'] = plot_path

    return render(request, 'msea/msea_get_plot.html', context)

