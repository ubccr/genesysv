from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.db.models import Q

import subprocess
import datetime
import hashlib
import datetime
import os
import re

from .utils import generate_variant_bplot
from django.views.decorators.gzip import gzip_page
from .forms import GeneForm, VariantForm
from .models import Gene, ReferenceSequence
from django.conf import settings
# Create your views here.


@gzip_page
def download_svg(request):
    if request.GET:
        print(request.GET)
        try:
            dataset = request.GET.get('dataset')
            gene = request.GET.get('gene')
            rs_id = request.GET.get('rs_id')
            vset = request.GET.get('vset')
        except:
            return HttpResponse('Missing parameter')
        # # Define command and arguments
        # # Rscript make.msea.plot.json-v12-2016.R $PWD/NM_138420_ansi_gene.json /tmp/ ansi $PWD/NM_138420_ansi_domain.json
        command = 'Rscript'
        path2script = os.path.join(settings.BASE_DIR, 'msea/management/commands/data/make.msea.plot.json-v12-2016.R')

        # # Variable number of args in a list
        output_json_path = os.path.join(settings.BASE_DIR, 'msea/output_json')


        gene_filename = os.path.join(output_json_path, "%s_%s_gene.json" %(rs_id, vset))
        domain_filename = os.path.join(output_json_path, "%s_%s_domain.json" %(rs_id, vset))

        output_folder = os.path.join(settings.BASE_DIR, 'msea/output_svg/')
        output_filename = "%s-%s-%s.svg" %(gene, rs_id, vset)
        output_path = os.path.join(output_folder, output_filename)

        args = [gene_filename , output_folder, vset, domain_filename]

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
def bokeh_plot(request):
    if request.POST:
        gene_form = GeneForm(request.user, request.POST)
        rs_id = request.POST['rs_id']
        dataset = request.POST['dataset']
        variant_form = VariantForm(rs_id, dataset, request.POST)
        if gene_form.is_valid() and variant_form.is_valid():
            gene_data = gene_form.cleaned_data
            variant_data = variant_form.cleaned_data

            dataset = gene_data['dataset']
            gene_name = gene_data['search_term']
            gene, rs_id = gene_name.split()
            rs_id = rs_id[1:-1]

            recurrent_variant_option = gene_data['recurrent_variant_option']
            variants_selected = variant_data['variant_choices']
            # variants_selected = ','.join(variants_selected)

            msea_type_name = "%s_%s" %(dataset, recurrent_variant_option)
            plots = []
            for vset in variants_selected:
                plot_path = generate_variant_bplot(msea_type_name, gene, rs_id, vset)
                plots.append((gene, rs_id, vset, os.path.basename(plot_path)))
            # print(plots)
            context = {}
            context['plots'] = plots
            context['dataset'] = dataset

            return render(request, 'msea/msea_bokeh_plot.html', context)

@gzip_page
def plots(request):
    project_name = "SIM_variants_step2_vartype_db"
    dataset_date = "2016-05-25"
    end_string = "Data4plot.RData"
    if request.POST:
        gene_form = GeneForm(request.POST)
        rs_id = request.POST['rs_id']
        variant_form = VariantForm(rs_id, request.POST)
        if gene_form.is_valid() and variant_form.is_valid():
            gene_data = gene_form.cleaned_data
            variant_data = variant_form.cleaned_data

            gene_name = gene_data['search_term']
            recurrent_variant_option = gene_data['recurrent_variant_option']
            variants_selected = variant_data['variant_choices']
            variants_selected = ','.join(variants_selected)

            print(gene_name, recurrent_variant_option, variants_selected)
            # return HttpResponse(variants_selected)
            # SIM_variants_step2_vartype_db_2016-05-18_noexpand_Data4plot.RData
            output_string = ""


            m = re.search('^[^\(]+', rs_id)

            gene = m.group(0).strip()
            m = re.search('\((.+)\)', rs_id)
            rs_id = m.group(1)
            rdata_filename = "%s_%s_%s_%s_%s" %(project_name,
                                                    dataset_date,
                                                    recurrent_variant_option,
                                                    rs_id,
                                                    end_string)



            output_folder = os.path.join(settings.BASE_DIR, 'static_root/rplots')

            files = []
            variants_to_generate = []
            for variant in variant_data['variant_choices']:
                filename = gene + '-' + rs_id + '-' + recurrent_variant_option + '-' + variant + '.svg'
                full_path = os.path.join(output_folder, filename)
                files.append(filename)
                if not os.path.exists(os.path.join(output_folder,filename)):
                    print('New Plot: ', full_path)
                    variants_to_generate.append(variant)


            variants_to_generate = ','.join(variants_to_generate)
            if variants_to_generate:
                generate_r_plot(rdata_filename, output_folder, variants_to_generate)

            context = {}
            context['files'] = files



            return render(request, 'msea/msea_plot.html', context)

@gzip_page
def search_gene_rs_id(request):
    q = request.GET.get('q', None)
    dataset = request.GET.get('dataset', None)
    if q and dataset:
        if len(q) > 0:
            genes = Gene.objects.filter(gene_name__icontains=q)
            reference_sequences = ReferenceSequence.objects.filter(msea_dataset__dataset=dataset).filter(Q(rs_id__icontains=q) | Q(gene__gene_name__icontains=q))
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
    dataset = request.GET['dataset']
    variant_form = VariantForm(rs_id, dataset)
    context = {'variant_form':variant_form,
               'rs_id': rs_id}
    # return HttpResponse(attribute_forms)
    return render(request, "msea/get_variant_snippet.html", context)
