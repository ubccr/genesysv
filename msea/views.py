from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.db.models import Q

import datetime
import hashlib
import datetime
import os
import re


from django.views.decorators.gzip import gzip_page
from .forms import GeneForm, VariantForm
from .models import Gene, ReferenceSequence
from django.conf import settings
# Create your views here.


def generate_r_plot(rdata_filename, output_folder, variants_to_generate):
    # Define command and arguments
    command = 'Rscript'
    path2script = os.path.join(settings.BASE_DIR, 'msea/management/commands/data/make.msea.plot.bigdw.R')

    # Variable number of args in a list
    args = [os.path.join(settings.BASE_DIR, 'msea/output/%s' %(rdata_filename)) , output_folder, variants_to_generate]

    # Build subprocess command
    cmd = [command, path2script] + args
    print(cmd)
    print(' '.join(cmd))
    # check_output will run the command and store to result
    import subprocess
    subprocess.check_call(cmd)


def msea_home(request):
    form = GeneForm()
    context = {'form':form}
    return render(request, 'msea/msea_home.html', context)

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
            expand_option = gene_data['expand_option']
            variants_selected = variant_data['variant_choices']
            variants_selected = ','.join(variants_selected)

            print(gene_name, expand_option, variants_selected)
            # return HttpResponse(variants_selected)
            # SIM_variants_step2_vartype_db_2016-05-18_noexpand_Data4plot.RData
            output_string = ""


            m = re.search('^[^\(]+', rs_id)

            gene = m.group(0).strip()
            m = re.search('\((.+)\)', rs_id)
            rs_id = m.group(1)
            rdata_filename = "%s_%s_%s_%s_%s" %(project_name,
                                                    dataset_date,
                                                    expand_option,
                                                    rs_id,
                                                    end_string)



            output_folder = os.path.join(settings.BASE_DIR, 'static_root/rplots')

            files = []
            variants_to_generate = []
            for variant in variant_data['variant_choices']:
                filename = gene + '-' + rs_id + '-' + expand_option + '-' + variant + '.svg'
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


def search_gene_rs_id(request):
    q = request.GET.get('q', None)
    if q:
        if len(q) > 0:
            genes = Gene.objects.filter(gene_name__icontains=q)
            reference_sequences = ReferenceSequence.objects.filter(Q(rs_id__icontains=q) | Q(gene__gene_name__icontains=q))
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


def get_variant_form(request):
    rs_id = request.GET['selected_rs_id']
    variant_form = VariantForm(rs_id)
    context = {'variant_form':variant_form,
               'rs_id': rs_id}
    # return HttpResponse(attribute_forms)
    return render(request, "msea/get_variant_snippet.html", context)
