from django.shortcuts import render, redirect
from django.http import HttpResponse, HttpResponseNotFound
from core.models import Dataset
import elasticsearch
from core.utils import get_es_document
from .forms import SampleSelectForm
from .models import SampleBamInfo, AnnotationReference
from django.conf import settings
from .utils import generate_url
import json
from pprint import pprint
import time


def get_sample_ids(result):
    return sorted([(ele['sample_ID']) for ele in result['sample'] if ele['sample_GT'] not in ['0/0', './.']])


def igvview(request):
    if request.POST:
        if 'igvredirect' in request.POST:
            dataset_id = request.POST.get('dataset_id')
            Chr = request.POST.get('Chr').replace('chr','')
            Start = int(request.POST.get('Start'))
            dataset = Dataset.objects.get(id=dataset_id)

            try:
                locus = '"chr%s:%d-%d"' % (Chr, Start - 50, Start + 50)
                bam_files = []
                for sample in (key for key, val in request.POST.items() if 'on' in val):
                    sample_bam_info_obj = SampleBamInfo.objects.get(
                        dataset=dataset, sample_id=sample)
                    path = sample_bam_info_obj.file_path
                    url = generate_url(path)
                    url = sample_bam_info_obj.bam_server + url
                    bai = generate_url(path + '.bai')
                    bai = sample_bam_info_obj.bam_server + bai
                    name = sample
                    tmp = {'url': url, 'name': name, 'bai': bai}
                    bam_files.append(tmp)

                annotation_reference_obj = AnnotationReference.objects.get(dataset=dataset)
                context = {}
                annotation = {}
                reference = {}
                annotation['name'] = annotation_reference_obj.annotation_name
                annotation['format'] = annotation_reference_obj.annotation_file_format
                annotation['url'] = annotation_reference_obj.annotation_url
                annotation['index_url'] = annotation_reference_obj.annotation_index_url
                reference['genome_id'] = annotation_reference_obj.genome_id
                reference['fasta_url'] = annotation_reference_obj.reference_fasta_url
                reference['cytoband_url'] = annotation_reference_obj.reference_cytoband_url
                context['annotation'] = annotation
                context['reference'] = reference
                context['bam_files'] = bam_files
                context['locus'] = locus
                return render(request, "igv/igv_lite.html", context)
            except Exception as e:
                print(e)
                return HttpResponse('Missing BAM file for: %s' % (sample))

        else:
            dataset_id = request.POST.get('dataset_id')
            dataset = Dataset.objects.get(id=dataset_id)
            variant_id = request.POST.get('variant_id')
            index_name = request.POST.get('index_name')
            type_name = request.POST.get('type_name')
            es = elasticsearch.Elasticsearch(host=dataset.es_host)
            result = get_es_document(es, index_name, type_name, variant_id)
            sample_ids = get_sample_ids(result)

            sample_select_form = SampleSelectForm(sample_ids)
            context = {}
            context['Variant'] = result['Variant']
            context['dataset_id'] = dataset_id
            context['variant_id'] = variant_id
            context['Chr'] = result['CHROM']
            context['Start'] = result['POS']
            context['sample_ids'] = sample_ids
            context['sample_select_form'] = sample_select_form
            return render(request, 'igv/igvview.html', context)
            # return HttpResponse(result)

    else:
        return HttpResponseNotFound()
