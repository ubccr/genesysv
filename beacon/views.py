import elasticsearch
import requests
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import HttpResponse, render

from search.models import Dataset

from .forms import BeaconQueryForm


def beacon(request):
    context = {}
    return render(request, "beacon/beacon.html", context)


def get_beacon_form(request):
    context = {}
    beacon_query_form = BeaconQueryForm()
    context['beacon_query_form'] = beacon_query_form
    return render(request, "beacon/get_beacon_form_snippet.html", context)


def beacon_query(request):
    form = BeaconQueryForm(request.GET or None)
    if request.method == 'GET' and form.is_valid():
        data = form.cleaned_data

        ips = list(set([ele.es_host for ele in Dataset.objects.all()]))
        for ip in ips:
            es = elasticsearch.Elasticsearch(host=ip)
            beacon_query_template = """
                {
                    "query": {
                        "bool": {
                            "filter": [
                                {"terms": {"CHROM": ["%s", "chr%s"]}},
                                {"term": {"ALT": "%s"}},
                                {"term": {"POS": "%s"}}
                            ]
                        }
                    }
                }
            """
            body = beacon_query_template % (data['chromosome'], data['chromosome'], data[
                                            'alternate_allele'], data['coordinate'])
            results = es.search(index='_all', body=body)

            if results['hits']['total'] >= 1:
                exists = True
                break
            else:
                exists = False
        context = {}
        context['beacon_query_form'] = form
        context['exists'] = exists

        return render(request, "beacon/get_beacon_form_snippet.html", context, status=200)

    else:
        context = {}
        context['beacon_query_form'] = form
        return render(request, "beacon/get_beacon_form_snippet.html", context, status=400)
