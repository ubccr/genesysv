from django.shortcuts import render, HttpResponse
from .forms import BeaconQueryForm
from django.http import JsonResponse
import requests
from django.conf import settings

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
        print('beacon', data)
        s = "http://199.109.195.45:9200/_search?q=Chr:%s&q=Alt:%s&q=Start:%s&pretty=true" %(data['chromosome'],
                                                                                            data['alternate_allele'],
                                                                                            data['coordinate']
                                                                                            )
        r = requests.get(s)
        print(r.json)
        if r.json()['hits']['total'] >= 1:
            exists = True
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

