from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.gzip import gzip_page
from datetime import datetime
from functools import lru_cache
from django.http import QueryDict
from .utils import ElasticSearchFilter
from .forms import ESFilterForm, ESFilterFormPart, ESAttributeFormPart, ESAttributeForm
import requests
import json
from .models import *
from .forms import *
from pprint import pprint
import time
from operator import itemgetter, attrgetter, methodcaller
import itertools

def filter_array_dicts(array, key, values):
    output = []
    for ele in array:
        tmp = ele.get(key)
        if tmp in values:
            output.append(ele)

    return output

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z


def subset_dict(input, keys):
    return {key:input[key] for key in keys}


@gzip_page
def get_study_form(request):
    form = StudyForm(request.user)
    context = {'form':form}
    return render(request, "search/get_study_snippet.html", context)

@gzip_page
def get_dataset_form(request):
    selected_study = request.GET['selected_study']
    form = DatasetForm(selected_study, request.user)
    context = {'form':form}
    return render(request, "search/get_dataset_snippet.html", context)

@gzip_page
def get_filter_form(request):
    tabs = []
    for tab in FilterTab.objects.all():
        tmp_dict = {}
        tmp_dict['name'] = tab.name
        tmp_dict['panels'] = []
        for panel in FilterPanel.objects.filter(filter_tab=tab):
            es_form = ESFilterFormPart(panel.filter_fields.all(), prefix='filter_')

            sub_panels = []
            for sub_panel in panel.filtersubpanel_set.all():
                tmp_sub_panel_dict = {}
                tmp_sub_panel_dict['display_name'] = sub_panel.name
                tmp_sub_panel_dict['name'] = ''.join(sub_panel.name.split()).lower()
                tmp_sub_panel_dict['form'] = ESFilterFormPart(sub_panel.filter_fields.all(), prefix='filter_')
                sub_panels.append(tmp_sub_panel_dict)

            tmp_dict['panels'].append({'display_name': panel.name,
                                      'name': ''.join(panel.name.split()).lower(),
                                      'form': es_form,
                                      'sub_panels': sub_panels })
        tabs.append(tmp_dict)

    context = {}
    context['tabs'] = tabs
    # print(context)
    return render(request, "search/get_filter_snippet.html", context)

@gzip_page
def get_attribute_form(request):
    tabs = []
    for tab in AttributeTab.objects.all():
        tmp_dict = {}
        tmp_dict['name'] = tab.name
        tmp_dict['panels'] = []
        for panel in AttributePanel.objects.filter(attribute_tab=tab):
            es_form = ESAttributeFormPart(panel.attribute_fields.all(), prefix='attribute_group')

            sub_panels = []
            for sub_panel in panel.attributesubpanel_set.all():
                tmp_sub_panel_dict = {}
                tmp_sub_panel_dict['display_name'] = sub_panel.name
                tmp_sub_panel_dict['name'] = ''.join(sub_panel.name.split()).lower()
                tmp_sub_panel_dict['form'] = ESAttributeFormPart(sub_panel.attribute_fields.all(), prefix='attribute_group')
                sub_panels.append(tmp_sub_panel_dict)

            tmp_dict['panels'].append({'display_name': panel.name,
                                      'name': ''.join(panel.name.split()).lower(),
                                      'form': es_form,
                                      'sub_panels': sub_panels })
        tabs.append(tmp_dict)

    context = {}
    context['tabs'] = tabs
    print(context)
    return render(request, "search/get_attribute_snippet.html", context)


@gzip_page
def search_home(request):
    context = {}
    return render(request, 'search/search.html', context)

@gzip_page
def search_old_home(request):
    tabs = []
    for tab in Tab.objects.all():
        tmp_dict = {}
        tmp_dict['name'] = tab.name
        tmp_dict['panels'] = []
        for panel in Panel.objects.filter(tab=tab):
            es_form = ESFormPart(panel.filter_fields.all())

            sub_form = []
            for sub_panel in panel.subpanel_set.all():
                tmp_sub_panel_dict = {}
                tmp_sub_panel_dict['display_name'] = sub_panel.name
                tmp_sub_panel_dict['name'] = ''.join(sub_panel.name.split()).lower()
                tmp_sub_panel_dict['form'] = ESFormPart(sub_panel.filter_fields.all())
                sub_form.append(tmp_sub_panel_dict)

            tmp_dict['panels'].append({'display_name': panel.name,
                                      'name': ''.join(panel.name.split()).lower(),
                                      'form': es_form,
                                      'sub_form': sub_form })
        tabs.append(tmp_dict)

    print(tabs)
    context = {}
    context['tabs'] = tabs
    return render(request, 'search/search_old_home.html', context)


def search_home2(request):
    es_form = ESForm()
    context = {}
    context['es_form'] = es_form
    return render(request, 'search/search_home.html', context)


@gzip_page
def search_result(request):

    if request.POST:
        start_time = datetime.now()
        attribute_order = json.loads(request.POST['attribute_order'])
        POST_data = QueryDict(request.POST['form_data'])
        es_filter_form = ESFilterForm(POST_data, prefix='filter_')
        es_attribute_form = ESAttributeForm(POST_data, prefix='attribute_group')
        # print(es_attribute_form)
        if es_filter_form.is_valid() and es_attribute_form.is_valid():
            es_filter = ElasticSearchFilter()

            es_filter_form_data = es_filter_form.cleaned_data
            es_attribute_form_data = es_attribute_form.cleaned_data


            attributes = []
            non_nested = []
            nested = []
            for key, val in es_attribute_form.cleaned_data.items():
                if val:
                    es_name, path = key.split('-')
                    if path:
                        attributes.append(path)
                        nested.append(path)
                    else:
                        attributes.append(es_name)
                        non_nested.append(es_name)

            nested = list(set(nested))
            terminate = True if request.POST.get('terminate') else False
            terminate = True

            keys = es_filter_form_data.keys()
            used_keys = []
            for key, es_name, es_filter_type, path in [ (ele, ele.split('-')[0], ele.split('-')[1], ele.split('-')[2]) for ele in keys ]:

                data = es_filter_form_data[key]

                if not data:
                    continue
                used_keys.append((key.split('-')[0], data))
                # print(key, es_name, es_filter_type, es_form_data[key], type(es_form_data[key]))
                field_obj = FilterField.objects.get(es_name=es_name, es_filter_type__name=es_filter_type, path=path)
                if es_filter_type in 'must_term':
                    if isinstance(data, list):
                        for ele in data:
                            es_filter.add_must_term(es_name, ele)
                    else:
                        es_filter.add_must_term(es_name, data)
                elif es_filter_type in 'should_term' and isinstance(data, str):
                    for ele in data.split('\n'):
                        es_filter.add_should_term(es_name, ele.strip())
                elif es_filter_type in 'should_term' and isinstance(data, list):
                    for ele in data:
                        es_filter.add_should_term(es_name, ele)
                elif es_filter_type in 'nested_must_term' and isinstance(data, str):
                    for ele in data.split('\n'):
                        print(es_name, ele.strip(), field_obj.path)
                        es_filter.add_nested_must_term(es_name, ele.strip(), field_obj.path)
                elif es_filter_type in 'nested_must_term' and isinstance(data, list):
                    for ele in data:
                        es_filter.add_nested_must_term(es_name, ele, field_obj.path)
                elif es_filter_type in 'nested_should_term' and isinstance(data, str):
                    for ele in data.split('\n'):
                        print(es_name, ele.strip(), field_obj.path)
                        es_filter.add_nested_should_term(es_name, ele.strip(), field_obj.path)
                elif es_filter_type in 'nested_should_term' and isinstance(data, list):
                    for ele in data:
                        print(ele)
                        es_filter.add_nested_should_term(es_name, ele, field_obj.path)
                elif es_filter_type in 'must_range_gte':
                    es_filter.add_must_range_gte(es_name, data)
                elif es_filter_type in 'must_range_lte':
                    es_filter.add_must_range_lte(es_name, data)

                for field in attributes:
                    es_filter.add_source(field)


            content =  es_filter.generate_query_string()

            content_generate_time = datetime.now() - start_time
            query = json.dumps(content)
            # pprint(query)
            if terminate:
                uri = 'http://199.109.195.45:9200/sim3/wgs_hg19_multianno/_search?terminate_after=80'
            else:
                uri = 'http://199.109.195.45:9200/sim3/wgs_hg19_multianno/_search?'
            response = requests.get(uri, data=query)
            results = json.loads(response.text)

            # results = es.search(index=INDEX_NAME, body=content)


            start_after_results_time = datetime.now()
            total = results['hits']['total']
            took = results['took']
            context = {}
            headers = []


            for key, val in attribute_order.items():
                # print('Hello,', key,val)
                order, es_name, path = val.split('-')
                headers.append((int(order), es_name))

            # print(attribute_order)
            headers = sorted(headers, key=itemgetter(0))
            _, headers  = zip(*headers)

            tmp_results = results['hits']['hits']
            results = []
            for ele in tmp_results:
                results.append(ele['_source'])

            final_results = []
            for result in results:


                if result['refGene']:
                    tmp_non_nested = subset_dict(result, non_nested)
                    tmp_nested = subset_dict(result, ['refGene',])
                    tmp_output = list(itertools.product([tmp_non_nested,], tmp_nested['refGene']))
                    for x,y in tmp_output:
                        tmp = merge_two_dicts(x,y)
                        final_results.append(tmp)

            print(final_results)

            context['used_keys'] = used_keys
            context['took'] = took
            context['total'] = total
            context['results'] = results
            context['headers'] = headers
            context['content_generate_time'] = content_generate_time
            context['after_results_time'] = datetime.now() - start_after_results_time
            context['total_time'] = datetime.now() - start_time
            return render(request, 'search/search_results.html', context)
