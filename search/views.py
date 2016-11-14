from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.gzip import gzip_page
from django.http import StreamingHttpResponse
from datetime import datetime
from functools import lru_cache
from django.http import QueryDict
from .utils import ElasticSearchFilter
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from .forms import ESFilterForm, ESFilterFormPart, ESAttributeFormPart, ESAttributeForm
import requests
import json
from .models import *
from .forms import *
from pprint import pprint
import time
from operator import itemgetter, attrgetter, methodcaller
import itertools
from django.core import serializers
import csv
from .utils import get_es_result
import elasticsearch


def compare_array_dictionaries(array_dict1, array_dict2):
    if len(array_dict1) != len(array_dict2):
        return False

    compare_results = []
    for ele1 in array_dict1:
        status = False
        for ele2 in array_dict2:
            if ele2.__eq__(ele1):
                status = True
                break
        compare_results.append(status)


    return True if all(compare_results) else False

def filter_dicts(array, key, values):
    output = []
    for ele in array:
        tmp = ele.get(key)
        if tmp in values:
            output.append(ele)

def filter_array_dicts(array, key, values, comparison_type):
    output = []
    # print(values)
    for ele in array:
        tmp = ele.get(key)
        if not tmp:
            continue
        # print(tmp)
        for val in values:
            if comparison_type == "lt":
                if float(tmp) < float(val):
                    output.append(ele)

            elif comparison_type == "lte":
                if float(tmp) <= float(val):
                    output.append(ele)

            elif comparison_type == "gt":
                if float(tmp) > float(val):
                    output.append(ele)

            elif comparison_type == "gte":
                if float(tmp) >= float(val):
                    output.append(ele)

            elif comparison_type == "equal":
                if tmp == val:
                    output.append(ele)
            elif comparison_type == "default":
                for ele_tmp in tmp.split('_'):
                    if val.lower() in ele_tmp.lower():
                        output.append(ele)
                        break
            else:
                if val in tmp:
                    output.append(ele)

    return output

def merge_two_dicts_array(input):
    output = []
    for x, y in input:
        output.append(merge_two_dicts(x, y))

    return output

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z


def subset_dict(input, keys):

    return {key:input[key] for key in keys if input.get(key)}


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
                if panel.are_sub_panels_mutually_exclusive:
                    MEgroup = "MEgroup_%d_%d" %(panel.id, sub_panel.id)
                else:
                    MEgroup = None
                tmp_sub_panel_dict = {}
                tmp_sub_panel_dict['display_name'] = sub_panel.name
                tmp_sub_panel_dict['name'] = ''.join(sub_panel.name.split()).lower()
                tmp_sub_panel_dict['form'] = ESFilterFormPart(sub_panel.filter_fields.all(), MEgroup, prefix='filter_')
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
        for idx_panel, panel in enumerate(AttributePanel.objects.filter(attribute_tab=tab), start=1):
            if panel.attribute_fields.all():
                es_form = ESAttributeFormPart(panel.attribute_fields.all(), prefix='%d___attribute_group' %(idx_panel))
            else:
                es_form = None

            sub_panels = []
            for idx_sub_panel, sub_panel in enumerate(panel.attributesubpanel_set.all(), start=1):
                tmp_sub_panel_dict = {}
                tmp_sub_panel_dict['display_name'] = sub_panel.name
                tmp_sub_panel_dict['name'] = ''.join(sub_panel.name.split()).lower()
                if sub_panel.attribute_fields.all():
                    tmp_sub_panel_dict['form'] = ESAttributeFormPart(sub_panel.attribute_fields.all(), prefix='%d_%d___attribute_group' %(idx_panel, idx_sub_panel))
                else:
                    tmp_sub_panel_dict['form'] = None
                tmp_sub_panel_dict['attribute_group_id'] = '%d_%d___attribute_group' %(idx_panel, idx_sub_panel)

                sub_panels.append(tmp_sub_panel_dict)

            # print(panel.name, es_form)
            tmp_dict['panels'].append({'display_name': panel.name,
                                      'name': ''.join(panel.name.split()).lower(),
                                      'form': es_form,
                                      'attribute_group_id': '%d___attribute_group' %(idx_panel),
                                      'sub_panels': sub_panels })
        tabs.append(tmp_dict)

    context = {}
    context['tabs'] = tabs
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
        study_form = StudyForm(request.user, POST_data)
        study_form.is_valid()
        study_string = study_form.cleaned_data['study']

        dataset_form = DatasetForm(study_string, request.user, POST_data)
        dataset_form.is_valid()
        dataset_string = dataset_form.cleaned_data['dataset']

        dataset_obj = Dataset.objects.get(description=dataset_string)

        if es_filter_form.is_valid() and es_attribute_form.is_valid():
            es_filter = ElasticSearchFilter()

            es_filter_form_data = es_filter_form.cleaned_data
            # print(es_filter_form_data)
            es_attribute_form_data = es_attribute_form.cleaned_data
            # print(es_attribute_form_data)
            source_fields = []
            non_nested_attribute_fields = []
            nested_attribute_fields = []


            ### I am going to treat gatkqs as a non-nested field

            for key, val in es_attribute_form.cleaned_data.items():
                if val:
                    # print(key,val)
                    es_name, path = key.split('-')
                    if path and es_name != "qs":
                        source_fields.append(path)
                        nested_attribute_fields.append(path)
                    elif es_name == "qs":
                        source_fields.append(path)
                        non_nested_attribute_fields.append("gatkQS")
                    else:
                        source_fields.append(es_name)
                        non_nested_attribute_fields.append(es_name)

            terminate = True if request.POST.get('terminate') else False
            terminate = True

            keys = es_filter_form_data.keys()
            used_keys = []


            nested_attribute_fields = list(set(nested_attribute_fields))
            dict_filter_fields = {}


            for key, es_name, es_filter_type, path in [ (ele, ele.split('-')[0], ele.split('-')[1], ele.split('-')[2]) for ele in keys ]:

                data = es_filter_form_data[key]


                if not data:
                    continue

                # print(key, es_name, es_filter_type, path, data, type(data))

                if path and path not in source_fields:
                    source_fields.append(path)
                elif not path and es_name not in source_fields:
                    source_fields.append(es_name)

                if path.strip():
                    post_filter_field = "%s___%s___%s" %(path, es_name, es_filter_type)
                    dict_filter_fields[post_filter_field] = []

                used_keys.append((key.split('-')[0], data))
                # print(key, data, es_filter_type)
                field_obj = FilterField.objects.get(es_name=es_name, es_filter_type__name=es_filter_type, path=path)
                if es_filter_type == 'filter_term':
                    if isinstance(data, list):
                        for ele in data:
                            es_filter.add_filter_term(es_name, ele.strip())
                    else:
                        es_filter.add_filter_term(es_name, data)

                elif es_filter_type == 'filter_terms' and isinstance(data, str):
                    es_filter.add_filter_terms(es_name, data.splitlines())

                elif es_filter_type == 'filter_terms' and isinstance(data, list):
                    es_filter.add_filter_terms(es_name, data)

                elif es_filter_type == 'nested_filter_term' and isinstance(data, str):
                    for ele in data.splitlines():
                        es_filter.add_nested_filter_term(es_name, ele.strip(), field_obj.path)
                        dict_filter_fields[post_filter_field].append(ele.strip())

                elif es_filter_type == 'nested_filter_term' and isinstance(data, list):
                    for ele in data:
                        es_filter.add_nested_filter_term(es_name, ele.strip(), field_obj.path)
                        dict_filter_fields[post_filter_field].append(ele.strip())
                elif es_filter_type == 'nested_filter_terms' and isinstance(data, str):
                    es_filter.add_nested_filter_terms(es_name, data.splitlines(), field_obj.path)
                    for ele in data.splitlines():
                        dict_filter_fields[post_filter_field].append(ele.strip())
                elif es_filter_type == 'nested_filter_terms' and isinstance(data, list):
                    es_filter.add_nested_filter_terms(es_name, data, field_obj.path)
                    for ele in data:
                        dict_filter_fields[post_filter_field].append(ele.strip())

                elif es_filter_type == 'filter_range_gte':
                    es_filter.add_filter_range_gte(es_name, data)

                elif es_filter_type == 'filter_range_lte':
                    es_filter.add_filter_range_lte(es_name, data)

                elif es_filter_type == 'nested_filter_range_gte':
                    es_filter.add_nested_filter_range_gte(es_name, data, path)
                    dict_filter_fields[post_filter_field].append(int(data.strip()))

                elif es_filter_type == 'filter_exists':
                    if data == 'only':
                        es_filter.add_filter_exists(es_name, data)
                    else:
                        es_filter.add_must_not_exists(es_name, '')

                elif es_filter_type == 'must_not_exists':
                    es_filter.add_must_not_exists(es_name, data)

            for field in source_fields:
                # if field:
                es_filter.add_source(field)


            content =  es_filter.generate_query_string()

            # pprint(content)
            content_generate_time = datetime.now() - start_time
            query = json.dumps(content)
            print(query)

            search_options = SearchOptions.objects.get(dataset=dataset_obj)
            if search_options.es_terminate:
                uri = 'http://%s:%s/%s/%s/_search?terminate_after=%d' %(dataset_obj.es_host,
                                                                        dataset_obj.es_port,
                                                                        dataset_obj.es_index_name,
                                                                        dataset_obj.es_type_name,
                                                                        search_options.es_terminate_size_per_shard)
            else:
                uri = 'http://%s:%s/%s/%s/_search?' %(dataset_obj.es_host, dataset_obj.es_port, dataset_obj.es_index_name, dataset_obj.es_type_name)
            response = requests.get(uri, data=query)
            results = json.loads(response.text)

            start_after_results_time = datetime.now()
            total = results['hits']['total']
            took = results['took']
            context = {}
            headers = []

            for key, val in attribute_order.items():
                # print(val)
                order, es_name, path = val.split('-')
                attribute = AttributeField.objects.get(dataset=dataset_obj, es_name=es_name, path=path)
                headers.append((int(order), attribute))

            headers = sorted(headers, key=itemgetter(0))
            _, headers  = zip(*headers)

            tmp_results = results['hits']['hits']
            results = []
            for ele in tmp_results:
                tmp_source = ele['_source']
                # print(ele['_id'])
                tmp_source['es_id'] = ele['_id']
                results.append(tmp_source)

            # print(results)
            # print(dict_filter_fields)
            if nested_attribute_fields:
                final_results = []
                results_count = 0
                for idx, result in enumerate(results):
                    if results_count>search_options.maximum_table_size:
                        break
                    # print(results)
                    for idx, path in enumerate(nested_attribute_fields):

                        if dict_filter_fields:
                            for key, val in dict_filter_fields.items():
                                key_path, key_es_name, key_es_filter_type = key.split('___')
                                if key_es_filter_type in ["filter_range_gte",
                                                          "filter_range_lte",
                                                          "filter_range_lt",
                                                          "nested_filter_range_gte",]:
                                    comparison_type = key_es_filter_type.split('_')[-1]
                                else:
                                    comparison_type = 'default'

                                # print(key_path, key_es_name, key_es_filter_type,val, comparison_type,result[key_path])

                                result[key_path] = filter_array_dicts(result[key_path], key_es_name, val, comparison_type)

                        if idx == 0:
                            combined_nested = result[path]
                            continue
                        else:
                            combined_nested = list(itertools.product(combined_nested, result[path]))
                            combined_nested = merge_two_dicts_array(combined_nested)



                    tmp_non_nested = subset_dict(result, non_nested_attribute_fields)
                    tmp_output = list(itertools.product([tmp_non_nested,], combined_nested))

                    for x,y in tmp_output:
                        tmp = merge_two_dicts(x,y)
                        tmp["es_id"] = result["es_id"]
                        final_results.append(tmp)
                        results_count += 1
            else:
                final_results = results


            header_json = serializers.serialize("json", headers)
            query_json = json.dumps(query)
            if nested_attribute_fields:
                nested_attribute_fields_json = json.dumps(nested_attribute_fields)
            else:
                nested_attribute_fields_json = None

            if non_nested_attribute_fields:
                non_nested_attribute_fields_json = json.dumps(non_nested_attribute_fields)
            else:
                non_nested_attribute_fields_json = None

            if dict_filter_fields:
                dict_filter_fields_json = json.dumps(dict_filter_fields)
            else:
                dict_filter_fields_json = None

            if used_keys:
                used_keys_json = json.dumps(used_keys)
            else:
                used_keys_json = None


            search_result_download_obj = SearchResultDownload.objects.create(
                                                dataset=dataset_obj,
                                                headers=header_json,
                                                query=query_json,
                                                nested_attribute_fields=nested_attribute_fields_json,
                                                non_nested_attribute_fields=non_nested_attribute_fields_json,
                                                dict_filter_fields=dict_filter_fields_json,
                                                used_keys=used_keys_json
                                            )


            # print("dict_filter_fields",dict_filter_fields)
            # print(final_results)
            context['used_keys'] = used_keys
            context['took'] = took
            context['total'] = total
            context['results'] = final_results
            context['content_generate_time'] = content_generate_time
            context['after_results_time'] = datetime.now() - start_after_results_time
            context['total_time'] = datetime.now() - start_time
            context['headers'] = headers
            context['search_result_download_obj_id'] = search_result_download_obj.id

            return render(request, 'search/search_results.html', context)

def yield_results(dataset_obj,
                    header_keys,
                    query,
                    nested_attribute_fields,
                    non_nested_attribute_fields,
                    dict_filter_fields,
                    used_keys):


    es = Elasticsearch(host=dataset_obj.es_host)
    # query = json.dumps(query)
    yield header_keys
    for ele in helpers.scan(es,
                            query=query,
                            scroll=u'5m',
                            size=1000,
                            preserve_order=False,
                            index=dataset_obj.es_index_name,
                            doc_type=dataset_obj.es_type_name):

        result = ele['_source']
        final_results = []
        if nested_attribute_fields:
            for idx, path in enumerate(nested_attribute_fields):
                if dict_filter_fields:
                    for key, val in dict_filter_fields.items():
                        key_path, key_es_name, key_es_filter_type = key.split('___')
                        if key_es_filter_type in ["filter_range_gte",
                                                  "filter_range_lte",
                                                  "filter_range_lt",
                                                  "nested_filter_range_gte",]:
                            comparison_type = key_es_filter_type.split('_')[-1]
                        else:
                            comparison_type = 'default'

                        result[key_path] = filter_array_dicts(result[key_path], key_es_name, val, comparison_type)

                if idx == 0:
                    combined_nested = result[path]
                    continue
                else:
                    combined_nested = list(itertools.product(combined_nested, result[path]))
                    combined_nested = merge_two_dicts_array(combined_nested)

            tmp_non_nested = subset_dict(result, non_nested_attribute_fields)
            tmp_output = list(itertools.product([tmp_non_nested,], combined_nested))

            for x,y in tmp_output:
                tmp = merge_two_dicts(x,y)
                final_results.append(tmp)
        else:
            final_results = [result,]



        for idx, result in enumerate(final_results):
            tmp = []

            for header in header_keys:
                tmp.append(str(result.get(header, None)))
            yield tmp



class Echo(object):
    """An object that implements just the write method of the file-like
    interface.
    """
    def write(self, value):
        """Write the value by returning it, instead of storing in a buffer."""
        return value
@gzip_page
def search_result_download(request):
    search_result_download_obj_id = request.POST['search_result_download_obj_id']
    search_result_download_obj = SearchResultDownload.objects.get(id=search_result_download_obj_id)
    dataset_obj = search_result_download_obj.dataset
    headers = search_result_download_obj.headers
    headers = [ele.object for ele in serializers.deserialize("json", headers)]
    query = json.loads(search_result_download_obj.query)
    query = json.loads(query)
    print(type(query))
    print(query)
    if search_result_download_obj.nested_attribute_fields:
        nested_attribute_fields = json.loads(search_result_download_obj.nested_attribute_fields)
    else:
        nested_attribute_fields = []

    if search_result_download_obj.non_nested_attribute_fields:
        non_nested_attribute_fields = json.loads(search_result_download_obj.non_nested_attribute_fields)
    else:
        non_nested_attribute_fields = []

    if search_result_download_obj.dict_filter_fields:
        dict_filter_fields = json.loads(search_result_download_obj.dict_filter_fields)
    else:
        dict_filter_fields = []

    if search_result_download_obj.used_keys:
        used_keys = json.loads(search_result_download_obj.used_keys)
    else:
        used_keys = []

    header_keys = [ele.es_name for ele in headers]


    pseudo_buffer = Echo()
    writer = csv.writer(pseudo_buffer, delimiter='\t')


    results = yield_results(dataset_obj,
                    header_keys,
                    query,
                    nested_attribute_fields,
                    non_nested_attribute_fields,
                    dict_filter_fields,
                    used_keys)
    response = StreamingHttpResponse((writer.writerow(row) for row in results if row), content_type="text/csv")
    response['Content-Disposition'] = 'attachment; filename="results.tsv"'
    return response

    # for ele in results:
    #     print(ele)
    # return HttpResponse(ele)


def get_variant(request, dataset_id, variant_id):
    dataset = Dataset.objects.get(id=dataset_id)
    es = elasticsearch.Elasticsearch(host=dataset.es_host)
    index_name = dataset.es_index_name
    type_name = dataset.es_type_name
    result = get_es_result(es, index_name, type_name, variant_id)

    context = {}
    # print(result)
    context["result"] = result
    context["variant_id"] = variant_id
    context["dataset_id"] = dataset.id
    context["index_name"] = index_name
    context["type_name"] = type_name
    return render(request, 'search/variant_info.html', context)

