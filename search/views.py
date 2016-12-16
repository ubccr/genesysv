from django.shortcuts import render, redirect
from django.core.urlresolvers import reverse_lazy
from django.http import HttpResponse, JsonResponse
from django.core.serializers.json import DjangoJSONEncoder
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
from django.views.generic import (
    ListView, DetailView, CreateView, UpdateView, DeleteView, FormView
    )
from django.shortcuts import get_object_or_404
from django.http import HttpResponseForbidden


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
                if val in tmp.split():
                    output.append(ele)

            elif comparison_type == "val_in":
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

@lru_cache(maxsize=None)
@gzip_page
def get_study_form(request):
    form = StudyForm(request.user)
    context = {'form':form}
    return render(request, "search/get_study_snippet.html", context)

@lru_cache(maxsize=None)
@gzip_page
def get_dataset_form(request):
    selected_study = request.GET['selected_study']
    form = DatasetForm(selected_study, request.user)
    context = {'form':form}
    return render(request, "search/get_dataset_snippet.html", context)

@lru_cache(maxsize=None)
@gzip_page
def get_filter_form(request):
    if request.GET:
        dataset = request.GET.get('selected_dataset')
        dataset_object = Dataset.objects.get(description=dataset)
        tabs = []
        for tab in FilterTab.objects.filter(dataset=dataset_object):
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

@lru_cache(maxsize=None)
@gzip_page
def get_attribute_form(request):
    if request.GET:
        dataset = request.GET.get('selected_dataset')
        dataset_object = Dataset.objects.get(description=dataset)
        tabs = []
        for tab in AttributeTab.objects.filter(dataset=dataset_object):
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

@lru_cache(maxsize=None)
@gzip_page
def search_home(request):
    context = {}
    context['load_search'] = 'false'
    context['information_json'] = 'false'
    return render(request, 'search/search.html', context)


@lru_cache(maxsize=None)
@gzip_page
def retrieve_saved_search(request, pk):

    saved_search_obj = get_object_or_404(SavedSearch, pk=pk)


    information = {}
    information['study'] = saved_search_obj.dataset.study.name
    information['dataset'] = saved_search_obj.dataset.description
    information['filters_used'] = saved_search_obj.get_filters_used
    information['attributes_selected'] = saved_search_obj.get_attributes_selected

    information_json = json.dumps(information, cls=DjangoJSONEncoder)

    context = {}
    context['information_json'] = information_json
    context['load_search'] = 'true'
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
def search(request):

    if request.POST:
        start_time = datetime.now()
        attribute_order = json.loads(request.POST['attribute_order'])
        POST_data = QueryDict(request.POST['form_data'])


        study_form = StudyForm(request.user, POST_data)
        study_form.is_valid()
        study_string = study_form.cleaned_data['study']

        dataset_form = DatasetForm(study_string, request.user, POST_data)
        dataset_form.is_valid()
        dataset_string = dataset_form.cleaned_data['dataset']

        dataset_obj = Dataset.objects.get(description=dataset_string)


        es_filter_form = ESFilterForm(dataset_obj, POST_data, prefix='filter_')
        es_attribute_form = ESAttributeForm(dataset_obj, POST_data, prefix='attribute_group')


        if es_filter_form.is_valid() and es_attribute_form.is_valid():
            es_filter = ElasticSearchFilter()

            es_filter_form_data = es_filter_form.cleaned_data
            # print(es_filter_form_data)
            es_attribute_form_data = es_attribute_form.cleaned_data
            # print(es_attribute_form_data)
            source_fields = []
            non_nested_attribute_fields = []
            nested_attribute_fields = []


            ### I am going to treat gatkqs as a non-nested field; This way the case and control gatk scores are
            ### not put on a separate line; Maybe better to add some attribute to the field in model instead.



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

            filters_used = {}
            for key, es_name, es_filter_type, path in [ (ele, ele.split('-')[0], ele.split('-')[1], ele.split('-')[2]) for ele in keys ]:

                data = es_filter_form_data[key]

                if not data:
                    continue

                filters_used[key] = data
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
                field_obj = FilterField.objects.get(dataset=dataset_obj,
                                                    es_name=es_name, es_filter_type__name=es_filter_type, path=path)
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

                elif es_filter_type == 'filter_range_lt':
                    es_filter.add_filter_range_lt(es_name, data)

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

                elif es_filter_type == 'nested_filter_exists':
                    es_filter.add_nested_filter_exists(es_name, data, path)


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

            attributes_selected = []
            for ele in headers:
                attributes_selected.append('%s-%s' %(ele.es_name, ele.path))
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
                    combined = False
                    combined_nested = None
                    for idx, path in enumerate(nested_attribute_fields):

                        if dict_filter_fields:
                            for key, val in dict_filter_fields.items():
                                key_path, key_es_name, key_es_filter_type = key.split('___')
                                if key_es_filter_type in ["filter_range_gte",
                                                          "filter_range_lte",
                                                          "filter_range_lt",
                                                          "nested_filter_range_gte",]:
                                    comparison_type = key_es_filter_type.split('_')[-1]
                                elif key_es_filter_type in ["nested_filter_term",
                                                            "nested_filter_terms",]:
                                    comparison_type = 'val_in'
                                else:
                                    comparison_type = 'val_in'
                                if comparison_type:
                                    filtered_results = filter_array_dicts(result[key_path], key_es_name, val, comparison_type)

                                if filtered_results:
                                    result[key_path] = filtered_results

                        if path not in result:
                            continue

                        if not combined:
                            combined_nested = result[path]
                            combined = True
                            continue
                        else:
                            combined_nested = list(itertools.product(combined_nested, result[path]))
                            combined_nested = merge_two_dicts_array(combined_nested)

                    tmp_non_nested = subset_dict(result, non_nested_attribute_fields)
                    if combined_nested:
                        tmp_output = list(itertools.product([tmp_non_nested,], combined_nested))
                        for x,y in tmp_output:
                            tmp = merge_two_dicts(x,y)
                            tmp["es_id"] = result["es_id"]
                            final_results.append(tmp)
                            results_count += 1
                    else:
                        final_results.append(result)
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



            filters_used = json.dumps(filters_used)
            attributes_selected = json.dumps(attributes_selected)


            search_log_obj = SearchLog.objects.create(
                                                dataset=dataset_obj,
                                                headers=header_json,
                                                query=query_json,
                                                nested_attribute_fields=nested_attribute_fields_json,
                                                non_nested_attribute_fields=non_nested_attribute_fields_json,
                                                dict_filter_fields=dict_filter_fields_json,
                                                used_keys=used_keys_json,
                                                filters_used=filters_used,
                                                attributes_selected=attributes_selected,
                                            )


            if request.user.is_authenticated():
                search_log_obj.user=request.user
                search_log_obj.save()


            if request.user.is_authenticated():
                save_search_form = SaveSearchForm(request.user, dataset_obj, filters_used or 'None', attributes_selected)
                context['save_search_form'] = save_search_form
            else:
                context['save_search_form'] = None


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
            context['search_log_obj_id'] = search_log_obj.id


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
            combined = False
            combined_nested = None
            for idx, path in enumerate(nested_attribute_fields):
                if dict_filter_fields:
                    for key, val in dict_filter_fields.items():
                        key_path, key_es_name, key_es_filter_type = key.split('___')
                        if key_es_filter_type in ["filter_range_gte",
                                                  "filter_range_lte",
                                                  "filter_range_lt",
                                                  "nested_filter_range_gte",]:
                            comparison_type = key_es_filter_type.split('_')[-1]
                        elif key_es_filter_type in ["nested_filter_term",
                                                    "nested_filter_terms",]:
                            comparison_type = 'val_in'
                        else:
                            comparison_type = 'val_in'
                        if comparison_type:
                            filtered_results = filter_array_dicts(result[key_path], key_es_name, val, comparison_type)

                        if filtered_results:
                            result[key_path] = filtered_results

                if path not in result:
                    continue

                if not combined:
                    combined_nested = result[path]
                    combined = True
                    continue
                else:
                    combined_nested = list(itertools.product(combined_nested, result[path]))
                    combined_nested = merge_two_dicts_array(combined_nested)

            tmp_non_nested = subset_dict(result, non_nested_attribute_fields)
            if combined_nested:
                tmp_output = list(itertools.product([tmp_non_nested,], combined_nested))
                for x,y in tmp_output:
                    tmp = merge_two_dicts(x,y)
                    final_results.append(tmp)
            else:
                final_results.append(result)

        # if nested_attribute_fields:
        #     for idx, path in enumerate(nested_attribute_fields):
        #         if dict_filter_fields:
        #             for key, val in dict_filter_fields.items():
        #                 key_path, key_es_name, key_es_filter_type = key.split('___')
        #                 if key_es_filter_type in ["filter_range_gte",
        #                                           "filter_range_lte",
        #                                           "filter_range_lt",
        #                                           "nested_filter_range_gte",]:
        #                     comparison_type = key_es_filter_type.split('_')[-1]
        #                 else:
        #                     comparison_type = 'default'

        #                 filtered_result = filter_array_dicts(result[key_path], key_es_name, val, comparison_type)
        #                 if filtered_result:
        #                     result[key_path] = filtered_result

        #         if idx == 0:
        #             combined_nested = result[path]
        #             continue
        #         else:
        #             combined_nested = list(itertools.product(combined_nested, result[path]))
        #             combined_nested = merge_two_dicts_array(combined_nested)

        #     tmp_non_nested = subset_dict(result, non_nested_attribute_fields)
        #     tmp_output = list(itertools.product([tmp_non_nested,], combined_nested))

        #     for x,y in tmp_output:
        #         tmp = merge_two_dicts(x,y)
        #         final_results.append(tmp)
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
def download_result(request):
    search_log_obj_id = request.POST['search_log_obj_id']
    search_log_obj = SearchLog.objects.get(id=search_log_obj_id)
    dataset_obj = search_log_obj.dataset
    headers = search_log_obj.headers
    headers = [ele.object for ele in serializers.deserialize("json", headers)]
    query = json.loads(search_log_obj.query)
    query = json.loads(query)
    # print(type(query))
    # print(query)
    if search_log_obj.nested_attribute_fields:
        nested_attribute_fields = json.loads(search_log_obj.nested_attribute_fields)
    else:
        nested_attribute_fields = []

    if search_log_obj.non_nested_attribute_fields:
        non_nested_attribute_fields = json.loads(search_log_obj.non_nested_attribute_fields)
    else:
        non_nested_attribute_fields = []

    if search_log_obj.dict_filter_fields:
        dict_filter_fields = json.loads(search_log_obj.dict_filter_fields)
    else:
        dict_filter_fields = []

    if search_log_obj.used_keys:
        used_keys = json.loads(search_log_obj.used_keys)
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

    user = request.user
    dataset = Dataset.objects.get(id=dataset_id)
    groups = [ele.name for ele in dataset.allowed_groups.all()]
    group_access = user.groups.filter(name__in=groups).exists()

    if not group_access and not dataset.is_public:
        return HttpResponseForbidden()


    es = elasticsearch.Elasticsearch(host=dataset.es_host)
    index_name = dataset.es_index_name
    type_name = dataset.es_type_name
    result = get_es_result(es, index_name, type_name, variant_id)

    context = {}
    # print(result)


    conserved_elements = [
        "gerp_plus_gt2",
        "genomicSuperDups",
        "phastConsElements100way",
        "phastConsElements46way",
        "tfbsConsSites",

    ]

    conserved_elements_available = True if any(True for ele in conserved_elements if result.get(ele)) else False

    coding_regions = [
        "fathmm_MKL_coding_pred",
        "FATHMM_pred",
        "LRT_pred",
        "LR_pred",
        "MetaLR_pred",
        "MetaSVM_pred",
        "MutationAssessor_pred",
        "MutationTaster_pred",
        "PROVEAN_pred",
        "Polyphen2_HDIV_pred",
        "Polyphen2_HVAR_pred",
        "RadialSVM_pred",
        "SIFT_pred"]

    splice_junctions = ["dbscSNV_ADA_SCORE", "dbscSNV_RF_SCORE"]

    coding_region_available = True if any(True for ele in coding_regions if result.get(ele)) else False
    splice_junctions_available = True if any(True for ele in splice_junctions if result.get(ele)) else False

    context["result"] = result
    context["conserved_elements_available"] = conserved_elements_available
    context["coding_region_available"] = coding_region_available
    context["splice_junctions_available"] = splice_junctions_available
    context["variant_id"] = variant_id
    context["dataset_id"] = dataset.id
    context["index_name"] = index_name
    context["type_name"] = type_name
    return render(request, 'search/variant_info.html', context)


def save_search(request):
    if request.POST:
        dataset = Dataset.objects.get(id=request.POST.get('dataset'))
        filters_used = request.POST.get('filters_used')
        attributes_selected = request.POST.get('attributes_selected')
        form = SaveSearchForm(request.user, dataset, filters_used, attributes_selected, request.POST)
        if form.is_valid():
            data = form.cleaned_data
            user = data.get('user')
            dataset = data.get('dataset')
            filters_used = data.get('filters_used')
            attributes_selected = data.get('attributes_selected')
            description = data.get('description')
            SavedSearch.objects.create(user=user, dataset=dataset, filters_used=filters_used, attributes_selected=attributes_selected, description=description)
            return redirect('search-home')
        else:
            return HttpResponseServerError()

class SavedSearchList(ListView):
    model = SavedSearch
    context_object_name = 'saved_search_list'
    template_name = 'search/savedsearch_list.html'
    def get_queryset(self):
        return SavedSearch.objects.filter(user=self.request.user)

class SavedSearchUpdate(UpdateView):
    model = SavedSearch
    fields = ('description',)
    success_url = reverse_lazy('saved-search-list')
    template_name = 'search/savedsearch_form_update.html'

class SavedSearchDelete(DeleteView):
    model = SavedSearch
    success_url = reverse_lazy('saved-search-list')
    template_name = 'search/savedsearch_confirm_delete.html'
