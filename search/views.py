from django.shortcuts import render, redirect, reverse
from django.urls import reverse_lazy
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
from django.conf import settings
import itertools
import hashlib
from collections import deque
from pybamview.models import SampleBamInfo
from django.db.models import Q
from django.views.generic.edit import UpdateView
from django.http import HttpResponseForbidden
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.cache import cache
from django.views.decorators.cache import cache_page
from collections import defaultdict
import multiprocessing
from functools import partial


from .forms import VariantStatusReviewUpdateForm, ReviewStatusForm

# CONSTANTS
REVIEW_STATUS_CHOICES = (
    ('approved', '<i class="fa fa-check" aria-hidden="true"></i>'),
    ('rejected', '<i class="fa fa-times" aria-hidden="true"></i>'),
    ('pending', '<i class="fa fa-question" aria-hidden="true"></i>'),
    ('not_reviewed', '--'),
)


def filter_FILTER_QUAL(result, FILTER_value, QUAL_value):
    case_filter_status = None
    control_filter_status = None
    case_qual_score = None
    control_qual_score = None
    for ele in result.get('FILTER'):
        if ele.get('FILTER_label'):
            if ele.get('FILTER_label') == "case":
                case_filter_status = ele.get('FILTER_value')
            elif ele.get('FILTER_label') == "control":
                control_filter_status = ele.get('FILTER_value')

    for ele in result.get('QUAL'):
        if ele.get('QUAL_label'):
            if ele.get('QUAL_label') == "case":
                case_qual_score = ele.get('QUAL_value')
            elif ele.get('QUAL_label') == "control":
                control_qual_score = ele.get('QUAL_value')

    keep_label = {}

    if case_filter_status == FILTER_value and case_qual_score >= QUAL_value:
        keep_label["case"] = True
    else:
        keep_label["case"] = False

    if control_filter_status == FILTER_value and control_qual_score >= QUAL_value:
        keep_label["control"] = True
    else:
        keep_label["control"] = False

    old_filter_data = result['FILTER']
    new_filter_data = []
    for ele in old_filter_data:
        label = ele['FILTER_label']
        if keep_label.get(label):
            new_filter_data.append(ele)

    old_qual_data = result['QUAL']
    new_qual_data = []
    for ele in old_qual_data:
        label = ele['QUAL_label']
        if keep_label.get(label):
            new_qual_data.append(ele)

    return (new_filter_data, new_qual_data)


def get_variant_review_status(variant_es_id, group):

    try:
        variant_review_status_obj = VariantReviewStatus.objects.get(
            variant_es_id=variant_es_id, group=group)
        return variant_review_status_obj.status
    except Exception as e:
        return 'not_reviewed'


def get_variant_review_status_obj(variant_es_id, group):

    try:
        variant_review_status_obj = VariantReviewStatus.objects.get(
            variant_es_id=variant_es_id, group=group)
        return variant_review_status_obj
    except:
        return None


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

    for ele in array:
        tmp = ele.get(key, 'missing')
        if tmp == 'missing':
            continue
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

            elif comparison_type == "number_equal":
                if float(val) == float(tmp):
                    output.append(ele)

            elif comparison_type == "keyword":
                if val == tmp:
                    output.append(ele)

            elif comparison_type == "text":
                print(val, tmp)
                if isinstance(tmp, list):
                    for ele_tmp in tmp:
                        if val.lower() in ele_tmp.lower():
                            output.append(ele)
                            break
                else:
                    if val.lower() in ele_tmp.lower():
                        output.append(ele)
                        break

            elif comparison_type == "val_in":
                for ele_tmp in tmp.split('_'):
                    if val.lower() in ele_tmp.lower():
                        output.append(ele)
                        break
            elif comparison_type == "nested_filter_exists":
                output.append(ele)
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
    # print(x,y)
    z = x.copy()
    z.update(y)
    return z


def subset_dict(input, keys):

    return {key: input[key] for key in keys if input.get(key) != None}


@gzip_page
def get_study_form(request):
    form = StudyForm(request.user)
    context = {'form': form}
    return render(request, "search/get_study_snippet.html", context)


def get_dataset_form_cached(selected_study, user):
    return DatasetForm(selected_study, user)


@gzip_page
def get_dataset_form(request):
    selected_study = request.GET['selected_study']
    form = get_dataset_form_cached(selected_study, request.user)
    context = {'form': form}
    return render(request, "search/get_dataset_snippet.html", context)


def get_filter_form_cached(dataset_object):
    tabs = deque()
    for tab in FilterTab.objects.filter(dataset=dataset_object):
        tmp_dict = {}
        tmp_dict['name'] = tab.name
        tmp_dict['panels'] = deque()
        for panel in tab.filter_panels.filter(is_visible=True):
            es_form = ESFilterFormPart(panel.filter_fields.filter(is_visible=True).select_related(
                'widget_type', 'form_type', 'es_filter_type'), prefix='filter_')
            sub_panels = deque()

            for sub_panel in panel.filtersubpanel_set.filter(is_visible=True):
                if panel.are_sub_panels_mutually_exclusive:
                    MEgroup = "MEgroup_%d_%d" % (panel.id, sub_panel.id)
                else:
                    MEgroup = None
                tmp_sub_panel_dict = {}
                tmp_sub_panel_dict['display_name'] = sub_panel.name
                tmp_sub_panel_dict['name'] = ''.join(
                    sub_panel.name.split()).lower()
                tmp_sub_panel_dict['form'] = ESFilterFormPart(sub_panel.filter_fields.filter(
                    is_visible=True).select_related('widget_type', 'form_type', 'es_filter_type'), MEgroup, prefix='filter_')
                sub_panels.append(tmp_sub_panel_dict)

            tmp_dict['panels'].append({'display_name': panel.name,
                                       'name': ''.join(panel.name.split()).lower(),
                                       'form': es_form,
                                       'sub_panels': sub_panels})
        tabs.append(tmp_dict)

    context = {}
    context['tabs'] = tabs
    return context


@gzip_page
def get_filter_form(request):
    if request.GET:
        dataset = request.GET.get('selected_dataset')
        dataset = Dataset.objects.get(description=dataset)
        cache_name = 'context_filter_{}'.format(dataset.name)
        if not cache.get(cache_name):
            context = get_filter_form_cached(dataset)
            response = render(
                request, "search/get_filter_snippet.html", context)
            cache.set(cache_name, response, None)  # 10 minutes
            return response
        else:
            return cache.get(cache_name)


def get_attribute_form_cached(dataset_object):
    tabs = deque()
    for tab in AttributeTab.objects.filter(dataset=dataset_object):
        tmp_dict = {}
        tmp_dict['name'] = tab.name
        tmp_dict['panels'] = deque()
        for idx_panel, panel in enumerate(tab.attribute_panels.filter(is_visible=True), start=1):
            if panel.attribute_fields.filter(is_visible=True):
                es_form = ESAttributeFormPart(panel.attribute_fields.filter(
                    is_visible=True), prefix='%d___attribute_group' % (idx_panel))
            else:
                es_form = None

            sub_panels = deque()
            for idx_sub_panel, sub_panel in enumerate(panel.attributesubpanel_set.filter(is_visible=True), start=1):
                tmp_sub_panel_dict = {}
                tmp_sub_panel_dict['display_name'] = sub_panel.name
                tmp_sub_panel_dict['name'] = ''.join(
                    sub_panel.name.split()).lower()
                if sub_panel.attribute_fields.filter(is_visible=True):
                    tmp_sub_panel_dict['form'] = ESAttributeFormPart(sub_panel.attribute_fields.filter(
                        is_visible=True), prefix='%d_%d___attribute_group' % (idx_panel, idx_sub_panel))
                else:
                    tmp_sub_panel_dict['form'] = None
                tmp_sub_panel_dict['attribute_group_id'] = '%d_%d___attribute_group' % (
                    idx_panel, idx_sub_panel)

                sub_panels.append(tmp_sub_panel_dict)

            tmp_dict['panels'].append({'display_name': panel.name,
                                       'name': ''.join(panel.name.split()).lower(),
                                       'form': es_form,
                                       'attribute_group_id': '%d___attribute_group' % (idx_panel),
                                       'sub_panels': sub_panels})
        tabs.append(tmp_dict)

    context = {}
    context['tabs'] = tabs
    return context


@gzip_page
def get_attribute_form(request):
    if request.GET:
        dataset = request.GET.get('selected_dataset')
        dataset = Dataset.objects.get(description=dataset)
        cache_name = 'context_attribute_{}'.format(dataset.name)
        if not cache.get(cache_name):
            context = get_attribute_form_cached(dataset)
            response = render(
                request, "search/get_attribute_snippet.html", context)
            cache.set(cache_name, response, None)  # 10 minutes
            return response
        else:
            return cache.get(cache_name)


@gzip_page
def search_home(request):
    context = {}
    context['load_search'] = 'false'
    context['information_json'] = 'false'
    return render(request, 'search/search.html', context)


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
                tmp_sub_panel_dict['name'] = ''.join(
                    sub_panel.name.split()).lower()
                tmp_sub_panel_dict['form'] = ESFormPart(
                    sub_panel.filter_fields.all())
                sub_form.append(tmp_sub_panel_dict)

            tmp_dict['panels'].append({'display_name': panel.name,
                                       'name': ''.join(panel.name.split()).lower(),
                                       'form': es_form,
                                       'sub_form': sub_form})
        tabs.append(tmp_dict)

    context = {}
    context['tabs'] = tabs
    return render(request, 'search/search_old_home.html', context)


def search_home2(request):
    es_form = ESForm()
    context = {}
    context['es_form'] = es_form
    return render(request, 'search/search_home.html', context)


def remove_nested_attributes_not_selected(results, nested_attributes_selected):
    for result in results:
        for path, nested_attributes in nested_attributes_selected.items():
            if path in ['FILTER', 'QUAL']:
                continue
            if result.get(path):
                old_data = result[path]
            else:
                continue
            new_data = []
            for ele in old_data:
                tmp_dict = {}
                for nested_attribute in nested_attributes:
                    if ele.get(nested_attribute):
                        tmp_dict[nested_attribute] = ele[nested_attribute]
                if tmp_dict and tmp_dict not in new_data:
                    new_data.append(tmp_dict)
            result[path] = new_data
    return results


@gzip_page
def search(request):

    if request.POST:
        start_time = datetime.now()

        # Ensure logged in users belong to a group so that Variant annotation
        # works!
        if not request.user.is_anonymous:
            if request.user.groups.count() > 1:
                print('More than one group')
                return HttpResponse(status=400)
            elif request.user.groups.count() == 0:
                print('All users must be in a group!')
                return HttpResponse(status=400)
            else:
                group = request.user.groups.all()[:1].get()

        show_review_status = request.POST.get('show_review_status')
        if show_review_status == "true":
            show_review_status = True
        elif show_review_status == "false":
            show_review_status = False
        else:
            show_review_status = True

        attribute_order = json.loads(request.POST['attribute_order'])
        POST_data = QueryDict(request.POST['form_data'])

        review_status_data = request.POST.get('review_status_to_filter', None)
        review_status_to_filter = []
        if review_status_data:
            for ele in review_status_data.split("&"):
                if 'review_status_to_filter' in ele:
                    tmp_val = ele.split('=')[1]
                    review_status_to_filter.append(tmp_val)
            review_status_form = ReviewStatusForm(
                initial={'review_status_to_filter': review_status_to_filter})
        else:
            review_status_form = ReviewStatusForm()

        study_form = StudyForm(request.user, POST_data)
        study_form.is_valid()
        study_string = study_form.cleaned_data['study']

        dataset_form = DatasetForm(study_string, request.user, POST_data)
        dataset_form.is_valid()
        dataset_string = dataset_form.cleaned_data['dataset']

        dataset_obj = Dataset.objects.get(description=dataset_string)

        es_filter_form = ESFilterForm(dataset_obj, POST_data, prefix='filter_')
        es_attribute_form = ESAttributeForm(
            dataset_obj, POST_data, prefix='attribute_group')

        if es_filter_form.is_valid() and es_attribute_form.is_valid():
            es_filter = ElasticSearchFilter()
            es_filter_form_data = es_filter_form.cleaned_data
            es_attribute_form_data = es_attribute_form.cleaned_data

            non_nested_attributes_selected = []
            nested_attributes_selected = {}

            non_nested_filters_applied = []
            nested_filters_applied = {}

            source_fields = []
            inner_hits_source_fields = {}

            nested_attribute_fields = []
            non_nested_attribute_fields = []
            # I am going to treat gatkqs as a non-nested field; This way the case and control gatk scores are
            # not put on a separate line; Maybe better to add some attribute to
            # the field in model instead.

            for key, val in es_attribute_form.cleaned_data.items():
                if val:
                    attribute_field_obj = AttributeField.objects.get(id=key)
                    es_name, path = attribute_field_obj.es_name, attribute_field_obj.path
                    if path:
                        if path not in nested_attributes_selected:
                            nested_attributes_selected[path] = []
                        nested_attributes_selected[path].append(
                            '%s.%s' % (path, es_name))
                        if path not in nested_attribute_fields:
                            nested_attribute_fields.append(path)
                    else:
                        non_nested_attributes_selected.append(es_name)
                        non_nested_attribute_fields.append(es_name)

            keys = es_filter_form_data.keys()
            used_keys = []

            nested_attribute_fields = list(set(nested_attribute_fields))
            dict_filter_fields = {}

            filters_used = {}

            FILTER_value = None
            QUAL_value = None

            for key, filter_field_obj in [(key, FilterField.objects.get(id=key)) for key in keys]:

                data = es_filter_form_data[key]

                if not data:
                    continue
                filters_used[key] = data

                filter_field_pk = filter_field_obj.id
                es_name = filter_field_obj.es_name
                path = filter_field_obj.path
                es_filter_type = filter_field_obj.es_filter_type.name

                if path == 'FILTER':
                    FILTER_value = data
                if path == 'QUAL':
                    QUAL_value = float(data)

                # Elasticsearch source fields use path for nested fields and
                # the actual field name for non-nested fields
                if path:
                    source_name = '%s.%s' % (path, es_name)
                    if path not in nested_filters_applied:
                        nested_filters_applied[path] = []
                    if source_name not in nested_filters_applied[path]:
                        nested_filters_applied[path].append(
                            '%s.%s' % (path, es_name))
                else:
                    non_nested_filters_applied.append(es_name)

                # Figure out which fields will be in the document source fields
                # and which will be in the inner hits fields

                # Keep a list of nested fields. Nested fields in Elasticsearch are not filtered. All the nested fields
                # are returned for a recored even if only one field match the
                # search criteria
                if path:
                    dict_filter_fields[filter_field_pk] = []

                used_keys.append((es_name, data))

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
                        if filter_field_obj.es_data_type == 'text':
                            if filter_field_obj.es_text_analyzer != 'whitespace':
                                es_filter.add_nested_filter_term(
                                    es_name, ele.strip().lower(), filter_field_obj.path)
                            else:
                                es_filter.add_nested_filter_term(
                                    es_name, ele.strip(), filter_field_obj.path)
                        else:
                            es_filter.add_nested_filter_term(
                                es_name, ele.strip(), filter_field_obj.path)

                        dict_filter_fields[filter_field_pk].append(ele.strip())

                elif es_filter_type == 'nested_filter_term' and isinstance(data, list):
                    for ele in data:
                        if filter_field_obj.es_data_type == 'text':
                            if filter_field_obj.es_text_analyzer != 'whitespace':
                                es_filter.add_nested_filter_term(
                                    es_name, ele.strip().lower(), filter_field_obj.path)
                            else:
                                es_filter.add_nested_filter_term(
                                    es_name, ele.strip(), filter_field_obj.path)
                        else:
                            es_filter.add_nested_filter_term(
                                es_name, ele.strip(), filter_field_obj.path)

                        dict_filter_fields[filter_field_pk].append(ele.strip())

                elif es_filter_type == 'nested_filter_terms' and isinstance(data, str):
                    data_split = data.splitlines()
                    if filter_field_obj.es_data_type == 'text':
                        if filter_field_obj.es_text_analyzer != 'whitespace':
                            data_split_lower = [ele.lower()
                                                for ele in data_split]
                            es_filter.add_nested_filter_terms(
                                es_name, data_split_lower, filter_field_obj.path)
                        else:
                            es_filter.add_nested_filter_terms(
                                es_name, data_split, filter_field_obj.path)
                    else:
                        es_filter.add_nested_filter_terms(
                            es_name, data_split, filter_field_obj.path)

                    for ele in data_split:
                        dict_filter_fields[filter_field_pk].append(ele.strip())

                elif es_filter_type == 'nested_filter_terms' and isinstance(data, list):
                    if filter_field_obj.es_data_type == 'text':
                        if filter_field_obj.es_text_analyzer != 'whitespace':
                            data_lowercase = [ele.lower() for ele in data]
                            es_filter.add_nested_filter_terms(
                                es_name, data_lowercase, filter_field_obj.path)
                        else:
                            es_filter.add_nested_filter_terms(
                                es_name, data, filter_field_obj.path)
                    else:
                        es_filter.add_nested_filter_terms(
                            es_name, data, filter_field_obj.path)

                    for ele in data:
                        dict_filter_fields[filter_field_pk].append(ele.strip())

                elif es_filter_type == 'filter_range_gte':
                    es_filter.add_filter_range_gte(
                        es_name, float(data.strip()))

                elif es_filter_type == 'filter_range_lte':
                    es_filter.add_filter_range_lte(
                        es_name, float(data.strip()))

                elif es_filter_type == 'filter_range_lt':
                    es_filter.add_filter_range_lt(es_name, float(data.strip()))

                elif es_filter_type == 'nested_filter_range_gte':
                    es_filter.add_nested_filter_range_gte(es_name, data, path)
                    dict_filter_fields[filter_field_pk].append(
                        float(data.strip()))

                elif es_filter_type == 'nested_filter_range_lte':
                    es_filter.add_nested_filter_range_lte(es_name, data, path)
                    dict_filter_fields[filter_field_pk].append(
                        float(data.strip()))

                elif es_filter_type == 'filter_exists':
                    if data == 'only':
                        es_filter.add_filter_exists(es_name, data)
                    else:
                        es_filter.add_must_not_exists(es_name, '')

                elif es_filter_type == 'must_not_exists':
                    es_filter.add_must_not_exists(es_name, data)

                elif es_filter_type == 'nested_filter_exists':
                    es_filter.add_nested_filter_exists(es_name, data, path)

            attributes_paths = nested_attributes_selected.keys()
            filter_paths = nested_filters_applied.keys()

            possible_paths = [
                ele for ele in attributes_paths if ele in filter_paths]

            if nested_filters_applied:
                inner_hits_source_fields = nested_filters_applied
                for pp_ele in possible_paths:
                    if pp_ele not in inner_hits_source_fields[pp_ele]:
                        for ele in nested_attributes_selected[pp_ele]:
                            if ele not in inner_hits_source_fields[pp_ele]:
                                inner_hits_source_fields[pp_ele].extend(
                                    nested_attributes_selected[pp_ele])
                        nested_attributes_selected.pop(pp_ele)

            source_fields.extend(non_nested_attributes_selected)

            if nested_attributes_selected:
                for nested_attribute_selected_key, nested_attribute_selected_value in nested_attributes_selected.items():
                    source_fields.extend(nested_attribute_selected_value)

            for field in source_fields:
                es_filter.add_source(field)

            if inner_hits_source_fields:
                es_filter.update_inner_hits_source(inner_hits_source_fields)

            content = es_filter.generate_query_string()

            content_generate_time = datetime.now() - start_time
            query = json.dumps(content)
            pprint(content)

            search_options = SearchOptions.objects.get(dataset=dataset_obj)

            if search_options.es_terminate:
                uri = 'http://%s:%s/%s/%s/_search?terminate_after=%d' % (dataset_obj.es_host,
                                                                         dataset_obj.es_port,
                                                                         dataset_obj.es_index_name,
                                                                         dataset_obj.es_type_name,
                                                                         search_options.es_terminate_size_per_shard)
            else:
                uri = 'http://%s:%s/%s/%s/_search?' % (
                    dataset_obj.es_host, dataset_obj.es_port, dataset_obj.es_index_name, dataset_obj.es_type_name)
            response = requests.get(uri, data=query, headers={
                                    'Content-type': 'application/json'})
            results = json.loads(response.text)

            start_after_results_time = datetime.now()
            total = results['hits']['total']
            took = results['took']
            context = {}
            headers = []

            nested_attributes_selected = defaultdict(list)
            for key, val in attribute_order.items():
                order, pk = val.split('-')

                attribute_field_obj = AttributeField.objects.get(id=pk)
                es_name = attribute_field_obj.es_name
                path = attribute_field_obj.path
                if path:
                    nested_attributes_selected[path].append(es_name)
                headers.append((int(order), attribute_field_obj))

            headers = sorted(headers, key=itemgetter(0))
            _, headers = zip(*headers)

            attributes_selected = []
            for ele in headers:
                attributes_selected.append('%d' % (ele.id))
            tmp_results = results['hits']['hits']
            results = []

            try:
                FilterField.objects.get(dataset=dataset_obj, es_name='Variant')
                variant_field_exist = True
            except:
                variant_field_exist = False

            variants_excluded = []
            for ele in tmp_results:
                tmp_source = ele['_source']
                es_id = ele['_id']
                inner_hits = ele.get('inner_hits')

                if show_review_status and not request.user.is_anonymous:
                    variant_review_status = get_variant_review_status(
                        es_id, group)
                    if review_status_to_filter and variant_review_status not in review_status_to_filter:
                        variants_excluded.append(
                            (dataset_obj.id, es_id, tmp_source['Variant']))
                        continue
                    tmp_source['variant_review_status'] = variant_review_status

                tmp_source['es_id'] = es_id
                if inner_hits:
                    for key, value in inner_hits.items():
                        if key not in tmp_source:
                            tmp_source[key] = []
                        hits_hits_array = inner_hits[key]['hits']['hits']
                        for hit in hits_hits_array:
                            tmp_hit_dict = {}
                            if hit['_source'].get(key):
                                # for Elasticsearch 5
                                for hit_key, hit_value in hit['_source'][key].items():
                                    tmp_hit_dict[hit_key] = hit_value
                            else:
                                # for Elasticsearch 6
                                for hit_key, hit_value in hit['_source'].items():
                                    tmp_hit_dict[hit_key] = hit_value

                            if tmp_hit_dict:
                                tmp_source[key].append(tmp_hit_dict)
                results.append(tmp_source)
            # gene_names
            result_hash_list = deque()
            all_genes = []
            if nested_attribute_fields:
                final_results = []
                results_count = 0
                for idx, result in enumerate(results):
                    if result.get('refGene'):
                        for ele in result.get('refGene'):
                            if ele.get('refGene_symbol'):
                                genes = ele.get('refGene_symbol').split()
                                all_genes.extend(genes)
                    if results_count > search_options.maximum_table_size:
                        break
                    combined = False
                    combined_nested = None
                    for idx, path in enumerate(nested_attribute_fields):
                        if path.startswith('FILTER') or path.startswith('QUAL'):
                            continue

                        if path not in result:
                            continue

                        if not combined:
                            combined_nested = result[path]
                            combined = True
                            continue
                        else:
                            combined_nested = list(itertools.product(
                                combined_nested, result[path]))
                            combined_nested = merge_two_dicts_array(
                                combined_nested)

                    tmp_non_nested = subset_dict(
                        result, non_nested_attribute_fields)
                    if combined_nested:
                        tmp_output = list(itertools.product(
                            [tmp_non_nested, ], combined_nested))

                        for x, y in tmp_output:
                            tmp = merge_two_dicts(x, y)
                            tmp["es_id"] = result["es_id"]
                            if show_review_status and not request.user.is_anonymous:
                                tmp['variant_review_status'] = result[
                                    'variant_review_status']

                            if result.get('FILTER'):
                                tmp['FILTER'] = result['FILTER']
                            if result.get('QUAL'):
                                tmp['QUAL'] = result['QUAL']

                            if tmp not in final_results:
                                final_results.append(tmp)
                                results_count += 1
                    else:
                        if result not in final_results:
                            final_results.append(result)
                            results_count += 1

            else:
                final_results = results

            header_json = serializers.serialize("json", headers)
            query_json = json.dumps(query)
            if nested_attribute_fields:
                nested_attribute_fields_json = json.dumps(
                    nested_attribute_fields)
            else:
                nested_attribute_fields_json = None

            if non_nested_attribute_fields:
                non_nested_attribute_fields_json = json.dumps(
                    non_nested_attribute_fields)
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

            try:
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

            except Exception as e:
                print(e)

            if request.user.is_authenticated:
                search_log_obj.user = request.user
                search_log_obj.save()

            if request.user.is_authenticated:
                save_search_form = SaveSearchForm(
                    request.user, dataset_obj, filters_used or 'None', attributes_selected)
                context['save_search_form'] = save_search_form
            else:
                context['save_search_form'] = None

            if all_genes:
                all_genes = sorted(list(set(all_genes)))
                if 'NONE' in all_genes:
                    all_genes.remove('NONE')
                gene_mania_link = "http://genemania.org/#/search/9606/%s" % (
                    '|'.join(all_genes))
                context['gene_mania_link'] = gene_mania_link

            context['review_status_form'] = review_status_form
            context['debug'] = settings.DEBUG
            context['REVIEW_STATUS_CHOICES'] = REVIEW_STATUS_CHOICES
            context['show_review_status'] = show_review_status
            context['variants_excluded'] = variants_excluded
            context['used_keys'] = used_keys
            context['took'] = took
            context['total'] = total
            context['results'] = final_results[:400]
            context['content_generate_time'] = content_generate_time
            context['after_results_time'] = datetime.now() - \
                start_after_results_time
            context['total_time'] = datetime.now() - start_time
            context['headers'] = headers
            context['search_log_obj_id'] = search_log_obj.id
            context['dataset_obj'] = dataset_obj
            context['variant_field_exist'] = variant_field_exist

            return render(request, 'search/search_results.html', context)


def yield_results(dataset_obj,
                  headers,
                  query,
                  nested_attribute_fields,
                  non_nested_attribute_fields,
                  dict_filter_fields,
                  used_keys,
                  review_status_to_filter,
                  group, FILTER_value=None, QUAL_value=None):

    es = Elasticsearch(host=dataset_obj.es_host)
    # query = json.dumps(query)
    header_keys = [ele.display_text for ele in headers]
    yield header_keys
    for ele in helpers.scan(es,
                            query=query,
                            scroll=u'5m',
                            size=1000,
                            preserve_order=False,
                            index=dataset_obj.es_index_name,
                            doc_type=dataset_obj.es_type_name):
        result = ele['_source']
        es_id = ele['_id']
        inner_hits = ele.get('inner_hits')
        variant_review_status = get_variant_review_status(es_id, group)
        if review_status_to_filter and variant_review_status not in review_status_to_filter:
            continue

        if inner_hits:
            for key, value in inner_hits.items():
                if key not in result:
                    result[key] = []
                hits_hits_array = inner_hits[key]['hits']['hits']
                for hit in hits_hits_array:
                    tmp_hit_dict = {}
                    if hit['_source'].get(key):
                        # for Elasticsearch 5
                        for hit_key, hit_value in hit['_source'][key].items():
                            tmp_hit_dict[hit_key] = hit_value
                    else:
                        # for Elasticsearch 6
                        for hit_key, hit_value in hit['_source'].items():
                            tmp_hit_dict[hit_key] = hit_value

                    if tmp_hit_dict:
                        result[key].append(tmp_hit_dict)

        final_results = []
        if nested_attribute_fields:
            combined = False
            combined_nested = None
            for idx, path in enumerate(nested_attribute_fields):
                if path.startswith('FILTER') or path.startswith('QUAL'):
                    continue
                if path not in result:
                    continue

                if not combined:
                    combined_nested = result[path]
                    combined = True
                    continue
                else:
                    combined_nested = list(itertools.product(
                        combined_nested, result[path]))
                    combined_nested = merge_two_dicts_array(combined_nested)

            tmp_non_nested = subset_dict(result, non_nested_attribute_fields)
            if combined_nested:
                tmp_output = list(itertools.product(
                    [tmp_non_nested, ], combined_nested))
                for x, y in tmp_output:
                    tmp = merge_two_dicts(x, y)
                    if result.get('FILTER'):
                        tmp['FILTER'] = result['FILTER']
                    if result.get('QUAL'):
                        tmp['QUAL'] = result['QUAL']
                    if tmp not in final_results:
                        final_results.append(tmp)
            else:
                if result not in final_results:
                    final_results.append(result)

        else:
            final_results = [result, ]

        for idx, result in enumerate(final_results):
            tmp = []

            for header in headers:
                path = header.path
                if path in ['FILTER', 'QUAL']:
                    if path == "FILTER":
                        data = result.get(path)
                        tmp.append('; '.join(["%s %s" % (
                            ele.get('FILTER_label', ""), ele.get('FILTER_value')) for ele in data]))
                    elif path == "QUAL":
                        data = result.get(path)
                        tmp.append('; '.join(
                            ["%s %s" % (ele.get('QUAL_label', ""), ele.get('QUAL_value')) for ele in data]))
                else:
                    tmp.append(str(result.get(header.es_name, None)))
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
    review_status_data = request.POST.get(
        'download_review_status_to_filter', None)
    review_status_to_filter = []
    if review_status_data:
        for ele in review_status_data.split("&"):
            if 'review_status_to_filter' in ele:
                tmp_val = ele.split('=')[1]
                review_status_to_filter.append(tmp_val)

    search_log_obj_id = request.POST['search_log_obj_id']
    search_log_obj = SearchLog.objects.get(id=search_log_obj_id)
    dataset_obj = search_log_obj.dataset
    headers = search_log_obj.headers
    headers = [ele.object for ele in serializers.deserialize("json", headers)]
    query = json.loads(search_log_obj.query)
    query = json.loads(query)
    if search_log_obj.nested_attribute_fields:
        nested_attribute_fields = json.loads(
            search_log_obj.nested_attribute_fields)
    else:
        nested_attribute_fields = []

    if search_log_obj.non_nested_attribute_fields:
        non_nested_attribute_fields = json.loads(
            search_log_obj.non_nested_attribute_fields)
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

    try:
        FILTER_field_obj = FilterField.objects.get(
            dataset=dataset_obj, es_name='FILTER_value')
        QUAL_field_obj = FilterField.objects.get(
            dataset=dataset_obj, es_name='QUAL_value')
        if str(FILTER_field_obj.id) in dict_filter_fields and str(QUAL_field_obj.id) in dict_filter_fields:
            FILTER_value = dict_filter_fields.get(str(FILTER_field_obj.id))[0]
            QUAL_value = float(dict_filter_fields.get(
                str(QUAL_field_obj.id))[0])
        else:
            FILTER_value = None
            QUAL_value = None
    except:
        FILTER_value = None
        QUAL_value = None

    pseudo_buffer = Echo()
    writer = csv.writer(pseudo_buffer, delimiter='\t')

    if request.user.groups.count() != 1 and not request.user.is_anonymous:
        return HttpResponse(status=400)
    elif request.user.is_anonymous:
        group = 'anonymous'
    else:
        group = request.user.groups.all()[:1].get()

    results = yield_results(dataset_obj,
                            headers,
                            query,
                            nested_attribute_fields,
                            non_nested_attribute_fields,
                            dict_filter_fields,
                            used_keys,
                            review_status_to_filter, group, FILTER_value=FILTER_value, QUAL_value=QUAL_value)
    response = StreamingHttpResponse(
        (writer.writerow(row) for row in results if row), content_type="text/csv")
    response['Content-Disposition'] = 'attachment; filename="results.tsv"'
    return response


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

    conserved_elements = [
        "gerp_plus_gt2",
        "genomicSuperDups",
        "phastConsElements100way",
        "phastConsElements46way",
        "tfbsConsSites",

    ]

    conserved_elements_available = True if any(
        True for ele in conserved_elements if result.get(ele)) else False

    fsp = FilterSubPanel.objects.get(
        dataset=dataset, filter_panel__name='Pathogenic Prediction', name='Coding Region')
    coding_regions = [ele.display_text for ele in fsp.filter_fields.all()]

    splice_junctions = ["dbscSNV_ADA_SCORE", "dbscSNV_RF_SCORE"]

    coding_region_available = True if any(
        True for ele in coding_regions if result.get(ele)) else False
    splice_junctions_available = True if any(
        True for ele in splice_junctions if result.get(ele)) else False

    baminfo_exists = SampleBamInfo.objects.filter(
        dataset__id=dataset_id).exists()

    context["baminfo_exists"] = baminfo_exists
    context["result"] = result
    context["conserved_elements_available"] = conserved_elements_available
    context["coding_region_available"] = coding_region_available
    context["splice_junctions_available"] = splice_junctions_available
    context["coding_regions"] = coding_regions
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
        form = SaveSearchForm(request.user, dataset,
                              filters_used, attributes_selected, request.POST)
        if form.is_valid():
            data = form.cleaned_data
            user = data.get('user')
            dataset = data.get('dataset')
            filters_used = data.get('filters_used')
            attributes_selected = data.get('attributes_selected')
            description = data.get('description')
            SavedSearch.objects.create(user=user, dataset=dataset, filters_used=filters_used,
                                       attributes_selected=attributes_selected, description=description)
            return redirect('saved-search-list')
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


def update_variant_review_status(request):
    if request.POST:
        # make sure user only belongs to one group
        if request.user.groups.count() != 1:
            print('No Group')
            return HttpResponse(status=400)
        else:
            group = request.user.groups.all()[:1].get()

        variant_es_id = request.POST.get("es_id")
        variant = request.POST.get("variant")
        datasetid = request.POST.get("datasetid")
        dataset_obj = Dataset.objects.get(id=datasetid)

        new_variant_review_status = request.POST.get("variant_review_status")

        variant_review_status_obj = get_variant_review_status_obj(
            variant_es_id, group)
        if new_variant_review_status == 'not_reviewed' and variant_review_status_obj:
            variant_review_status_obj.delete()
            return HttpResponse(status=200)
        elif variant_review_status_obj and variant_review_status_obj.status != new_variant_review_status:
            variant_review_status_obj.status = new_variant_review_status
            variant_review_status_obj.save()
            variant_review_status_history = VariantReviewStatusHistory.objects.create(variant_review_status=variant_review_status_obj,
                                                                                      user=request.user,
                                                                                      status=new_variant_review_status)

            return HttpResponse(status=200)
        elif not variant_review_status_obj:
            variant_review_status_obj, _ = VariantReviewStatus.objects.get_or_create(group=group,
                                                                                     variant_es_id=variant_es_id,
                                                                                     variant=variant,
                                                                                     dataset=dataset_obj,
                                                                                     status=new_variant_review_status)
            variant_review_status_history = VariantReviewStatusHistory.objects.create(variant_review_status=variant_review_status_obj,
                                                                                      user=request.user,
                                                                                      status=new_variant_review_status)
            return HttpResponse(status=200)
        else:
            return HttpResponse(status=400)


def list_variant_review_status(request):
    user_variants = VariantReviewStatus.objects.filter(
        user=request.user).prefetch_related('user')
    group_variants = VariantReviewStatus.objects.filter(shared_with_group__pk__in=request.user.groups.values_list(
        'pk', flat=True)).exclude(user=request.user).prefetch_related('user')

    context = {}
    context['user_variants'] = user_variants
    context['group_variants'] = group_variants
    return render(request, 'search/variant_review_list.html', context)


def delete_variant(request, pk):
    try:
        variant_review_status_obj = VariantReviewStatus.objects.get(
            pk=pk, user=request.user)
        review_status = variant_review_status_obj.variant_review_status
        variant_review_status_obj.delete()
        # print(reverse('list-variant-status', kwargs={'review_status': review_status}))
        return redirect(reverse('list-variant-status', kwargs={'review_status': review_status}))
    except:
        raise PermissionDenied


class VariantReviewStatusUpdateView(UpdateView):
    model = VariantReviewStatus
    form_class = VariantStatusReviewUpdateForm
    template_name = 'search/variant_review_status_update.html'


def list_variant_review_summary(request):
    MAX_SUMMARY_LEN = 5

    if request.user.groups.count() != 1:
        print('No Group')
        return HttpResponse(status=400)
    else:
        group = request.user.groups.all()[:1].get()

    group_approved = VariantReviewStatus.objects.filter(
        group=group, status='approved')
    group_approved_count = group_approved.count()
    group_approved = group_approved[:MAX_SUMMARY_LEN]

    group_rejected = VariantReviewStatus.objects.filter(
        group=group, status='rejected')
    group_rejected_count = group_rejected.count()
    group_rejected = group_rejected[:MAX_SUMMARY_LEN]

    group_pending = VariantReviewStatus.objects.filter(
        group=group, status='pending')
    group_pending_count = group_pending.count()
    group_pending = group_pending[:MAX_SUMMARY_LEN]

    context = {}

    context['group_approved'] = group_approved
    context['group_approved_count'] = group_approved_count

    context['group_rejected'] = group_rejected
    context['group_rejected_count'] = group_rejected_count

    context['group_pending'] = group_pending
    context['group_pending_count'] = group_pending_count

    context['any_variants'] = any(
        [group_approved, group_rejected, group_pending])

    return render(request, 'search/variant_review_status_summary.html', context)


def list_group_variant_status(request, review_status):
    if request.user.groups.count() != 1:
        print('No Group')
        return HttpResponse(status=400)
    else:
        group = request.user.groups.all()[:1].get()
    review_status = VariantReviewStatus.objects.filter(
        group=group, status=review_status)

    context = {}
    context['review_status'] = review_status
    context['REVIEW_STATUS_CHOICES'] = REVIEW_STATUS_CHOICES
    return render(request, 'search/list_group_review_status.html', context)
