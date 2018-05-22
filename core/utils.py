import copy
import itertools
import json
import pprint
from collections import defaultdict
from operator import itemgetter

import elasticsearch
import memcache
from django.core import serializers
from natsort import natsorted

from core import models as core_models
#from core import forms as core_forms


def flush_memcache():
    mc = memcache.Client(['127.0.0.1:11211'], debug=0)
    mc.flush_all()


def get_values_from_es(dataset_es_index_name,
                       dataset_es_type_name,
                       dataset_es_host,
                       dataset_es_port,
                       field_es_name,
                       field_path):

    es = elasticsearch.Elasticsearch(
        host=dataset_es_host, port=dataset_es_port)

    if not field_path:
        body_non_nested_template = """
            {
                "size": 0,
                "aggs" : {
                    "values" : {
                        "terms" : { "field" : "%s", "size" : 30000 }
                    }
                }
            }
        """
        body = body_non_nested_template % (field_es_name)
        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["buckets"] if ele['key']])

    elif field_path:
        body_nested_template = """
            {
                "size": 0,
                "aggs" : {
                    "values" : {
                        "nested" : {
                            "path" : "%s"
                        },
                        "aggs" : {
                            "values" : {"terms" : {"field" : "%s.%s", "size" : 30000}}
                        }
                    }
                }
            }
        """
        body = body_nested_template % (field_path,
                                       field_path,
                                       field_es_name)

        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["values"]["buckets"] if ele['key']])


def get_es_document(es, index_name, type_name, es_id):
    result = es.get(index=index_name, doc_type=type_name, id=es_id)
    return result["_source"]


class ElasticSearchFilter:

    def __init__(self):
        self.query_string = {}
        self.size = 400
        self.source = []
        self.inner_hits_source = {}

        self.filter_term = []
        self.filter_terms = []
        self.nested_filter_term = {}
        self.nested_filter_terms = {}

        self.must_wildcard = []
        self.nested_must_wildcard = []

        self.filter_range_gt = []
        self.filter_range_gte = []
        self.filter_range_lt = []
        self.filter_range_lte = []
        self.nested_filter_range_gte = {}
        self.nested_filter_range_lte = {}

        self.filter_exists = []
        self.must_not_exists = []
        self.nested_filter_exists = {}

    def add_source(self, field_name):
        self.source.append(field_name)

    def get_source(self):
        return self.source

    def update_inner_hits_source(self, update_dict):
        self.inner_hits_source.update(update_dict)

    def get_inner_hits_source(self):
        return self.inner_hits_source

    def get_inner_hits_source_path(self, path):
        return self.inner_hits_source[path]

    def add_filter_term(self, field_name, value):
        self.filter_term.append((field_name, value))

    def get_filter_term(self):
        return self.filter_term

    def add_filter_terms(self, field_name, value):
        self.filter_terms.append((field_name, value))

    def get_filter_terms(self):
        return self.filter_terms

    def add_nested_filter_term(self, field_name, value, path):
        if path not in self.nested_filter_term:
            self.nested_filter_term[path] = []
        self.nested_filter_term[path].append((field_name, value))

    def get_nested_filter_term(self):
        if list(self.nested_filter_term):
            return self.nested_filter_term

    def add_nested_filter_terms(self, field_name, value, path):
        if path not in self.nested_filter_terms:
            self.nested_filter_terms[path] = []
        self.nested_filter_terms[path].append((field_name, value))

    def get_nested_filter_terms(self):
        if list(self.nested_filter_terms):
            return self.nested_filter_terms

    def add_must_wildcard(self, field_name, value):
        self.must_wildcard.append((field_name, value))

    def get_must_wildcard(self):
        return self.must_wildcard

    def add_nested_must_wildcard(self, field_name, value, path):
        self.nested_must_wildcard.append((field_name, value, path))

    def get_nested_must_wildcard(self):
        return self.nested_must_wildcard

    def add_filter_range_gt(self, field_name, value):
        self.filter_range_gt.append((field_name, float(value)))

    def get_filter_range_gt(self):
        return self.filter_range_gt

    def add_filter_range_gte(self, field_name, value):
        self.filter_range_gte.append((field_name, float(value)))

    def get_filter_range_gte(self):
        return self.filter_range_gte

    def add_filter_range_lt(self, field_name, value):
        self.filter_range_lt.append((field_name, float(value)))

    def get_filter_range_lt(self):
        return self.filter_range_lt

    def add_filter_range_lte(self, field_name, value):
        self.filter_range_lte.append((field_name, float(value)))

    def get_filter_range_lte(self):
        return self.filter_range_lte

    def add_nested_filter_range_gte(self, field_name, value, path):
        if path not in self.nested_filter_range_gte:
            self.nested_filter_range_gte[path] = []
        self.nested_filter_range_gte[path].append((field_name, float(value)))

    def get_nested_filter_range_gte(self):
        if list(self.nested_filter_range_gte):
            return self.nested_filter_range_gte

    def add_nested_filter_range_lte(self, field_name, value, path):
        if path not in self.nested_filter_range_lte:
            self.nested_filter_range_lte[path] = []
        self.nested_filter_range_lte[path].append((field_name, float(value)))

    def get_nested_filter_range_lte(self):
        if list(self.nested_filter_range_lte):
            return self.nested_filter_range_lte

    def add_filter_exists(self, field_name, value):
        self.filter_exists.append((field_name, value))

    def get_filter_exists(self):
        return self.filter_exists

    def add_must_not_exists(self, field_name, value):
        self.must_not_exists.append((field_name, value))

    def get_must_not_exists(self):
        return self.must_not_exists

    def add_nested_filter_exists(self, field_name, value, path):
        if path not in self.nested_filter_exists:
            self.nested_filter_exists[path] = []
        self.nested_filter_exists[path].append((field_name, value))

    def get_nested_filter_exists(self):
        if list(self.nested_filter_exists):
            return self.nested_filter_exists

    def nested_path_exists(self, query_string, path):
        for ele in query_string["query"]["bool"]["filter"]:
            if not ele.get("nested"):
                continue
            if ele["nested"]["path"] == path:
                return True
        return False

    def get_nested_dict(self, query_string, path):
        for ele in query_string["query"]["bool"]["filter"]:
            if not ele.get("nested"):
                continue
            if ele["nested"]["path"] == path:
                return ele
        nested = {
            "nested": {
                "path": path,
                "score_mode": "none",
                "query": {
                    "bool": {
                        "filter": []
                    }
                },
                "inner_hits": {
                    "_source": []
                }
            }
        }
        return nested

    def generate_query_string(self):
        query_string = {
            "size": self.size,
            "query": {
                "bool": {}
            }
        }

        if self.get_filter_term():
            filter_term = self.get_filter_term()
            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for field_name, value in filter_term:
                query_string["query"]["bool"]["filter"].append(
                    {"term": {field_name: value}})

        if self.get_filter_terms():
            filter_terms = self.get_filter_terms()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for field_name, value in filter_terms:
                query_string["query"]["bool"]["filter"].append(
                    {"terms": {field_name: value}})

        if self.get_nested_filter_term():
            nested_filter_term = self.get_nested_filter_term()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for path in list(nested_filter_term):

                nested = self.get_nested_dict(query_string, path)
                nested_path_exists = self.nested_path_exists(
                    query_string, path)

                if nested_path_exists:
                    query_string["query"]["bool"]["filter"].remove(nested)

                for field_name, value in nested_filter_term[path]:
                    path_fieldname = "%s.%s" % (path, field_name)
                    nested["nested"]["query"]["bool"]["filter"].append(
                        {"term": {path_fieldname: value}})
                    for ele in self.get_inner_hits_source_path(path):
                        if ele not in nested["nested"]["inner_hits"]["_source"]:
                            nested["nested"]["inner_hits"][
                                "_source"].append(ele)

                query_string["query"]["bool"]["filter"].append(nested)

        if self.get_nested_filter_terms():
            nested_filter_terms = self.get_nested_filter_terms()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for path in list(nested_filter_terms):
                nested = self.get_nested_dict(query_string, path)
                nested_path_exists = self.nested_path_exists(
                    query_string, path)

                if nested_path_exists:
                    query_string["query"]["bool"]["filter"].remove(nested)

                for field_name, value in nested_filter_terms[path]:
                    tmp = []
                    for ele in value:
                        tmp.extend(ele.split())
                    value = tmp
                    path_fieldname = "%s.%s" % (path, field_name)

                    nested["nested"]["query"]["bool"]["filter"].append(
                        {"terms": {path_fieldname: value}})
                    for ele in self.get_inner_hits_source_path(path):
                        if ele not in nested["nested"]["inner_hits"]["_source"]:
                            nested["nested"]["inner_hits"][
                                "_source"].append(ele)

                query_string["query"]["bool"]["filter"].append(nested)

        if self.get_nested_must_wildcard():
            nested_must_wildcard = self.get_nested_must_wildcard()

            if "must" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["must"] = []

            for field_name, value, path in nested_must_wildcard:
                nested = {"nested": {"path": path,
                                     "query": {"bool": {"must": []}}}
                          }

                path_fieldname = "%s.%s" % (path, field_name)
                nested["nested"]["query"]["bool"]["must"].append(
                    {"wildcard": {path_fieldname: "%s" % (value)}})
                for ele in self.get_inner_hits_source_path(path):
                    if ele not in nested["nested"]["inner_hits"]["_source"]:
                        nested["nested"]["inner_hits"]["_source"].append(ele)
            query_string["query"]["bool"]["must"].append(nested)

        if self.get_filter_range_gt():
            filter_range_gt = self.get_filter_range_gt()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for field_name, value in filter_range_gt:
                query_string["query"]["bool"]["filter"].append(
                    {"range": {field_name: {"gt": value}}})

        if self.get_filter_range_gte():
            filter_range_gte = self.get_filter_range_gte()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for field_name, value in filter_range_gte:
                query_string["query"]["bool"]["filter"].append(
                    {"range": {field_name: {"gte": value}}})

        if self.get_filter_range_lt():
            filter_range_lt = self.get_filter_range_lt()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for field_name, value in filter_range_lt:
                query_string["query"]["bool"]["filter"].append(
                    {"range": {field_name: {"lt": value}}})

        if self.get_filter_range_lte():
            filter_range_lte = self.get_filter_range_lte()

            for field_name, value in filter_range_lte:
                if value == "0":
                    if "must_not" not in query_string["query"]["bool"]:
                        query_string["query"]["bool"]["must_not"] = []
                    query_string["query"]["bool"]["must_not"].append(
                        {"exists": {"field": field_name}})
                else:
                    if "filter" not in query_string["query"]["bool"]:
                        query_string["query"]["bool"]["filter"] = []
                    query_string["query"]["bool"]["filter"].append(
                        {"range": {field_name: {"lte": value}}})

        if self.get_nested_filter_range_lte():
            nested_filter_range_lte = self.get_nested_filter_range_lte()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for path in list(nested_filter_range_lte):

                nested = self.get_nested_dict(query_string, path)
                nested_path_exists = self.nested_path_exists(
                    query_string, path)

                if nested_path_exists:
                    query_string["query"]["bool"]["filter"].remove(nested)

                for field_name, value in nested_filter_range_lte[path]:
                    path_fieldname = "%s.%s" % (path, field_name)
                    nested["nested"]["query"]["bool"]["filter"].append(
                        {"range": {path_fieldname: {"lte": value}}})
                    for ele in self.get_inner_hits_source_path(path):
                        if ele not in nested["nested"]["inner_hits"]["_source"]:
                            nested["nested"]["inner_hits"][
                                "_source"].append(ele)

                    query_string["query"]["bool"]["filter"].append(nested)

        if self.get_nested_filter_range_gte():
            nested_filter_range_gte = self.get_nested_filter_range_gte()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for path in list(nested_filter_range_gte):

                nested = self.get_nested_dict(query_string, path)
                nested_path_exists = self.nested_path_exists(
                    query_string, path)

                if nested_path_exists:
                    query_string["query"]["bool"]["filter"].remove(nested)

                for field_name, value in nested_filter_range_gte[path]:
                    path_fieldname = "%s.%s" % (path, field_name)
                    nested["nested"]["query"]["bool"]["filter"].append(
                        {"range": {path_fieldname: {"gte": value}}})
                    for ele in self.get_inner_hits_source_path(path):
                        if ele not in nested["nested"]["inner_hits"]["_source"]:
                            nested["nested"]["inner_hits"][
                                "_source"].append(ele)

                    query_string["query"]["bool"]["filter"].append(nested)

        if self.get_filter_exists():
            filter_exists = self.get_filter_exists()
            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for field_name, value in filter_exists:
                query_string["query"]["bool"]["filter"].append(
                    {"exists": {"field": field_name}})

        if self.get_must_not_exists():
            must_not_exists = self.get_must_not_exists()
            if "must_not" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["must_not"] = []

            for field_name, value in must_not_exists:
                query_string["query"]["bool"]["must_not"].append(
                    {"exists": {"field": field_name + value.strip()}})

        if self.get_nested_filter_exists():
            nested_filter_exists = self.get_nested_filter_exists()

            if "filter" not in query_string["query"]["bool"]:
                query_string["query"]["bool"]["filter"] = []

            for path in list(nested_filter_exists):
                nested = self.get_nested_dict(query_string, path)
                nested_path_exists = self.nested_path_exists(
                    query_string, path)

                if nested_path_exists:
                    query_string["query"]["bool"]["filter"].remove(nested)

                for field_name, value in nested_filter_exists[path]:
                    path_fieldname = "%s.%s" % (path, field_name)
                    nested["nested"]["query"]["bool"]["filter"].append(
                        {"exists": {"field": path_fieldname}})
                    for ele in self.get_inner_hits_source_path(path):
                        if ele not in nested["nested"]["inner_hits"]["_source"]:
                            nested["nested"]["inner_hits"][
                                "_source"].append(ele)

                    query_string["query"]["bool"]["filter"].append(nested)

        if self.get_source():
            query_string["_source"] = sorted(list(set(self.get_source())))

        if not query_string["query"]["bool"]:
            query_string.pop("query")

        return query_string


class BaseElasticSearchQueryDSL:

    def __init__(self, dataset_obj, filter_form_data, attribute_form_data, attribute_order):
        self.dataset_obj = dataset_obj
        self.filter_form_data = filter_form_data
        self.attribute_form_data = attribute_form_data
        self.attribute_order = attribute_order
        self.non_nested_attributes_selected = []
        self.nested_attributes_selected = {}
        self.non_nested_filters_applied = []
        self.nested_filters_applied = {}
        self.nested_attribute_fields = []
        self.non_nested_attribute_fields = []
        self.filters_used = {}
        self.attributes_selected = []
        self.base_query_body = None
        self.base_query_header = None

    def determine_selected_attirbute_fields(self):
        for attribute_field_obj in core_models.AttributeField.objects.filter(id__in=self.attribute_form_data.keys()):
            key = str(attribute_field_obj.id)
            val = self.attribute_form_data[key]
            if val:
                es_name, path = attribute_field_obj.es_name, attribute_field_obj.path
                if path:
                    if path not in self.nested_attributes_selected:
                        self.nested_attributes_selected[path] = []
                    self.nested_attributes_selected[
                        path].append('%s.%s' % (path, es_name))

                    if path not in self.nested_attribute_fields:
                        self.nested_attribute_fields.append(path)
                else:
                    self.non_nested_attributes_selected.append(es_name)
                    self.non_nested_attribute_fields.append(es_name)

        self.nested_attribute_fields = list(set(self.nested_attribute_fields))

    def determine_data_table_header(self):
        header = []
        nested_attributes_selected = defaultdict(list)

        pks_orders = {}
        for key, val in self.attribute_order.items():
            order, pk = val.split('-')
            pks_orders[int(pk)] = order

        for attribute_field_obj in core_models.AttributeField.objects.filter(id__in=list(pks_orders)):
            es_name = attribute_field_obj.es_name
            path = attribute_field_obj.path
            if path:
                nested_attributes_selected[path].append(es_name)
            header.append(
                (int(pks_orders[attribute_field_obj.id]), attribute_field_obj))

        header = sorted(header, key=itemgetter(0))
        _, header = zip(*header)
        self.base_query_header = header

        for ele in header:
            self.attributes_selected.append('%d' % (ele.id))

    def generate_base_query_from_filters(self):
        es_filter = ElasticSearchFilter()
        keys = self.filter_form_data.keys()
        dict_filter_fields = {}
        source_fields = []
        inner_hits_source_fields = {}

        for filter_field_obj in core_models.FilterField.objects.filter(id__in=keys).select_related('widget_type', 'form_type', 'es_filter_type'):
            key = str(filter_field_obj.id)
            data = self.filter_form_data[key]

            if not data:
                continue

            self.filters_used[key] = data
            filter_field_pk = filter_field_obj.id
            es_name = filter_field_obj.es_name
            path = filter_field_obj.path
            es_filter_type = filter_field_obj.es_filter_type.name

            # Elasticsearch source fields use path for nested fields and
            # the actual field name for non-nested fields
            if path:
                source_name = '%s.%s' % (path, es_name)
                if path not in self.nested_filters_applied:
                    self.nested_filters_applied[path] = []
                if source_name not in self.nested_filters_applied[path]:
                    self.nested_filters_applied[path].append(
                        '%s.%s' % (path, es_name))
            else:
                self.non_nested_filters_applied.append(es_name)

            if path:
                dict_filter_fields[filter_field_pk] = []

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

        attributes_paths = self.nested_attributes_selected.keys()
        filter_paths = self.nested_filters_applied.keys()

        possible_paths = [
            ele for ele in attributes_paths if ele in filter_paths]

        if self.nested_filters_applied:
            inner_hits_source_fields = copy.deepcopy(
                self.nested_filters_applied)
            for pp_ele in possible_paths:
                if pp_ele not in inner_hits_source_fields[pp_ele]:
                    for ele in self.nested_attributes_selected[pp_ele]:
                        if ele not in inner_hits_source_fields[pp_ele]:
                            inner_hits_source_fields[pp_ele].extend(
                                self.nested_attributes_selected[pp_ele])
                    self.nested_attributes_selected.pop(pp_ele)

        source_fields.extend(self.non_nested_attributes_selected)

        if self.nested_attributes_selected:
            for nested_attribute_selected_key, nested_attribute_selected_value in self.nested_attributes_selected.items():
                source_fields.extend(nested_attribute_selected_value)

        for field in source_fields:
            es_filter.add_source(field)

        if inner_hits_source_fields:
            es_filter.update_inner_hits_source(inner_hits_source_fields)

        body = es_filter.generate_query_string()

        self.base_query_body = body

    def default_process_forms(self):
        self.determine_selected_attirbute_fields()
        self.determine_data_table_header()
        self.generate_base_query_from_filters()

    def process_forms(self):
        self.default_process_forms()

    def get_query_body(self):
        return self.base_query_body

    def get_header(self):
        return self.base_query_header

    def get_nested_attribute_fields(self):
        return self.nested_attribute_fields

    def get_non_nested_attribute_fields(self):
        return self.non_nested_attribute_fields

    def get_filters_used(self):
        return self.filters_used

    def get_attributes_selected(self):
        return self.attributes_selected


class BaseElasticSearchQueryExecutor:

    def __init__(self, dataset_obj, query_body, elasticsearch_terminate_after=0):
        self.dataset_obj = dataset_obj
        self.query_body = query_body
        self.elasticsearch_terminate_after = elasticsearch_terminate_after
        self.elasticsearch_response = None

    def excecute_elasticsearch_query(self):

        es = elasticsearch.Elasticsearch(
            host=self.dataset_obj.es_host, port=self.dataset_obj.es_port)
        response = es.search(
            index=self.dataset_obj.es_index_name,
            doc_type=self.dataset_obj.es_type_name,
            body=json.dumps(self.query_body),
            request_timeout=120,
            terminate_after=self.elasticsearch_terminate_after)

        self.elasticsearch_response = response

    def get_elasticsearch_response(self):
        self.excecute_elasticsearch_query()
        return self.elasticsearch_response

    def get_elasticsearch_response_time(self):
        return self.elasticsearch_response.get('took')


class BaseElasticsearchResponseParser:
    flatten_nested = True
    maximum_table_size = 400
    fields_to_skip_flattening = []

    def __init__(self, elasticsearch_response, non_nested_attribute_fields, nested_attribute_fields):
        self.elasticsearch_response = elasticsearch_response
        self.non_nested_attribute_fields = non_nested_attribute_fields
        self.nested_attribute_fields = nested_attribute_fields
        self.results = []
        self.flattened_results = []

    def subset_dict(self, input, keys):

        return {key: input[key] for key in keys if input.get(key) is not None}

    def merge_two_dicts(self, x, y):
        z = x.copy()
        z.update(y)
        return z

    def merge_two_dicts_array(self, input):
        output = []
        for x, y in input:
            output.append(self.merge_two_dicts(x, y))

        return output

    def extract_nested_results_from_elasticsearch_response(self):
        hits = self.elasticsearch_response['hits']['hits']
        for hit in hits:
            tmp_source = hit['_source']
            es_id = hit['_id']
            inner_hits = hit.get('inner_hits')

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
            self.results.append(tmp_source)

    def flatten_nested_results(self):

        if self.nested_attribute_fields:
            flattened_results = []
            results_count = 0
            for idx, result in enumerate(self.results):
                if results_count > self.maximum_table_size:
                    break
                combined = False
                combined_nested = None
                for idx, path in enumerate(self.nested_attribute_fields):
                    for field_to_skip in self.fields_to_skip_flattening:
                        if path.startswith(field_to_skip):
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
                        combined_nested = self.merge_two_dicts_array(
                            combined_nested)

                tmp_non_nested = self.subset_dict(
                    result, self.non_nested_attribute_fields)
                if combined_nested:
                    tmp_output = list(itertools.product(
                        [tmp_non_nested, ], combined_nested))

                    for x, y in tmp_output:
                        tmp = self.merge_two_dicts(x, y)
                        tmp["es_id"] = result["es_id"]

                        for field_to_skip in self.fields_to_skip_flattening:
                            if result.get(field_to_skip):
                                tmp[field_to_skip] = result[field_to_skip]

                        if tmp not in flattened_results:
                            flattened_results.append(tmp)
                            results_count += 1
                else:
                    if result not in flattened_results:
                        flattened_results.append(result)
                        results_count += 1
        else:
            flattened_results = self.results

        self.flattened_results = flattened_results

    def get_results(self):
        self.extract_nested_results_from_elasticsearch_response()
        if self.flatten_nested:
            self.flatten_nested_results()
            return self.flattened_results[:self.maximum_table_size]
        else:
            return self.results[:self.maximum_table_size]


class BaseSearchElasticsearch:

    def __init__(self, *args, **kwargs):
        self.user = kwargs.get('user')
        self.dataset_obj = kwargs.get('dataset_obj')
        self.filter_form_data = kwargs.get('filter_form_data')
        self.attribute_form_data = kwargs.get('attribute_form_data')
        self.attribute_order = kwargs.get('attribute_order')
        self.elasticsearch_dsl_class = kwargs.get(
            'elasticsearch_dsl_class')
        self.elasticsearch_query_executor_class = kwargs.get(
            'elasticsearch_query_executor_class')
        self.elasticsearch_response_parser_class = kwargs.get(
            'elasticsearch_response_parser_class')
        self.header = None
        self.results = None
        self.query_body = None
        self.elasticsearch_response_time = None
        self.nested_attribute_fields = None
        self.non_nested_attribute_fields = None
        self.filters_used = None
        self.attributes_selected = None
        self.search_log_id = None

    def run_elasticsearch_dsl(self):
        elasticsearch_dsl = self.elasticsearch_dsl_class(
            self.dataset_obj, self.filter_form_data, self.attribute_form_data, self.attribute_order)
        elasticsearch_dsl.process_forms()
        self.header = elasticsearch_dsl.get_header()
        self.query_body = elasticsearch_dsl.get_query_body()
        self.nested_attribute_fields = elasticsearch_dsl.get_nested_attribute_fields()
        self.non_nested_attribute_fields = elasticsearch_dsl.get_non_nested_attribute_fields()
        self.filters_used = elasticsearch_dsl.get_filters_used()
        self.attributes_selected = elasticsearch_dsl.get_attributes_selected()

    def run_elasticsearch_query_executor(self):
        elasticsearch_query_executor = self.elasticsearch_query_executor_class(
            self.dataset_obj, self.query_body)
        self.elasticsearch_response = elasticsearch_query_executor.get_elasticsearch_response()
        self.elasticsearch_response_time = elasticsearch_query_executor.get_elasticsearch_response_time()

    def run_elasticsearch_response_parser_class(self):
        elasticsearch_response_parser = self.elasticsearch_response_parser_class(
            self.elasticsearch_response, self.non_nested_attribute_fields, self.nested_attribute_fields)
        self.results = elasticsearch_response_parser.get_results()

    def log_search(self):

        # convert to json
        header_json = serializers.serialize("json", self.header)
        query_body_json = json.dumps(self.query_body)

        nested_attribute_fields_json = json.dumps(
            self.nested_attribute_fields) if self.nested_attribute_fields else None
        non_nested_attribute_fields_json = json.dumps(
            self.non_nested_attribute_fields) if self.non_nested_attribute_fields else None

        search_log_obj = core_models.SearchLog.objects.create(
            dataset=self.dataset_obj,
            header=header_json,
            query=query_body_json,
            nested_attribute_fields=nested_attribute_fields_json,
            non_nested_attribute_fields=non_nested_attribute_fields_json,
            filters_used=self.filters_used,
            attributes_selected=self.attributes_selected
        )

        if self.user.is_authenticated:
            search_log_obj.user = self.user
            search_log_obj.save()

        self.search_log_id = search_log_obj.id

    def search(self):
        self.run_elasticsearch_dsl()
        self.run_elasticsearch_query_executor()
        self.run_elasticsearch_response_parser_class()
        self.log_search()

    def get_header(self):
        return self.header

    def get_results(self):
        return self.results

    def get_elasticsearch_response_time(self):
        return self.elasticsearch_response_time

    def get_search_log_id(self):
        return self.search_log_id

    def get_filters_used(self):
        return self.filters_used

    def get_attributes_selected(self):
        return self.attributes_selected


class BaseDownloadAllResults:
    fields_to_skip_flattening = []

    def __init__(self, search_log_obj):
        self.search_log_obj = search_log_obj
        self.results = []
        self.flattened_results = []
        self.header = [ele.object for ele in serializers.deserialize("json", self.search_log_obj.header)]
        self.query_body = json.loads(self.search_log_obj.query)
        if self.search_log_obj.nested_attribute_fields:
            self.nested_attribute_fields = json.loads(
                self.search_log_obj.nested_attribute_fields)
        else:
            nested_attribute_fields = []

        if self.search_log_obj.non_nested_attribute_fields:
            self.non_nested_attribute_fields = json.loads(
                self.search_log_obj.non_nested_attribute_fields)
        else:
            non_nested_attribute_fields = []

    def merge_two_dicts(self, x, y):
        z = x.copy()
        z.update(y)
        return z

    def merge_two_dicts_array(self, input):
        output = []
        for x, y in input:
            output.append(self.merge_two_dicts(x, y))

        return output

    def generate_row(self, header, tmp_source):
        tmp = []
        for ele in header:
            tmp.append(str(tmp_source.get(ele.es_name, None)))

        return tmp

    def yield_rows(self):

        header_keys = [ele.display_text for ele in self.header]
        yield header_keys

        es = elasticsearch.Elasticsearch(
            host=self.search_log_obj.dataset.es_host)
        for hit in elasticsearch.helpers.scan(es,
                                              query=self.query_body,
                                              scroll=u'5m',
                                              size=1000,
                                              preserve_order=False,
                                              index=self.search_log_obj.dataset.es_index_name,
                                              doc_type=self.search_log_obj.dataset.es_type_name):
            tmp_source = hit['_source']
            es_id = hit['_id']
            inner_hits = hit.get('inner_hits')

            tmp_source['es_id'] = es_id
            if inner_hits:
                for key, value in inner_hits.items():
                    if key not in tmp_source:
                        tmp_source[key] = []
                    hits_hits_array = inner_hits[key]['hits']['hits']
                    for hit in hits_hits_array:
                        tmp_hit_dict = {}
                        for hit_key, hit_value in hit['_source'].items():
                            tmp_hit_dict[hit_key] = hit_value

                        if tmp_hit_dict:
                            tmp_source[key].append(tmp_hit_dict)

            row = self.generate_row(self.header, tmp_source)
            yield row

    def flatten_nested_results(self):

        if self.nested_attribute_fields:
            flattened_results = []
            results_count = 0
            for idx, result in enumerate(self.results):
                if results_count > self.maximum_table_size:
                    break
                combined = False
                combined_nested = None
                for idx, path in enumerate(self.nested_attribute_fields):
                    for field_to_skip in self.fields_to_skip_flattening:
                        if path.startswith(field_to_skip):
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
                        combined_nested = self.merge_two_dicts_array(
                            combined_nested)

                tmp_non_nested = self.subset_dict(
                    result, self.non_nested_attribute_fields)
                if combined_nested:
                    tmp_output = list(itertools.product(
                        [tmp_non_nested, ], combined_nested))

                    for x, y in tmp_output:
                        tmp = self.merge_two_dicts(x, y)
                        tmp["es_id"] = result["es_id"]

                        for field_to_skip in self.fields_to_skip_flattening:
                            if result.get(field_to_skip):
                                tmp[field_to_skip] = result[field_to_skip]

                        if tmp not in flattened_results:
                            flattened_results.append(tmp)
                            results_count += 1
                else:
                    if result not in flattened_results:
                        flattened_results.append(result)
                        results_count += 1
        else:
            flattened_results = self.results

        self.flattened_results = flattened_results
