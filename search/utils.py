from natsort import natsorted
import elasticsearch
from pprint import pprint


class ElasticSearchFilter():

    def __init__(self):
        self.query_string = {}
        self.size = 1000
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


def get_es_result(es, index_name, type_name, es_id):
    result = es.get(index=index_name, doc_type=type_name, id=es_id)
    return result["_source"]


def get_from_es(dataset_es_index_name,
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
                        "terms" : { "field" : "%s", "size" : 3000 }
                    }
                }
            }
        """
        body = body_non_nested_template % (field_es_name)
        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["buckets"]])

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
                            "values" : {"terms" : {"field" : "%s.%s", "size" : 3000}}
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
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["values"]["buckets"]])
