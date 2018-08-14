import copy
import datetime
import pprint
import sys
from collections import Counter, deque

import elasticsearch
from elasticsearch import helpers
from natsort import natsorted

from core.models import AttributeField, SearchLog
from core.utils import (BaseElasticSearchQueryDSL,
                        BaseElasticSearchQueryExecutor,
                        BaseElasticsearchResponseParser,
                        BaseSearchElasticsearch, get_values_from_es)

thismodule = sys.modules[__name__]

def filter_using_inner_hits(source_data, inner_hits_data):

    inner_hit_candidates = []
    for ele in inner_hits_data['hits']['hits']:
        data = ele['_source']
        inner_hit_keys = sorted(data.keys())
        test_string = ''.join(["%s:%s" % (key, str(data[key])) for key in inner_hit_keys if key.strip()])
        inner_hit_candidates.append(test_string)

    output = []
    for ele in source_data:
        test_string = ''.join(["%s:%s" % (key, str(ele[key])) for key in inner_hit_keys if key.strip()])
        if test_string in inner_hit_candidates:
            output.append(ele)

    return output


def filter_source_by_family_id(sample_data, family_id):
    output = []
    for ele in sample_data:
        if ele.get('Family_ID') == family_id:
            output.append(ele)

    return output


def extract_sample_inner_hits_as_array(inner_hits_sample):
    output = []
    for ele in inner_hits_sample:
        output.append(ele.get('_source'))
    return output


class MendelianElasticSearchQueryExecutor(BaseElasticSearchQueryExecutor):
    # pass

    def __init__(self, dataset_obj, query_body, family_dict, mendelian_analysis_type, limit_results=True, elasticsearch_terminate_after=400):
        super().__init__(dataset_obj, query_body, elasticsearch_terminate_after=400)
        self.family_dict = family_dict
        self.mendelian_analysis_type = mendelian_analysis_type
        self.family_results = {}
        self.limit_results = limit_results
        self.elasticsearch_terminate_after = elasticsearch_terminate_after


    def add_analysis_type_filter(self, analysis_type):
        query_body = copy.deepcopy(self.query_body)
        if 'query' not in query_body:
            query_body['query'] = {
                "nested": {
                    "inner_hits": {"size": 100},
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"sample.mendelian_diseases": analysis_type}}
                            ]
                        }
                    }
                }
            }
        elif query_body['query']['bool']['filter']:
            filter_array = query_body['query']['bool']['filter']
            filter_array_copy = copy.deepcopy(filter_array)
            sample_array = None
            for ele in filter_array:
                if 'nested' in ele and ele['nested']['path'] == 'sample':
                    filter_array_copy.remove(ele)
                    sample_array = ele

            if sample_array:
                sample_array['nested']['inner_hits'] = {}
                sample_array['nested']['query']['bool']['filter'].append({"term": {"sample.mendelian_diseases": analysis_type}})
                filter_array_copy.append(sample_array)
                query_body['query']['bool']['filter'] = filter_array_copy
            else:
                query_body['query']['bool']['filter'].append(
                    {
                        "nested": {
                            "inner_hits": {"size": 100},
                            "path": "sample",
                            "score_mode": "none",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {"sample.mendelian_diseases": analysis_type}}
                                    ]
                                }
                            }
                        }
                    }
                )
        return query_body


    def search(self):
        results = {
            "took": None,
            "hits": {
                "total": None,
                "hits": deque()
            }
        }
        count = 0
        start_time = datetime.datetime.now()

        es = elasticsearch.Elasticsearch(host=self.dataset_obj.es_host, port=self.dataset_obj.es_port)

        if 'CSQ_nested' in es.indices.get_mapping()[self.dataset_obj.es_index_name]['mappings'][self.dataset_obj.es_type_name]['properties']:
            annotation = 'VEP'
        elif 'ExonicFunc_refGene' in es.indices.get_mapping()[self.dataset_obj.es_index_name]['mappings'][self.dataset_obj.es_type_name]['properties']:
            annotation = 'ANNOVAR'

        query_body = self.add_analysis_type_filter(self.mendelian_analysis_type)

        pprint.pprint(query_body)
        x = datetime.datetime.now()
        for hit in helpers.scan(
                es,
                query=query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=self.dataset_obj.es_index_name,
                doc_type=self.dataset_obj.es_type_name):

            if self.limit_results and len(results['hits']['hits']) > self.elasticsearch_terminate_after:
                break

            inner_hits_sample = hit['inner_hits']['sample']['hits']['hits']
            sample_data = extract_sample_inner_hits_as_array(inner_hits_sample)

            tmp_results = hit.copy()
            tmp_results['_source']['sample'] = sample_data
            tmp_results['inner_hits']['sample']['hits']['hits'] = [
                {'_nested': {'field': 'sample'}, '_source': sample_data[0]}]
            results['hits']['hits'].append(tmp_results)
            count += 1
        elapsped_time = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

        results['took'] = elapsped_time
        results['hits']['total'] = count

        return results

    def excecute_elasticsearch_query(self):
        results = self.search()
        self.elasticsearch_response = results


class MendelianElasticsearchResponseParser(BaseElasticsearchResponseParser):
    maximum_table_size = 400


class MendelianSearchElasticsearch(BaseSearchElasticsearch):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mendelian_analysis_type = kwargs.get('mendelian_analysis_type')
        self.number_of_kindred = kwargs.get('number_of_kindred')
        self.family_dict = None

    def _get_family(self, dataset_es_index_name, dataset_es_type_name, dataset_es_host, dataset_es_port, Family_ID):
        body_template = """
          {
           "_source": false,
           "size": 1,
           "query": {
               "nested": {
                   "path": "sample",
                   "score_mode": "none",
                   "query": {
                       "bool": {
                            "must" : [{
                                "term": { "sample.Family_ID": "%s"}}],
                            "must_not": [
                                { "term": { "sample.Father_ID": -9}},
                                { "term": { "sample.Mother_ID": -9}}
                            ]
                        }
                   },
                   "inner_hits": {}
               }
           }
        }
        """
        es = elasticsearch.Elasticsearch(
            host=dataset_es_host, port=dataset_es_port)

        body = body_template % (Family_ID)
        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)

        result = results['hits']['hits'][0]['inner_hits']['sample']['hits']['hits'][0]["_source"]
        father_id = result.get('Father_ID')
        mother_id = result.get('Mother_ID')
        child_id = result.get('Sample_ID')
        child_sex = result.get('Sex')
        return (father_id, mother_id, child_id, child_sex)

    def get_family_dict(self):

        family_ids = get_values_from_es(self.dataset_obj.es_index_name,
                                        self.dataset_obj.es_type_name,
                                        self.dataset_obj.es_host,
                                        self.dataset_obj.es_port,
                                        'Family_ID',
                                        'sample')

        family_dict = {}
        for family_id in family_ids:
            father_id, mother_id, child_id, child_sex = self._get_family(self.dataset_obj.es_index_name,
                                                                         self.dataset_obj.es_type_name,
                                                                         self.dataset_obj.es_host,
                                                                         self.dataset_obj.es_port,
                                                                         family_id)
            family_dict[family_id] = {'father_id': father_id,
                                      'mother_id': mother_id, 'child_id': child_id, 'child_sex': child_sex}

        self.family_dict = family_dict

    def apply_kindred_filtering(self, elasticsearch_response):

        if not self.number_of_kindred:
            return elasticsearch_response
        else:
            results = {
                "took": None,
                "hits": {
                    "total": None,

                    "hits": deque()
                }
            }
            es_ids = [variant.get('_id') for variant in elasticsearch_response['hits']['hits']]
            es_id_counter = Counter(es_ids)

            for variant in elasticsearch_response['hits']['hits']:
                if es_id_counter.get(variant.get('_id')) > int(self.number_of_kindred):
                    results['hits']['hits'].append(variant)

            return results

    def run_elasticsearch_query_executor(self, limit_results=True):

        self.get_family_dict()

        elasticsearch_query_executor = self.elasticsearch_query_executor_class(
            self.dataset_obj, self.query_body, self.family_dict, self.mendelian_analysis_type, limit_results)
        self.elasticsearch_response = elasticsearch_query_executor.get_elasticsearch_response()
        self.elasticsearch_response = self.apply_kindred_filtering(self.elasticsearch_response)
        self.elasticsearch_response_time = elasticsearch_query_executor.get_elasticsearch_response_time()

    def search(self):
        self.run_elasticsearch_dsl()
        self.run_elasticsearch_query_executor()
        self.run_elasticsearch_response_parser_class()
        self.log_search()

    def download(self):
        self.run_elasticsearch_query_executor()
        self.run_elasticsearch_response_parser_class()
