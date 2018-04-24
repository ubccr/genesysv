from core.utils import BaseElasticSearchQueryDSL, BaseSearchElasticsearch, BaseSearchElasticsearch, BaseElasticSearchQueryExecutor, BaseElasticsearchResponseParser
from core.models import AttributeField
import elasticsearch
from elasticsearch import helpers
from collections import deque
import datetime
import pprint
import sys
from natsort import natsorted
import copy

thismodule = sys.modules[__name__]


def get_genes_es(dataset_es_index_name,
                 dataset_es_type_name,
                 dataset_es_host,
                 dataset_es_port,
                 field_es_name,
                 field_path,
                 body):

    es = elasticsearch.Elasticsearch(
        host=dataset_es_host, port=dataset_es_port)

    if not field_path:
        body["size"] = 0
        body["aggs"] = {"values": {
            "terms": {"field": field_es_name, "size": 30000}}}
        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["buckets"] if ele['key']])

    elif field_path:
        body["size"] = 0
        body["aggs"] = {
            "values": {
                "nested": {
                    "path": field_path
                },
                "aggs": {
                    "values": {"terms": {"field": "%s.%s" % (field_path, field_es_name), "size": 30000}}
                }
            }
        }
        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["values"]["buckets"] if ele['key']])

def is_autosomal_dominant(sample_array, father_id, mother_id, child_id):

    looking_for_ids = (father_id, mother_id, child_id)
    mother_gt = father_gt = child_gt = None
    for ele in sample_array:

        sample_id = ele.get('sample_ID')

        if sample_id not in looking_for_ids:
            continue

        if sample_id == father_id:
            father_gt = ele.get('sample_GT')
        elif sample_id == mother_id:
            mother_gt = ele.get('sample_GT')
        elif sample_id == child_id:
            child_gt = ele.get('sample_GT')
            if child_gt not in ['0/1', '0|1', '0|1']:
                None

    if not all((father_gt, mother_gt, child_gt)):
        return None

    case_1 = case_2 = False

    # Case 1
    """
    mother_genotype == '0/1' or '0|1' or '1|0',
    father_genotype == '0/0' or '0|0',
    affected child_genotype == '0/1'  or '0|1' or '1|0'.
    """
    if mother_gt in ['0/1', '0|1', '1|0', ] and father_gt in ['0/0', '0|0', ]:
        case_1 = True

    # Case 2
    """
    mother_genotype == '0/0' or '0|0',
    father_genotype == '0/1' or '0|1' or '1|0',
    affected child_genotype == '0/1'  or '0|1' or '1|0'.
    """
    if mother_gt in ['0/0', '0|0', ] and father_gt in ['0/1', '0|1', '1|0', ]:
        case_2 = True

    if any((case_1, case_2)):
        return (father_gt, mother_gt, child_gt)
    else:
        return None


def is_autosomal_recessive(sample_array, father_id, mother_id, child_id):

    looking_for_ids = (father_id, mother_id, child_id)
    mother_gt = father_gt = child_gt = None
    for ele in sample_array:

        sample_id = ele.get('sample_ID')

        if sample_id not in looking_for_ids:
            continue

        if sample_id == father_id:
            father_gt = ele.get('sample_GT')
            if father_gt in ['0/0', '0|0', ]:
                continue
        elif sample_id == mother_id:
            mother_gt = ele.get('sample_GT')
            if mother_gt in ['0/0', '0|0', ]:
                continue
        elif sample_id == child_id:
            child_gt = ele.get('sample_GT')
            if child_gt not in ['1/1', '1|1']:
                return None

    if not all((father_gt, mother_gt, child_gt)):
        return None

    # Rule
    """
    mother_genotype == '0/1' or '0|1' or '1|0',
    father_genotype == '0/1' or '0|1' or '1|0',
    affected child_genotype == '1/1' or '1|1'.
    """
    if father_gt in ['0/1', '0|1', '1|0', ] and mother_gt in ['0/1', '0|1', '1|0', ]:
        return (father_gt, mother_gt, child_gt)
    else:
        return None


def is_denovo(sample_array, father_id, mother_id, child_id):

    looking_for_ids = (father_id, mother_id, child_id)
    mother_gt = father_gt = child_gt = None
    for ele in sample_array:

        sample_id = ele.get('sample_ID')

        if sample_id not in looking_for_ids:
            continue

        if sample_id == father_id:
            father_gt = ele.get('sample_GT')
            if father_gt not in ['0/0', '0|0', ]:
                return None
        elif sample_id == mother_id:
            mother_gt = ele.get('sample_GT')
            if mother_gt not in ['0/0', '0|0', ]:
                return None
        elif sample_id == child_id:
            child_gt = ele.get('sample_GT')
            if child_gt not in ['0/1', '0|1', '0|1']:
                None

    if not all((father_gt, mother_gt, child_gt)):
        return None

    return (father_gt, mother_gt, child_gt)


def get_genotypes(sample_array, father_id, mother_id, child_id):

    looking_for_ids = (father_id, mother_id, child_id)
    mother_gt = father_gt = child_gt = None
    for ele in sample_array:

        sample_id = ele.get('sample_ID')

        if sample_id not in looking_for_ids:
            continue

        if sample_id == father_id:
            father_gt = ele.get('sample_GT')
            if father_gt not in ['0/1', '0|1', '1/0', '1|0', '0/0', '0|0']:
                return None
        elif sample_id == mother_id:
            mother_gt = ele.get('sample_GT')
            if mother_gt not in ['0/1', '0|1', '1/0', '1|0', '0/0', '0|0']:
                return None
        elif sample_id == child_id:
            child_gt = ele.get('sample_GT')

    if all((father_gt, mother_gt, child_gt)):
        return [father_gt, mother_gt, child_gt]
    else:
        return None

def are_variants_compound_heterozygous(variant_genotypes):
    compound_heterozygous_found = False
    gt_pair_whose_reverse_to_find = None
    compound_heterozygous_variants = []
    for ele in variant_genotypes:
        father_gt = ele[2]
        mother_gt = ele[3]

        sum_digits = sum([int(char)
                          for char in father_gt + mother_gt if char.isdigit()])

        if sum_digits != 1:
            continue

        if not gt_pair_whose_reverse_to_find:
            gt_pair_whose_reverse_to_find = [father_gt, mother_gt]
            compound_heterozygous_variants.append(ele[0])
            continue

        current_gt_pair = [father_gt, mother_gt]
        current_gt_pair.reverse()
        if gt_pair_whose_reverse_to_find == current_gt_pair:
            compound_heterozygous_variants.append(ele[0])
            compound_heterozygous_found = True

    if compound_heterozygous_found:
        return compound_heterozygous_variants
    else:
        return False


def is_compound_heterozygous_for_gene(es, dataset_obj, query, father_id, mother_id, child_id):
    compound_heterozygous_variants = []
    saved_hits = {}
    variant_genotypes = []
    for ele in helpers.scan(es,
                            query=query,
                            scroll=u'5m',
                            size=1000,
                            preserve_order=False,
                            index=dataset_obj.es_index_name,
                            doc_type=dataset_obj.es_type_name):

        result = ele['_source']
        es_id = ele['_id']
        saved_hits[es_id] = ele.copy()

        variant_id = result.get('Variant')
        length_results = len(result.get('sample'))
        variant_genotype = get_genotypes(
            result.get('sample'), father_id, mother_id, child_id)

        if variant_genotype:
            saved_hits[es_id]['_source']['father_gt'] = variant_genotype[0]
            saved_hits[es_id]['_source']['mother_gt'] = variant_genotype[1]
            saved_hits[es_id]['_source']['child_gt'] = variant_genotype[2]
            variant_genotype.insert(0, variant_id)
            variant_genotype.insert(0, es_id)
            variant_genotypes.append(variant_genotype)
    results = []
    if len(variant_genotypes) > 1:
        found_compound_heterozygous = are_variants_compound_heterozygous(
            variant_genotypes)
        if found_compound_heterozygous:
            for es_id in found_compound_heterozygous:
                results.append(saved_hits[es_id])
            return results
        else:
            return None
    else:
        return None




class MendelianElasticSearchQueryExecutor(BaseElasticSearchQueryExecutor):
    # pass

    def __init__(self, dataset_obj, query_body, father_id, mother_id, child_id, mendelian_analysis_function, elasticsearch_terminate_after=0, ):
        super().__init__(dataset_obj, query_body, elasticsearch_terminate_after=0)
        self.father_id = father_id
        self.mother_id = mother_id
        self.child_id = child_id
        self.mendelian_analysis_function = mendelian_analysis_function


    def non_gene_based_search(self):
        function = getattr(thismodule, self.mendelian_analysis_function)

        es = elasticsearch.Elasticsearch(
            host=self.dataset_obj.es_host, port=self.dataset_obj.es_port)
        tmp_results = {
            "took": None,
            "hits": {
                "total": None,

                "hits": deque()
            }
        }
        count = 0
        start_time = datetime.datetime.now()
        for hit in helpers.scan(
                es,
                query=self.query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=self.dataset_obj.es_index_name,
                doc_type=self.dataset_obj.es_type_name):

            result = hit['_source']

            analysis_output = function(result.get('sample'),
                                       self.father_id, self.mother_id, self.child_id)
            if analysis_output:
                result = hit.copy()
                result['_source']['father_gt'] = analysis_output[0]
                result['_source']['mother_gt'] = analysis_output[1]
                result['_source']['child_gt'] = analysis_output[2]
                tmp_results['hits']['hits'].append(result)
                count += 1

        elapsped_time = int(
            (datetime.datetime.now() - start_time).total_seconds() * 1000)

        tmp_results['took'] = elapsped_time
        tmp_results['hits']['total'] = count

        return tmp_results


    def gene_based_search(self):
        es = elasticsearch.Elasticsearch(
            host=self.dataset_obj.es_host, port=self.dataset_obj.es_port)
        tmp_results = {
            "took": None,
            "hits": {
                "total": None,

                "hits": deque()
            }
        }

        genes = get_genes_es(self.dataset_obj.es_index_name,
                             self.dataset_obj.es_type_name,
                             self.dataset_obj.es_host,
                             self.dataset_obj.es_port,
                             'CSQ_nested_SYMBOL',
                             'CSQ_nested',
                             self.query_body)


        count = 0
        start_time = datetime.datetime.now()
        for gene in genes:
            query_body = copy.deepcopy(self.query_body)
            if not 'query' in query_body:
                query_body['query'] = {"bool": {
                    "filter": [
                        {
                            "nested": {
                                "path": "CSQ_nested",
                                "query": {
                                    "bool": {
                                        "filter": [
                                            {
                                                "term": {
                                                    "CSQ_nested.CSQ_nested_SYMBOL": gene
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "nested": {
                                "path": "sample",
                                "query": {
                                    "bool": {
                                        "filter": [
                                            {
                                                "terms": {"sample.sample_GT": ["0|1", "1|0", "0/1", "1/0"]}
                                            },
                                            {
                                                "term": {"sample.sample_ID": self.child_id}
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }}
            elif query_body['query']['bool']['filter']:
                query_body['query']['bool']['filter'].extend(
                    ({
                        "nested": {
                            "path": "CSQ_nested",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "CSQ_nested.CSQ_nested_SYMBOL": gene
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    },
                        {
                        "nested": {
                            "path": "sample",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "terms": {"sample.sample_GT": ["0|1", "1|0", "0/1", "1/0"]}
                                        },
                                        {
                                            "term": {"sample.sample_ID": self.child_id}
                                        }
                                    ]
                                }
                            }
                        }
                    })
                )

            results = is_compound_heterozygous_for_gene(
                es, self.dataset_obj, query_body, self.father_id, self.mother_id, self.child_id)
            if results:
                count += 1
                tmp_results['hits']['hits'].extend(results)
        return tmp_results


    def excecute_elasticsearch_query(self):

        if self.mendelian_analysis_function in ['is_autosomal_dominant', 'is_autosomal_recessive', 'is_denovo']:
            results = self.non_gene_based_search()
        elif self.mendelian_analysis_function == 'is_compound_heterozygous_for_gene':
            results = self.gene_based_search()


        self.elasticsearch_response = results


class MendelianElasticsearchResponseParser(BaseElasticsearchResponseParser):
    maximum_table_size = 4000
    fields_to_skip_flattening = ['FILTER', 'QUAL']

class MendelianSearchElasticsearch(BaseSearchElasticsearch):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.father_id = kwargs.get('father_id')
        self.mother_id = kwargs.get('mother_id')
        self.child_id = kwargs.get('child_id')
        self.mendelian_analysis_type = kwargs.get('mendelian_analysis_type')

        if self.mendelian_analysis_type == 'autosomal_dominant':
            self.mendelian_analysis_function = 'is_autosomal_dominant'
        elif self.mendelian_analysis_type == 'autosomal_recessive':
            self.mendelian_analysis_function = 'is_autosomal_recessive'
        elif self.mendelian_analysis_type == 'denovo':
            self.mendelian_analysis_function = 'is_denovo'
        elif self.mendelian_analysis_type == 'compound_heterozygous':
            self.mendelian_analysis_function = 'is_compound_heterozygous_for_gene'

    def add_sample_to_source(self):
        if 'sample' not in self.query_body.get('_source'):
            self.query_body['_source'].append('sample')

    def add_autosomal_dominant_query_string(self):
        if not 'query' in self.query_body:
            self.query_body['query'] = {
                "nested": {
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"match": {
                                    "sample.sample_ID": self.child_id}}
                            ],
                            "must_not": [
                                {"match": {
                                    "sample.sample_GT": "0/0"}},
                                {"match": {
                                    "sample.sample_GT": "0|0"}},
                                {"match": {
                                    "sample.sample_GT": "1/1"}},
                                {"match": {
                                    "sample.sample_GT": "1|1"}}
                            ]
                        }
                    }
                }
            }
        elif self.query_body['query']['bool']['filter']:
            self.query_body['query']['bool']['filter'].append(
                {
                    "nested": {
                        "path": "sample",
                        "score_mode": "none",
                        "query": {
                            "bool": {
                                "filter": [
                                    {"match": {
                                        "sample.sample_ID": self.child_id}}
                                ],
                                "must_not": [
                                    {"match": {"sample.sample_GT": "0/0"}},
                                    {"match": {"sample.sample_GT": "0|0"}},
                                    {"match": {"sample.sample_GT": "1/1"}},
                                    {"match": {"sample.sample_GT": "1|1"}}
                                ]
                            }
                        }
                    }
                }
            )

    def add_autosomal_recessive_query_string(self):
        if not 'query' in self.query_body:
            self.query_body['query'] = {
                "nested": {
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"match": {
                                    "sample.sample_ID": self.child_id}}
                            ],
                            "should": [
                                {"match": {
                                    "sample.sample_GT": "1/1"}},
                                {"match": {
                                    "sample.sample_GT": "1|1"}}
                            ],
                            "minimum_should_match": 1
                        }
                    }
                }
            }
        elif self.query_body['query']['bool']['filter']:
            self.query_body['query']['bool']['filter'].append(
                {
                    "nested": {
                        "path": "sample",
                        "score_mode": "none",
                        "query": {
                            "bool": {
                                "filter": [
                                    {"match": {
                                        "sample.sample_ID": self.child_id}}
                                ],
                                "should": [
                                    {"match": {"sample.sample_GT": "1/1"}},
                                    {"match": {"sample.sample_GT": "1|1"}}
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    }
                }
            )

    def add_denovo_query_string(self):
        if not 'query' in self.query_body:
            self.query_body['query'] = {
                "nested": {
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"sample.sample_ID": self.child_id}}
                            ],
                            "must_not": [
                                {"term": {"sample.sample_GT": "0/0"}},
                                {"term": {"sample.sample_GT": "0|0"}}
                            ]
                        }
                    }
                }
            }
        elif self.query_body['query']['bool']['filter']:
            self.query_body['query']['bool']['filter'].append(
                {"nested": {
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                            "bool": {
                                "filter": [
                                    {"term": {"sample.sample_ID": self.child_id}}
                                ],
                                "must_not": [
                                    {"term": {"sample.sample_GT": "0/0"}},
                                    {"term": {"sample.sample_GT": "0|0"}}
                                ]
                            }
                    }
                }
                }
            )

    def add_gt_header_fields(self):
        father_gt_obj = AttributeField.objects.get(
            dataset=self.dataset_obj, display_text='Father GT')
        mother_gt_obj = AttributeField.objects.get(
            dataset=self.dataset_obj, display_text='Mother GT')
        child_gt_obj = AttributeField.objects.get(
            dataset=self.dataset_obj, display_text='Child GT')
        header = list(self.header)
        header.insert(0, child_gt_obj)
        header.insert(0, mother_gt_obj)
        header.insert(0, father_gt_obj)
        header = tuple(header)
        self.header = header
        self.non_nested_attribute_fields.extend(
            ('father_gt', 'mother_gt', 'child_gt'))

    def run_elasticsearch_query_executor(self):

        self.add_gt_header_fields()
        self.add_sample_to_source()

        if self.mendelian_analysis_type == 'autosomal_dominant':
            self.add_autosomal_dominant_query_string()
        elif self.mendelian_analysis_type == 'autosomal_recessive':
            self.add_autosomal_recessive_query_string()
        elif self.mendelian_analysis_type == 'denovo':
            self.add_denovo_query_string()

        elasticsearch_query_executor = self.elasticsearch_query_executor_class(
            self.dataset_obj, self.query_body, self.father_id, self.mother_id, self.child_id, self.mendelian_analysis_function)
        self.elasticsearch_response = elasticsearch_query_executor.get_elasticsearch_response()
        self.elasticsearch_response_time = elasticsearch_query_executor.get_elasticsearch_response_time()

    def search(self):
        self.run_elasticsearch_dsl()

        self.run_elasticsearch_query_executor()
        self.run_elasticsearch_response_parser_class()
