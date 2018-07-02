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


def get_genes_es(dataset_es_index_name,
                 dataset_es_type_name,
                 dataset_es_host,
                 dataset_es_port,
                 field_es_name,
                 field_path,
                 body):

    es = elasticsearch.Elasticsearch(
        host=dataset_es_host, port=dataset_es_port)
    body = copy.deepcopy(body)
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


def is_autosomal_dominant(sample_information):

    # If Mendelian - autosomal dominant selected:
    #  If Mother is affected (Phenotype == 2):
    #       Then filter by this rule:
    #                                   mother_genotype == '0/1' or '0|1' or '1|0',
    #                                   father_genotype == '0/0' or '0|0',
    #                                   affected child_genotype == '0/1'  or '0|1' or '1|0'.

    # If Father is affected (Phenotype  == 2):
    #                                   mother_genotype == '0/0' or '0|0',
    #                                   father_genotype == '0/1' or '0|1' or '1|0',
    #                                   affected child_genotype == '0/1'  or '0|1' or '1|0'.
    # If only the child is affected (Phenotype == 2):
    #       Then follow the de novo rules.

    # Case Mother (Phenotype == 2)
    if sample_information.get('Mother_Phenotype') == '2':
        if sample_information.get('Mother_Genotype') in ['0/1', '0|1', '1|0'] and sample_information.get('Father_Genotype') in ['0/0', '0|0'] and sample_information.get('GT') in ['0/1', '0|1', '1|0']:
            return True
        else:
            return False

    # Case Father (Phenotype == 2)
    elif sample_information.get('Father_Phenotype') == '2':
        if sample_information.get('Father_Genotype') in ['0/1', '0|1', '1|0'] and sample_information.get('Mother_Genotype') in ['0/0', '0|0'] and sample_information.get('GT') in ['0/1', '0|1', '1|0']:
            return True
        else:
            return False
    elif sample_information.get('Phenotype') == '2':
        if sample_information.get('Father_Genotype') in ['0/0', '0|0'] and sample_information.get('Mother_Genotype') in ['0/0', '0|0'] and sample_information.get('GT') in ['0/1', '0|1', '1|0']:
            return True
        else:
            return False
    else:
        return False


def is_autosomal_recessive(sample_array, father_id, mother_id, child_id):

    looking_for_ids = (father_id, mother_id, child_id)
    mother_gt = father_gt = child_gt = None
    for ele in sample_array:

        sample_id = ele.get('Sample_ID')

        if sample_id not in looking_for_ids:
            continue

        if sample_id == father_id:
            father_gt = ele.get('GT')
        elif sample_id == mother_id:
            mother_gt = ele.get('GT')
        elif sample_id == child_id:
            child_gt = ele.get('GT')
            if child_gt not in ['1/1', '1|1']:
                return None

    if not all((father_gt, mother_gt, child_gt)):
        return None

    if child_gt == 'N/A':
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
    mother_gt = father_gt = child_gt = 'N/A'
    for ele in sample_array:

        sample_id = ele.get('Sample_ID')

        if sample_id not in looking_for_ids:
            continue

        if sample_id == father_id:
            return None
        elif sample_id == mother_id:
            return None
        elif sample_id == child_id:
            child_gt = ele.get('GT')
            if child_gt not in ['0/1', '0|1', '0|1']:
                None

    if child_gt == 'N/A':
        return None
    return (father_gt, mother_gt, child_gt)


def get_genotypes(sample_array, father_id, mother_id, child_id):

    looking_for_ids = (father_id, mother_id, child_id)
    mother_gt = father_gt = child_gt = '0/0'
    for ele in sample_array:

        sample_id = ele.get('Sample_ID')

        if sample_id not in looking_for_ids:
            continue

        if sample_id == father_id:
            father_gt = ele.get('GT')
            if father_gt not in ['0/1', '0|1', '1/0', '1|0']:
                return None
        elif sample_id == mother_id:
            mother_gt = ele.get('GT')
            if mother_gt not in ['0/1', '0|1', '1/0', '1|0']:
                return None
        elif sample_id == child_id:
            child_gt = ele.get('GT')

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


def is_compound_heterozygous_for_gene(es, dataset_obj, gene, query, family_id, father_id, mother_id, child_id):
    saved_hits = {}
    variant_genotypes = []

    for ele in helpers.scan(es,
                            query=query,
                            scroll=u'5m',
                            size=1000,
                            preserve_order=False,
                            index=dataset_obj.es_index_name,
                            doc_type=dataset_obj.es_type_name):

        sample_source = ele['inner_hits']['sample']['hits']['hits']
        sample_data = extract_sample_inner_hits_as_array(sample_source)[0]

        result = ele['_source']
        es_id = ele['_id']
        ele_copy = ele.copy()
        ele_copy['_source']['sample'] = [sample_data]
        saved_hits[es_id] = ele_copy

        father_gt = sample_data.get('Father_Genotype')
        mother_gt = sample_data.get('Mother_Genotype')
        child_gt = sample_data.get('GT')

        sum_digits = sum([int(char)
                          for char in father_gt + mother_gt if char.isdigit()])
        if sum_digits != 1:
            continue

        variant_genotype = [father_gt, mother_gt, child_gt]
        variant_id = result.get('Variant')

        if variant_genotype:
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

    def __init__(self, dataset_obj, query_body, family_dict, mendelian_analysis_type, elasticsearch_terminate_after=0):
        super().__init__(dataset_obj, query_body, elasticsearch_terminate_after=0)
        self.family_dict = family_dict
        self.mendelian_analysis_type = mendelian_analysis_type
        self.family_results = {}

        if self.mendelian_analysis_type == 'autosomal_dominant':
            self.mendelian_analysis_function = 'is_autosomal_dominant'
        elif self.mendelian_analysis_type == 'autosomal_recessive':
            self.mendelian_analysis_function = 'is_autosomal_recessive'
        elif self.mendelian_analysis_type == 'denovo':
            self.mendelian_analysis_function = 'is_denovo'
        elif self.mendelian_analysis_type == 'compound_heterozygous':
            self.mendelian_analysis_function = 'is_compound_heterozygous_for_gene'

    def add_autosomal_dominant_query_string(self, family_id, child_id):
        query_body = copy.deepcopy(self.query_body)
        if 'query' not in query_body:
            query_body['query'] = {
                "nested": {
                    "inner_hits": {},
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"sample.Family_ID": family_id}},
                                {"term": {"sample.Sample_ID": child_id}}
                            ],
                            "should": [
                                {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}}
                            ],
                            "minimum_should_match": 1
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
                sample_array['nested']['query']['bool']['filter'].append({"term": {"sample.Family_ID": family_id}})
                sample_array['nested']['query']['bool']['filter'].append({"term": {"sample.Sample_ID": child_id}})
                sample_array['nested']['query']['bool']['minimum_should_match'] = 1
                sample_array['nested']['query']['bool']['should'] = [
                    {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}}
                ]

                filter_array_copy.append(sample_array)
                query_body['query']['bool']['filter'] = filter_array_copy
            else:
                query_body['query']['bool']['filter'].append(
                    {
                        "nested": {
                            "inner_hits": {},
                            "path": "sample",
                            "score_mode": "none",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {"sample.Family_ID": family_id}},
                                        {"term": {"sample.Sample_ID": child_id}}
                                    ],
                                    "should": [
                                        {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}}
                                    ],
                                    "minimum_should_match": 1
                                }
                            }
                        }
                    }
                )
        return query_body

    def add_autosomal_recessive_query_string(self, child_id):
        query_body = copy.deepcopy(self.query_body)
        if 'query' not in query_body:
            query_body['query'] = {
                "nested": {
                    "inner_hits": {},
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {
                                    "sample.Sample_ID": child_id}}
                            ],
                            "should": [
                                {"terms": {"sample.GT": ["1/1", "1|1"]}},
                                {"terms": {"sample.Mother_Genotype": ["0/1", "0|1", "1|0"]}},
                                {"terms": {"sample.Father_Genotype": ["0/1", "0|1", "1|0"]}}
                            ],
                            "minimum_should_match": 3
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
                sample_array['nested']['query']['bool']['filter'].append({'term': {'sample.Sample_ID': child_id}})
                sample_array['nested']['query']['bool']['minimum_should_match'] = 3
                sample_array['nested']['query']['bool']['should'] = [
                    {"terms": {"sample.GT": ["1/1", "1|1"]}},
                    {"terms": {"sample.Mother_Genotype": ["0/1", "0|1", "1|0"]}},
                    {"terms": {"sample.Father_Genotype": ["0/1", "0|1", "1|0"]}}
                ]

                filter_array_copy.append(sample_array)
                query_body['query']['bool']['filter'] = filter_array_copy
            else:
                query_body['query']['bool']['filter'].append(
                    {
                        "nested": {
                            "inner_hits": {},
                            "path": "sample",
                            "score_mode": "none",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {
                                            "sample.Sample_ID": child_id}}
                                    ],
                                    "should": [
                                        {"terms": {"sample.GT": ["1/1", "1|1"]}},
                                        {"terms": {"sample.Mother_Genotype": ["0/1", "0|1", "1|0"]}},
                                        {"terms": {"sample.Father_Genotype": ["0/1", "0|1", "1|0"]}}
                                    ],
                                    "minimum_should_match": 3
                                }
                            }
                        }
                    }
                )
        return query_body

    def add_denovo_query_string(self, child_id):
        query_body = copy.deepcopy(self.query_body)
        if 'query' not in query_body:
            query_body['query'] = {
                "nested": {
                    "inner_hits": {},
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"sample.Sample_ID": child_id}},
                                {"term": {"sample.Mother_Genotype": "0/0"}},
                                {"term": {"sample.Father_Genotype": "0/0"}}
                            ],
                            "should": [
                                {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}}
                            ],
                            "minimum_should_match": 1
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
                sample_array['nested']['query']['bool']['filter'].append({'term': {'sample.Sample_ID': child_id}})
                sample_array['nested']['query']['bool']['filter'].append(
                    {"term": {"sample.Mother_Genotype": "0/0"}})
                sample_array['nested']['query']['bool']['filter'].append(
                    {"term": {"sample.Father_Genotype": "0/0"}})
                sample_array['nested']['query']['bool']['minimum_should_match'] = 1
                sample_array['nested']['query']['bool']['should'] = [
                    {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}}
                ]

                filter_array_copy.append(sample_array)
                query_body['query']['bool']['filter'] = filter_array_copy

            else:
                query_body['query']['bool']['filter'].append(
                    {"nested": {
                        "inner_hits": {},
                        "path": "sample",
                        "score_mode": "none",
                        "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {"sample.Sample_ID": child_id}},
                                        {"term": {"sample.Mother_Genotype": "0/0"}},
                                        {"term": {"sample.Father_Genotype": "0/0"}}
                                    ],
                                    "should": [
                                        {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}}
                                    ],
                                    "minimum_should_match": 1
                                }
                        }
                    }
                    }
                )

        return query_body

    def add_child_ids_query_string(self, child_ids):
        query_body = copy.deepcopy(self.query_body)
        if 'query' not in query_body:
            query_body['query'] = {
                "nested": {
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                        "bool": {
                            "filter": [
                                {"terms": {"sample.Sample_ID": child_ids}}
                            ]
                        }
                    }
                }
            }
        elif query_body['query']['bool']['filter']:
            query_body['query']['bool']['filter'].append(
                {"nested": {
                    "path": "sample",
                    "score_mode": "none",
                    "query": {
                            "bool": {
                                "filter": [
                                    {"terms": {"sample.Sample_ID": child_ids}}
                                ]
                            }
                    }
                }
                }
            )

        return query_body

    def non_gene_based_search(self):
        function = getattr(thismodule, self.mendelian_analysis_function)
        family_results = {}
        results = {
            "took": None,
            "hits": {
                "total": None,
                "hits": deque()
            }
        }
        count = 0
        start_time = datetime.datetime.now()
        for family_id, family in self.family_dict.items():

            if self.mendelian_analysis_type == 'autosomal_dominant':
                query_body = self.add_autosomal_dominant_query_string(family_id, family.get('child_id'))
            elif self.mendelian_analysis_type == 'autosomal_recessive':
                query_body = self.add_autosomal_recessive_query_string(family.get('child_id'))
            elif self.mendelian_analysis_type == 'denovo':
                query_body = self.add_denovo_query_string(family.get('child_id'))

            es = elasticsearch.Elasticsearch(
                host=self.dataset_obj.es_host, port=self.dataset_obj.es_port)

            for hit in helpers.scan(
                    es,
                    query=query_body,
                    scroll=u'5m',
                    size=1000,
                    preserve_order=False,
                    index=self.dataset_obj.es_index_name,
                    doc_type=self.dataset_obj.es_type_name):

                inner_hits_sample = hit['inner_hits']['sample']['hits']['hits']
                sample_data = extract_sample_inner_hits_as_array(inner_hits_sample)
                sample_data = filter_source_by_family_id(sample_data, family_id)

                # only autosomal dominant requires a function to filter the sample data. Denovo and autosomal recessive are filtered directly by ES
                if self.mendelian_analysis_type == 'autosomal_dominant':
                    if len(sample_data) > 1:
                        print('error!')
                        continue
                    if not is_autosomal_dominant(sample_data[0]):
                        continue

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

    def is_gene_in_query_body(self):
        csq_nested_array = None

        try:
            for ele in self.query_body['query']['bool']['filter']:
                if 'nested' in ele and ele['nested']['path'] == 'CSQ_nested':
                    csq_nested_array = ele
                    break

            if csq_nested_array:
                for ele in csq_nested_array['nested']['query']['bool']['filter']:
                    if 'terms' not in ele:
                        continue
                    if ele['terms'].get('CSQ_nested.SYMBOL'):
                        return ele['terms'].get('CSQ_nested.SYMBOL')
                return False
            else:
                return False
        except KeyError:
            return False

    def add_gene_to_query_body(self, gene):
        query_body = copy.deepcopy(self.query_body)
        if 'query' not in query_body:
            query_body['query'] = {}
        if 'bool' not in query_body['query']:
            query_body['query']['bool'] = {}
        if 'filter' not in query_body['query']['bool']:
            query_body['query']['bool']['filter'] = []
        filter_array = query_body['query']['bool']['filter']
        filter_array_copy = copy.deepcopy(filter_array)
        csq_nested_array = None
        for ele in filter_array:
            if 'nested' in ele and ele['nested']['path'] == 'CSQ_nested':
                filter_array_copy.remove(ele)
                csq_nested_array = ele

        if csq_nested_array:
            # is gene already specified?
            for ele in csq_nested_array['nested']['query']['bool']['filter']:
                if 'terms' not in ele:
                    continue
                if ele['terms'].get('CSQ_nested.SYMBOL'):
                    return query_body

            csq_nested_array['nested']['inner_hits'] = {}
            csq_nested_array['nested']['query']['bool']['filter'].append({"terms": {"CSQ_nested.SYMBOL": [gene,]}})

            filter_array_copy.append(csq_nested_array)
            query_body['query']['bool']['filter'] = filter_array_copy

        else:
            query_body['query']['bool']['filter'].append(
                {
                    "nested": {
                        "inner_hits": {},
                        "path": "CSQ_nested",
                        "query": {
                            "bool": {
                                "filter": [
                                    {
                                        "terms": {
                                            "CSQ_nested.SYMBOL": [gene,]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            )

        return query_body

    def gene_based_search(self):
        es = elasticsearch.Elasticsearch(
            host=self.dataset_obj.es_host, port=self.dataset_obj.es_port)

        child_ids = [val.get('child_id') for key, val in self.family_dict.items()]

        if self.is_gene_in_query_body():
            genes = [self.is_gene_in_query_body(), ]

        else:
            genes = get_genes_es(self.dataset_obj.es_index_name,
                                 self.dataset_obj.es_type_name,
                                 self.dataset_obj.es_host,
                                 self.dataset_obj.es_port,
                                 'SYMBOL',
                                 'CSQ_nested',
                                 self.query_body)

        count = 0
        start_time = datetime.datetime.now()
        results = {
            "took": None,
            "hits": {
                "total": None,
                "hits": deque()
            }
        }
        for family_id, family in self.family_dict.items():

            for gene in genes:
                if not self.is_gene_in_query_body():
                    query_body = self.add_gene_to_query_body(gene)
                else:
                    query_body = copy.deepcopy(self.query_body)

                filter_array = query_body['query']['bool']['filter']
                filter_array_copy = copy.deepcopy(filter_array)
                sample_array = None
                for ele in filter_array:
                    if 'nested' in ele and ele['nested']['path'] == 'sample':
                        filter_array_copy.remove(ele)
                        sample_array = ele
                if sample_array:
                    sample_array['nested']['inner_hits'] = {}
                    sample_array['nested']['query']['bool']['filter'].append(
                        {"terms": {"sample.GT": ["0|1", "1|0", "0/1", "1/0"]}})
                    sample_array['nested']['query']['bool']['filter'].append(
                        {"term": {"sample.Sample_ID": family.get('child_id')}})

                    if 'must_not' not in sample_array['nested']['query']['bool']:
                        sample_array['nested']['query']['bool']['must_not'] = [
                            {"term": {"sample.Father_Genotype": "NA"}},
                            {"term": {"sample.Mother_Genotype": "NA"}}
                        ]
                    else:
                        sample_array['nested']['query']['bool']['must_not'].append(
                            {"term": {"sample.Father_Genotype": "NA"}})
                        sample_array['nested']['query']['bool']['must_not'].append(
                            {"term": {"sample.Mother_Genotype": "NA"}})

                    filter_array_copy.append(sample_array)
                    query_body['query']['bool']['filter'] = filter_array_copy

                else:
                    query_body['query']['bool']['filter'].append(
                        ({
                            "nested": {
                                "inner_hits": {},
                                "path": "sample",
                                "query": {
                                    "bool": {
                                        "filter": [
                                            {"terms": {"sample.GT": ["0|1", "1|0", "0/1", "1/0"]}},
                                            {"term": {"sample.Sample_ID": family.get('child_id')}},
                                            {"term": {"sample.Family_ID": family_id}}
                                        ],
                                        "must_not": [
                                            {"term": {"sample.Father_Genotype": "NA"}},
                                            {"term": {"sample.Mother_Genotype": "NA"}}
                                        ]
                                    }
                                }
                            }
                        })
                    )

                compound_heterozygous_results = is_compound_heterozygous_for_gene(
                    es, self.dataset_obj, gene, query_body, family_id, family.get('father_id'), family.get('mother_id'), family.get('child_id'))
                if compound_heterozygous_results:
                    count += 1
                    results['hits']['hits'].extend(compound_heterozygous_results)
        return results

    def excecute_elasticsearch_query(self):

        if self.mendelian_analysis_function in ['is_autosomal_dominant', 'is_autosomal_recessive', 'is_denovo']:
            results = self.non_gene_based_search()
        elif self.mendelian_analysis_function == 'is_compound_heterozygous_for_gene':
            results = self.gene_based_search()

        self.elasticsearch_response = results


class MendelianElasticsearchResponseParser(BaseElasticsearchResponseParser):
    maximum_table_size = 10000000


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
        return (father_id, mother_id, child_id)

    def get_family_dict(self):

        family_ids = get_values_from_es(self.dataset_obj.es_index_name,
                                        self.dataset_obj.es_type_name,
                                        self.dataset_obj.es_host,
                                        self.dataset_obj.es_port,
                                        'Family_ID',
                                        'sample')
        family_dict = {}
        for family_id in family_ids:
            father_id, mother_id, child_id = self._get_family(self.dataset_obj.es_index_name,
                                                              self.dataset_obj.es_type_name,
                                                              self.dataset_obj.es_host,
                                                              self.dataset_obj.es_port,
                                                              family_id)
            family_dict[family_id] = {'father_id': father_id, 'mother_id': mother_id, 'child_id': child_id}

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

    def run_elasticsearch_query_executor(self):

        self.get_family_dict()

        elasticsearch_query_executor = self.elasticsearch_query_executor_class(
            self.dataset_obj, self.query_body, self.family_dict, self.mendelian_analysis_type)
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
