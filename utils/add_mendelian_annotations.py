autosomal_recessive_vep_query_body_template = """{
 "_source": ["sample", "CHROM", "ID", "POS", "REF", "Variant"
 ],
 "query": {
     "bool": {
         "filter": [
             {"nested": {
                 "path": "sample",
                 "query": {
                   "bool": {
                     "filter": [
                       {"term": {"sample.Sample_ID": "%s"}},
                       {"terms": {"sample.GT": ["1/1", "1|1"]}},
                       {"term": {"sample.Phenotype": "2"}},
                       {"terms": {"sample.Mother_Genotype": ["0/1", "0|1", "1|0"]}},
                       {"terms": {"sample.Father_Genotype": ["0/1", "0|1", "1|0"]}}
                     ]
                   }
                 },
                 "score_mode": "none"
                }
             },
             {"nested": {
                 "path": "CSQ_nested",
                 "query": {
                   "bool": {
                     "filter": [
                       {"terms": {"CSQ_nested.Consequence": ["frameshift_variant", "splice_acceptor_variant", "splice_donor_variant", "start_lost", "start_retained_variant", "stop_gained", "stop_lost"]}}
                     ]
                   }
                 },
                 "score_mode": "none"
                }
             }
         ],
         "must_not" : [
             {"terms": {"CHROM": ["X", "Y"]}}
        ]
     }
 }
}"""

denovo_query_body_template = """{
"_source": ["sample", "CHROM", "ID", "POS", "REF", "Variant"],
"query": {
    "bool": {
        "filter": [
            {"nested": {
              "path": "sample",
              "query": {
                  "bool": {
                   "filter": [
                       {"term": {"sample.Sample_ID": "%s"}},
                       {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}},
                       {"term": {"sample.Phenotype": "2"}},
                       {"term": {"sample.Mother_Genotype": "0/0"}},
                       {"term": {"sample.Father_Genotype": "0/0"}}
                   ]
                  }
              },
              "score_mode": "none"
             }
            }
        ],
         "must_not" : [
             {"terms": {"CHROM": ["X", "Y"]}}
        ]
    }
}
}"""


autosomal_dominant_query_body_template = """{
"_source": ["sample", "CHROM", "ID", "POS", "REF", "Variant"],
"query": {
    "bool": {
        "filter": [
            {"nested": {
                "path": "sample",
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"sample.Sample_ID": "%s"}},
                            {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}},
                            {"term": {"sample.Phenotype": "2"}}
                        ]
                    }
                },
                "score_mode": "none"
            }
        }
        ],
         "must_not" : [
             {"terms": {"CHROM": ["X", "Y"]}}
        ]
    }
}
}"""

compound_heterozygous_query_body_template = """{
 "_source": ["sample", "CHROM", "ID", "POS", "REF", "Variant"
 ],
 "query": {
     "bool": {
         "filter": [
             {"nested": {
                 "path": "sample",
                 "query": {
                   "bool": {
                     "filter": [
                       {"term": {"sample.Sample_ID": "%s"}},
                       {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}},
                       {"term": {"sample.Phenotype": "2"}},
                       {"term": {"sample.Mother_Phenotype": "1"}},
                       {"term": {"sample.Father_Phenotype": "1"}}
                     ]
                   }
                 },
                 "score_mode": "none"
                }
             },
             {"nested": {
                 "path": "CSQ_nested",
                 "query": {
                   "bool": {
                     "filter": [
                       {"terms": {"CSQ_nested.Consequence": ["frameshift_variant", "splice_acceptor_variant", "splice_donor_variant", "start_lost", "start_retained_variant", "stop_gained", "stop_lost"]}},
                       {"term": {"CSQ_nested.SYMBOL": "%s"}}
                     ]
                   }
                 },
                 "score_mode": "none"
                }
             }
         ],
         "must_not" : [
             {"terms": {"CHROM": ["X", "Y"]}}
        ]
     }
 }
}"""


x_linked_dominant_query_body_template = """{
"_source": ["sample","CHROM","ID","POS","REF","Variant"
],
"query": {
    "bool": {
        "filter": [
            {"term": {"CHROM": "X"}},
            {"nested": {
                    "path": "sample",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"sample.Sample_ID": "%s"}},
                                {"term": {"sample.Phenotype": "2"}}
                            ]
                        }
                    },
                    "score_mode": "none"
                }
            }
        ],
        "must_not" : [
            {"range": {"POS": {"gt": %d, "lt": %d}}},
            {"range": {"POS": {"gt": %d, "lt": %d}}}

        ]
    }
}
}"""


x_linked_recessive_vep_query_body_template = """{
"_source": ["sample","CHROM","ID","POS","REF","Variant"
],
"query": {
    "bool": {
        "filter": [
            {"term": {"CHROM": "X"}},
            {"nested": {
                    "path": "sample",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"sample.Sample_ID": "%s"}},
                                {"term": {"sample.Phenotype": "2"}}
                            ]
                        }
                    },
                    "score_mode": "none"
                }
            },
            {"nested": {
                 "path": "CSQ_nested",
                 "query": {
                   "bool": {
                     "filter": [
                       {"terms": {"CSQ_nested.Consequence": ["frameshift_variant", "splice_acceptor_variant", "splice_donor_variant", "start_lost", "start_retained_variant", "stop_gained", "stop_lost"]}}
                     ]
                   }
                 },
                 "score_mode": "none"
                }
             }
        ],
        "must_not" : [
            {"range": {"POS": {"gt": %d, "lt": %d}}},
            {"range": {"POS": {"gt": %d, "lt": %d}}}

        ]
    }
}
}"""


x_linked_de_novo_query_body_template = """{
"_source": ["sample","CHROM","ID","POS","REF","Variant"
],
"query": {
    "bool": {
        "filter": [
            {"term": {"CHROM": "X"}},
            {"nested": {
                    "path": "sample",
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"sample.Sample_ID": "%s"}},
                                {"term": {"sample.Phenotype": "2"}}
                            ]
                        }
                    },
                    "score_mode": "none"
                }
            }
        ],
        "must_not" : [
            {"range": {"POS": {"gt": %d, "lt": %d}}},
            {"range": {"POS": {"gt": %d, "lt": %d}}}

        ]
    }
}
}"""



import elasticsearch
from elasticsearch import helpers
import pprint
import json
from natsort import natsorted


def is_autosomal_dominant(sample_information):

    if sample_information.get('Mother_Phenotype') == '2':
        if sample_information.get('Mother_Genotype') in ['0/1', '0|1', '1|0'] and sample_information.get('Father_Genotype') in ['0/0', '0|0']:
            return True
    # Case Father (Phenotype == 2)
    elif sample_information.get('Father_Phenotype') == '2':
        if sample_information.get('Mother_Genotype') in ['0/0', '0|0'] and sample_information.get('Father_Genotype') in ['0/1', '0|1', '1|0']:
            return True


def is_x_linked_dominant(sample_information):

    if sample_information.get('Sex') == '1':
        if (sample_information.get('GT') in ["0/1", "0|1", "1|0", "1/1", "1|1", "1"] and
            sample_information.get('Mother_Genotype') in ["0/1", "0|1"] and
            sample_information.get('Mother_Phenotype') == "2"):
            return True
    elif sample_information.get('Sex') == '2':
        if sample_information.get('GT') in ["0/1", "0|1", "1|0"]:
            if (sample_information.get('Mother_Genotype') in ["0/0"] and
                (sample_information.get('Father_Genotype') in ["0/1", "0|1", "1|0", "1"] and
                 sample_information.get('Father_Phenotype')) == "2"):
                return True
            elif (sample_information.get('Mother_Genotype') in ["0/1", "0|1", "1|0"] and
                (sample_information.get('Father_Genotype') in ["0/0", "0|0", "0"] and
                 sample_information.get('Mother_Phenotype')) == "2"):
                return True


def is_x_linked_recessive(sample_information):

    if sample_information.get('Sex') == '1':
        if (sample_information.get('GT') not in ["0/0", "0|0", "0"] and
            sample_information.get('Mother_Genotype') in ["0/1", "0|1", "1|0"] and
            sample_information.get('Mother_Phenotype') == "1"):
            return True
    elif sample_information.get('Sex') == '2':
        if (sample_information.get('GT') in ["0/1", "0|1", "1|0"] and
            sample_information.get('Mother_Genotype') in ["0/1", "0|1", "1|0"] and
            sample_information.get('Mother_Phenotype') == "1" and
            sample_information.get('Father_Genotype') in ["0/1", "0|1", "1|0", "1"] and
            sample_information.get('Father_Phenotype') == "2"):
            return True


def is_x_linked_de_novo(sample_information):

    if sample_information.get('Sex') == '1':
        if (sample_information.get('GT') not in ["0/1", "0|1", "1|0", "1/1", "1|1", "1"] and
            sample_information.get('Mother_Genotype') in ["0/0", "0|0", "0"] and
            sample_information.get('Mother_Phenotype') == "1" and
            sample_information.get('Father_Genotype') in ["0/0", "0|0", "0"] and
            sample_information.get('Father_Phenotype') == "1"):
            return True
    elif sample_information.get('Sex') == '2':
        if (sample_information.get('GT') in ["0/1", "0|1", "1|0"] and
            sample_information.get('Mother_Genotype') in ["0/0", "0|0", "0"] and
            sample_information.get('Mother_Phenotype') == "1" and
            sample_information.get('Father_Genotype') in ["0/0", "0|0", "0"] and
            sample_information.get('Father_Phenotype') == "1"):
            return True



def get_vep_genes_from_es_for_compound_heterozygous(es, index_name, doc_type_name):
    compound_heterozygous_query_body_template = """{
         "_source": ["sample", "CHROM", "ID", "POS", "REF", "Variant", "CSQ_nested"
         ],
         "query": {
             "bool": {
                 "filter": [
                     {"nested": {
                         "path": "sample",
                         "query": {
                           "bool": {
                             "filter": [
                               {"terms": {"sample.GT": ["0/1", "0|1", "1|0"]}},
                               {"term": {"sample.Phenotype": "2"}},
                               {"term": {"sample.Mother_Phenotype": "1"}},
                               {"term": {"sample.Father_Phenotype": "1"}}
                             ]
                           }
                         },
                         "score_mode": "none"
                        }
                     },
                     {"nested": {
                         "path": "CSQ_nested",
                         "query": {
                           "bool": {
                             "filter": [
                               {"terms": {"CSQ_nested.Consequence": ["frameshift_variant", "splice_acceptor_variant", "splice_donor_variant", "start_lost", "start_retained_variant", "stop_gained", "stop_lost"]}}
                             ]
                           }
                         },
                         "score_mode": "none"
                        }
                     }
                 ],
                 "must_not" : [
                     {"terms": {"CHROM": ["X", "Y"]}}
                ]
             }
         },
         "size": 0,
        "aggs" : {
            "values" : {
                "nested" : {
                    "path" : "CSQ_nested"
                },
                "aggs" : {
                    "values" : {"terms" : {"field" : "CSQ_nested.SYMBOL", "size" : 30000}}
                }
            }
        }
        }"""

    results = es.search(index=index_name, doc_type=doc_type_name, body=compound_heterozygous_query_body_template, request_timeout=120)
    return natsorted([ele['key'] for ele in results["aggregations"]["values"]["values"]["buckets"] if ele['key']])



def get_values_from_es(es, index_name, doc_type_name, field_es_name, field_path):

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
        results = es.search(index=index_name, doc_type=doc_type_name, body=body, request_timeout=120)
        return [ele['key'] for ele in results["aggregations"]["values"]["buckets"] if ele['key']]

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

        results = es.search(index=index_name, doc_type=doc_type_name, body=body, request_timeout=120)
        return [ele['key'] for ele in results["aggregations"]["values"]["values"]["buckets"] if ele['key']]


def get_family(es, index_name, doc_type_name, Family_ID):
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

    body = body_template % (Family_ID)
    results = es.search(index=index_name, doc_type=doc_type_name, body=body, request_timeout=120)

    result = results['hits']['hits'][0]['inner_hits']['sample']['hits']['hits'][0]["_source"]
    father_id = result.get('Father_ID')
    mother_id = result.get('Mother_ID')
    child_id = result.get('Sample_ID')
    child_sex = result.get('Sex')
    return (father_id, mother_id, child_id, child_sex)


def get_family_dict(es, index_name, doc_type_name):

    family_ids = get_values_from_es(es, index_name, doc_type_name, 'Family_ID', 'sample')

    family_dict = {}

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
    family_dict = {}
    for family_id in family_ids:

        body = body_template % (family_id)
        results = es.search(index=index_name, doc_type=doc_type_name, body=body, request_timeout=120)

        result = results['hits']['hits'][0]['inner_hits']['sample']['hits']['hits'][0]["_source"]
        father_id = result.get('Father_ID')
        mother_id = result.get('Mother_ID')
        child_id = result.get('Sample_ID')
        child_sex = result.get('Sex')
        family_dict[family_id] = {'father_id': father_id,
                                  'mother_id': mother_id, 'child_id': child_id, 'child_sex': child_sex}

    return family_dict


def pop_sample_with_id(sample_array, sample_id):

    saved_index = 0
    for index, sample in enumerate(sample_array):
        if sample.get('Sample_ID') == sample_id:
            saved_index = index

    sample = sample_array.pop(saved_index)

    return sample


def pop_sample_with_id_apply_compound_het_rules(sample_array, sample_id):

    saved_index = 0
    for index, sample in enumerate(sample_array):
        if sample.get('Sample_ID') == sample_id:
            saved_index = index

    sample = sample_array.pop(saved_index)

    if (sample.get('Mother_Genotype') in ["0/1", "0|1", "1|0"] and
        sample.get('Father_Genotype') in ["0/0", "0|0"]):
        return sample
    elif (sample.get('Mother_Genotype') in ["0/0", "0|0"] and
        sample.get('Father_Genotype') in ["0/1", "0|1", "1|0"]):
        return sample

    return None


def are_variants_compound_heterozygous(variants):
    compound_heterozygous_found = False
    gt_pair_whose_reverse_to_find = None
    compound_heterozygous_variants = []
    for variant in variants:
        father_gt = variant.get('Father_Genotype')
        mother_gt =variant.get('Mother_Genotype')

        sum_digits = sum([int(char)
                          for char in father_gt + mother_gt if char.isdigit()])

        if sum_digits != 1:
            continue

        if not gt_pair_whose_reverse_to_find:
            gt_pair_whose_reverse_to_find = [father_gt, mother_gt]
            compound_heterozygous_variants.append(variant)
            continue

        current_gt_pair = [father_gt, mother_gt]
        current_gt_pair.reverse()
        if gt_pair_whose_reverse_to_find == current_gt_pair:
            compound_heterozygous_variants.append(variant)
            compound_heterozygous_found = True

    if compound_heterozygous_found:
        return compound_heterozygous_variants
    else:
        return False

def annotate_autosomal_recessive(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    updated_count = 0
    for family_id, family in family_dict.items():
        child_id = family.get('child_id')
        # print(child_id)
        query_body = autosomal_recessive_vep_query_body_template % (child_id)
        # print(query_body)
        query_body = json.loads(query_body)
        for hit in helpers.scan(
                es,
                query=query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=index_name,
                doc_type=doc_type_name):
            # pprint.pprint(hit["_source"])
            es_id = hit['_id']
            sample_array = hit["_source"]["sample"]
            # print(len(sample_array))
            sample = pop_sample_with_id(sample_array, child_id)
            mendelian_diseases = sample.get('mendelian_diseases')
            to_update = False
            if mendelian_diseases:
                if isinstance(mendelian_diseases, list):
                    if 'autosomal_recessive' not in mendelian_diseases:
                        mendelian_diseases.append('autosomal_recessive')
                        to_update = True
                elif isinstance(mendelian_diseases, str):
                    if mendelian_diseases != 'autosomal_recessive':
                        mendelian_diseases = [mendelian_diseases, 'autosomal_recessive']
                        to_update = True
            else:
                to_update = True
                sample['mendelian_diseases'] = 'autosomal_recessive'

            # pprint.pprint(sample)
            sample_array.append(sample)
            action = {
                "_index": index_name,
                '_op_type': 'update',
                "_type": doc_type_name,
                "_id": es_id,
                "doc": {
                    "sample": sample_array
                }
            }
            if to_update:
                actions.append(action)
                updated_count += 1

            if count % 500 == 0:
                helpers.bulk(es, actions)
                actions = []

            count += 1
        helpers.bulk(es, actions)

    print('Found {} autosomal_recessive samples'.format(count))


def annotate_denovo(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    updated_count = 0
    for family_id, family in family_dict.items():
        child_id = family.get('child_id')
        # print(child_id)
        query_body = denovo_query_body_template % (child_id)
        # print(query_body)
        query_body = json.loads(query_body)
        for hit in helpers.scan(
                es,
                query=query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=index_name,
                doc_type=doc_type_name):
            # pprint.pprint(hit["_source"])
            es_id = hit['_id']
            sample_array = hit["_source"]["sample"]
            # print(len(sample_array))
            sample = pop_sample_with_id(sample_array, child_id)
            mendelian_diseases = sample.get('mendelian_diseases')
            to_update = False
            if mendelian_diseases:
                if isinstance(mendelian_diseases, list):
                    if 'denovo' not in mendelian_diseases:
                        mendelian_diseases.append('denovo')
                        print(es_id, mendelian_diseases)
                        to_update = True
                elif isinstance(mendelian_diseases, str):
                    if mendelian_diseases != 'denovo':
                        mendelian_diseases = [mendelian_diseases, 'denovo']
                        print(es_id, mendelian_diseases)
                        to_update = True
            else:
                sample['mendelian_diseases'] = 'denovo'
                to_update = True

            if to_update:
                sample_array.append(sample)
                action = {
                    "_index": index_name,
                    '_op_type': 'update',
                    "_type": doc_type_name,
                    "_id": es_id,
                    "doc": {
                        "sample": sample_array
                    }
                }

                actions.append(action)
                updated_count += 1

            if count % 500 == 0:
                helpers.bulk(es, actions)
                actions = []

            count += 1
        helpers.bulk(es, actions)

    print('Found {} denovo samples'.format(count))


def annotate_autosomal_dominant(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    updated_count = 0
    for family_id, family in family_dict.items():
        child_id = family.get('child_id')
        # print(child_id)
        query_body = autosomal_dominant_query_body_template % (child_id)
        # print(query_body)
        query_body = json.loads(query_body)
        for hit in helpers.scan(
                es,
                query=query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=index_name,
                doc_type=doc_type_name):
            # pprint.pprint(hit["_source"])
            es_id = hit['_id']
            sample_array = hit["_source"]["sample"]
            # print(len(sample_array))
            sample = pop_sample_with_id(sample_array, child_id)
            if is_autosomal_dominant(sample):
                mendelian_diseases = sample.get('mendelian_diseases')
                # sample['mendelian_diseases'] = 'autosomal_dominant'
                to_update = True
                if mendelian_diseases:
                    if isinstance(mendelian_diseases, list):
                        if 'autosomal_dominant' not in mendelian_diseases:
                            mendelian_diseases.append('autosomal_dominant')
                            print(es_id, mendelian_diseases)
                            to_update = True
                    elif isinstance(mendelian_diseases, str):
                        if mendelian_diseases != 'autosomal_dominant':
                            mendelian_diseases = [mendelian_diseases, 'autosomal_dominant']
                            print(es_id, mendelian_diseases)
                            to_update = True
                else:
                    sample['mendelian_diseases'] = 'autosomal_dominant'
                    to_update = True

                if to_update:
                    sample_array.append(sample)
                    action = {
                        "_index": index_name,
                        '_op_type': 'update',
                        "_type": doc_type_name,
                        "_id": es_id,
                        "doc": {
                            "sample": sample_array
                        }
                    }

                    actions.append(action)
                    updated_count += 1

                if count % 500 == 0:
                    helpers.bulk(es, actions)
                    actions = []

                count += 1
        helpers.bulk(es, actions)

    print('Found {} autosomal dominant samples'.format(count))


range_rules = {
    'hg19/GRCh37': ([60001, 2699520], [154931044, 155260560]),
    'hg38/GRCh38': ([10001, 2781479], [155701383, 156030895])
}


def annotate_x_linked_dominant(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    updated_count = 0
    for family_id, family in family_dict.items():
        child_id = family.get('child_id')
        # print(child_id)
        query_body = x_linked_dominant_query_body_template % (
            child_id,
            range_rules['hg19/GRCh37'][0][0],
            range_rules['hg19/GRCh37'][0][1],
            range_rules['hg19/GRCh37'][1][0],
            range_rules['hg19/GRCh37'][1][1])
        # print(query_body)
        query_body = json.loads(query_body)
        for hit in helpers.scan(
                es,
                query=query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=index_name,
                doc_type=doc_type_name):
            # pprint.pprint(hit["_source"])
            es_id = hit['_id']
            sample_array = hit["_source"]["sample"]
            # print(len(sample_array))
            sample = pop_sample_with_id(sample_array, child_id)
            if is_x_linked_dominant(sample):
                mendelian_diseases = sample.get('mendelian_diseases')
                # sample['mendelian_diseases'] = 'x_linked_dominant'
                to_update = True
                if mendelian_diseases:
                    if isinstance(mendelian_diseases, list):
                        if 'x_linked_dominant' not in mendelian_diseases:
                            mendelian_diseases.append('x_linked_dominant')
                            print(es_id, mendelian_diseases)
                            to_update = True
                    elif isinstance(mendelian_diseases, str):
                        if mendelian_diseases != 'x_linked_dominant':
                            mendelian_diseases = [mendelian_diseases, 'x_linked_dominant']
                            print(es_id, mendelian_diseases)
                            to_update = True
                else:
                    sample['mendelian_diseases'] = 'x_linked_dominant'
                    to_update = True

                if to_update:
                    sample_array.append(sample)
                    action = {
                        "_index": index_name,
                        '_op_type': 'update',
                        "_type": doc_type_name,
                        "_id": es_id,
                        "doc": {
                            "sample": sample_array
                        }
                    }

                    actions.append(action)
                    updated_count += 1

                if count % 500 == 0:
                    helpers.bulk(es, actions)
                    actions = []

                count += 1
        helpers.bulk(es, actions)

    print('Found {} x_linked_dominant samples'.format(count))


def annotate_x_linked_recessive(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    updated_count = 0
    for family_id, family in family_dict.items():
        child_id = family.get('child_id')
        # print(child_id)
        query_body = x_linked_recessive_vep_query_body_template % (
            child_id,
            range_rules['hg19/GRCh37'][0][0],
            range_rules['hg19/GRCh37'][0][1],
            range_rules['hg19/GRCh37'][1][0],
            range_rules['hg19/GRCh37'][1][1])
        # print(query_body)
        query_body = json.loads(query_body)
        for hit in helpers.scan(
                es,
                query=query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=index_name,
                doc_type=doc_type_name):
            # pprint.pprint(hit["_source"])
            es_id = hit['_id']
            sample_array = hit["_source"]["sample"]
            # print(len(sample_array))
            sample = pop_sample_with_id(sample_array, child_id)
            if is_x_linked_recessive(sample):
                mendelian_diseases = sample.get('mendelian_diseases')
                # sample['mendelian_diseases'] = 'x_linked_recessive'
                to_update = True
                if mendelian_diseases:
                    if isinstance(mendelian_diseases, list):
                        if 'x_linked_recessive' not in mendelian_diseases:
                            mendelian_diseases.append('x_linked_recessive')
                            print(es_id, mendelian_diseases)
                            to_update = True
                    elif isinstance(mendelian_diseases, str):
                        if mendelian_diseases != 'x_linked_recessive':
                            mendelian_diseases = [mendelian_diseases, 'x_linked_recessive']
                            print(es_id, mendelian_diseases)
                            to_update = True
                else:
                    sample['mendelian_diseases'] = 'x_linked_recessive'
                    to_update = True

                if to_update:
                    sample_array.append(sample)
                    action = {
                        "_index": index_name,
                        '_op_type': 'update',
                        "_type": doc_type_name,
                        "_id": es_id,
                        "doc": {
                            "sample": sample_array
                        }
                    }

                    actions.append(action)
                    updated_count += 1

                if count % 500 == 0:
                    helpers.bulk(es, actions)
                    actions = []

                count += 1
        helpers.bulk(es, actions)

    print('Found {} x_linked_recessive samples'.format(count))

def annotate_x_linked_denovo(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    updated_count = 0
    for family_id, family in family_dict.items():
        child_id = family.get('child_id')
        # print(child_id)
        query_body = x_linked_de_novo_query_body_template % (
            child_id,
            range_rules['hg19/GRCh37'][0][0],
            range_rules['hg19/GRCh37'][0][1],
            range_rules['hg19/GRCh37'][1][0],
            range_rules['hg19/GRCh37'][1][1])
        # print(query_body)
        query_body = json.loads(query_body)
        for hit in helpers.scan(
                es,
                query=query_body,
                scroll=u'5m',
                size=1000,
                preserve_order=False,
                index=index_name,
                doc_type=doc_type_name):
            # pprint.pprint(hit["_source"])
            es_id = hit['_id']
            sample_array = hit["_source"]["sample"]
            # print(len(sample_array))
            sample = pop_sample_with_id(sample_array, child_id)
            if is_x_linked_de_novo(sample):
                mendelian_diseases = sample.get('mendelian_diseases')
                # sample['mendelian_diseases'] = 'x_linked_de_novo'
                to_update = True
                if mendelian_diseases:
                    if isinstance(mendelian_diseases, list):
                        if 'x_linked_de_novo' not in mendelian_diseases:
                            mendelian_diseases.append('x_linked_denovo')
                            print(es_id, mendelian_diseases)
                            to_update = True
                    elif isinstance(mendelian_diseases, str):
                        if mendelian_diseases != 'x_linked_denovo':
                            mendelian_diseases = [mendelian_diseases, 'x_linked_denovo']
                            print(es_id, mendelian_diseases)
                            to_update = True
                else:
                    sample['mendelian_diseases'] = 'x_linked_denovo'
                    to_update = True

                if to_update:
                    sample_array.append(sample)
                    action = {
                        "_index": index_name,
                        '_op_type': 'update',
                        "_type": doc_type_name,
                        "_id": es_id,
                        "doc": {
                            "sample": sample_array
                        }
                    }

                    actions.append(action)
                    updated_count += 1

                if count % 500 == 0:
                    helpers.bulk(es, actions)
                    actions = []

                count += 1
        helpers.bulk(es, actions)

    print('Found {} x_linked_denovo samples'.format(count))


def annotate_compound_heterozygous(es, index_name, doc_type_name, family_dict):

    count = 0

    updated_count = 0
    for family_id, family in family_dict.items():

        child_id = family.get('child_id')
        # print(child_id)
        genes = get_vep_genes_from_es_for_compound_heterozygous(es, index_name, doc_type_name)
        for gene in genes:
            actions = []
            query_body = compound_heterozygous_query_body_template % (child_id, gene)
            query_body = json.loads(query_body)
            samples = []
            for hit in helpers.scan(
                    es,
                    query=query_body,
                    scroll=u'5m',
                    size=1000,
                    preserve_order=False,
                    index=index_name,
                    doc_type=doc_type_name):
                # pprint.pprint(hit["_source"])
                es_id = hit['_id']
                sample_array = hit["_source"]["sample"]
                # print(len(sample_array))
                sample = pop_sample_with_id_apply_compound_het_rules(sample_array, child_id)

                if not sample:
                    continue

                sample.update({'es_id': es_id})
                samples.append(sample)

            if len(samples) > 1 and are_variants_compound_heterozygous(samples):

                for sample in samples:
                    es_id = sample.pop("es_id")

                    mendelian_diseases = sample.get('mendelian_diseases')
                    # sample['mendelian_diseases'] = 'x_linked_de_novo'
                    to_update = True
                    if mendelian_diseases:
                        if isinstance(mendelian_diseases, list):
                            if 'compound_heterozygous' not in mendelian_diseases:
                                mendelian_diseases.append('compound_heterozygous')
                                print(es_id, mendelian_diseases)
                                to_update = True
                        elif isinstance(mendelian_diseases, str):
                            if mendelian_diseases != 'compound_heterozygous':
                                mendelian_diseases = [mendelian_diseases, 'compound_heterozygous']
                                print(es_id, mendelian_diseases)
                                to_update = True
                    else:
                        sample['mendelian_diseases'] = 'compound_heterozygous'
                        to_update = True


                    if to_update:
                        print(child_id, es_id)
                        res = es.get(index=index_name, doc_type=doc_type_name, id=es_id)
                        sample_array = res['_source']['sample']
                        _ = pop_sample_with_id_apply_compound_het_rules(sample_array, child_id)
                        sample_array.append(sample)
                        action = {
                            "_index": index_name,
                            '_op_type': 'update',
                            "_type": doc_type_name,
                            "_id": es_id,
                            "doc": {
                                "sample": sample_array
                            }
                        }

                        actions.append(action)
                        updated_count += 1

                    count += 1

            if actions:
                helpers.bulk(es, actions)

    print('Found {} compound_heterozygous samples'.format(count))

def main():
    import datetime
    index_name = "ashkenazitrio4families"
    doc_type_name = "ashkenazitrio4families_"
    es = elasticsearch.Elasticsearch(host='199.109.193.178', port=9200)
    family_dict = get_family_dict(es, index_name, doc_type_name)

    all_start_time = datetime.datetime.now()
    start_time = datetime.datetime.now()
    print('Starting annotate_autosomal_recessive', start_time)
    annotate_autosomal_recessive(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_autosomal_recessive',int((datetime.datetime.now() - start_time).total_seconds()))

    start_time = datetime.datetime.now()
    print('Starting annotate_denovo', start_time)
    annotate_denovo(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_denovo',int((datetime.datetime.now() - start_time).total_seconds()))

    start_time = datetime.datetime.now()
    print('Starting annotate_autosomal_dominant', start_time)
    annotate_autosomal_dominant(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_autosomal_dominant',int((datetime.datetime.now() - start_time).total_seconds()))

    start_time = datetime.datetime.now()
    print('Starting annotate_x_linked_dominant', start_time)
    annotate_x_linked_dominant(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_x_linked_dominant',int((datetime.datetime.now() - start_time).total_seconds()))

    start_time = datetime.datetime.now()
    print('Starting annotate_x_linked_recessive', start_time)
    annotate_x_linked_recessive(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_x_linked_recessive',int((datetime.datetime.now() - start_time).total_seconds()))

    start_time = datetime.datetime.now()
    print('Starting annotate_x_linked_denovo', start_time)
    annotate_x_linked_denovo(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_x_linked_denovo',int((datetime.datetime.now() - start_time).total_seconds()))

    start_time = datetime.datetime.now()
    print('Starting annotate_compound_heterozygous', start_time)
    annotate_compound_heterozygous(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_compound_heterozygous',int((datetime.datetime.now() - start_time).total_seconds()))

    print('Finished annotating all in seconds: ',int((datetime.datetime.now() - all_start_time).total_seconds()))


if __name__ == "__main__":
    main()
