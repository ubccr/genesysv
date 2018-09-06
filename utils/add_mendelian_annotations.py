import elasticsearch
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


autosomal_recessive_annovar_query_body_template = """{
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
             }
         ],
         "must_not" : [
             {"terms": {"CHROM": ["X", "Y"]}}
        ],
        "should" : [
             {"terms": {"ExonicFunc_ensGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
             {"terms": {"ExonicFunc_refGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
             {"term": {"Func_ensGene": "splicing"}},
             {"term": {"Func_refGene": "splicing"}}
        ],
        "minimum_should_match": 1

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
                        ],
                        "should" :[
                            {"term": {"sample.Mother_Phenotype": "2"}},
                            {"term": {"sample.Father_Phenotype": "2"}}
                        ],
                        "minimum_should_match": 1
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

compound_heterozygous_vep_query_body_template = """{
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


compound_heterozygous_annovar_query_body_template = """{
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
                 "path": "AAChange_refGene",
                 "query": {
                   "bool": {
                     "filter": [
                       {"term": {"AAChange_refGene.Gene": "%s"}}
                     ]
                   }
                 },
                 "score_mode": "none"
                }
             }
         ],
         "must_not" : [
             {"terms": {"CHROM": ["X", "Y"]}}
        ],
        "should" : [
             {"terms": {"ExonicFunc_ensGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
             {"terms": {"ExonicFunc_refGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
             {"term": {"Func_ensGene": "splicing"}},
             {"term": {"Func_refGene": "splicing"}}
        ],
        "minimum_should_match": 1
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
                            ],
                            "should" :[
                                {"term": {"sample.Mother_Phenotype": "2"}},
                                {"term": {"sample.Father_Phenotype": "2"}}
                            ],
                            "minimum_should_match": 1
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


x_linked_recessive_annovar_query_body_template = """{
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

        ],
        "should" : [
             {"terms": {"ExonicFunc_ensGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
             {"terms": {"ExonicFunc_refGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
             {"term": {"Func_ensGene": "splicing"}},
             {"term": {"Func_refGene": "splicing"}}
        ],
        "minimum_should_match": 1
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

    if sample_information.get('Mother_Phenotype') == '2' and sample_information.get('Father_Phenotype') == '2':
        return False

    if sample_information.get('Mother_Phenotype') == '2':
        if sample_information.get('Mother_Genotype') in ['0/1', '0|1', '1|0'] and sample_information.get('Father_Genotype') in ['0/0', '0|0']:
            return True
    # Case Father (Phenotype == 2)
    elif sample_information.get('Father_Phenotype') == '2':
        if sample_information.get('Mother_Genotype') in ['0/0', '0|0'] and sample_information.get('Father_Genotype') in ['0/1', '0|1', '1|0']:
            return True


    return False

def is_x_linked_dominant(sample_information):

    if sample_information.get('Mother_Phenotype') == '2' and sample_information.get('Father_Phenotype') == '2':
        return False

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

    return False

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

    return False

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

    return False

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



def get_annovar_genes_from_es_for_compound_heterozygous(es, index_name, doc_type_name):
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
                     }
                 ],
                 "must_not" : [
                     {"terms": {"CHROM": ["X", "Y"]}}
                ],
                "should" : [
                     {"terms": {"ExonicFunc_ensGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
                     {"terms": {"ExonicFunc_refGene": ["frameshift_deletion", "frameshift_insertion", "stopgain", "stoploss"]}},
                     {"term": {"Func_ensGene": "splicing"}},
                     {"term": {"Func_refGene": "splicing"}}
                ],
                "minimum_should_match": 1
             }
         },
         "size": 0,
        "aggs" : {
            "values" : {
                "nested" : {
                    "path" : "AAChange_refGene"
                },
                "aggs" : {
                    "values" : {"terms" : {"field" : "AAChange_refGene.Gene", "size" : 30000}}
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
                            "must" : [{"term": { "sample.Family_ID": "%s"}},
                                      {"exists": { "field": "sample.Father_ID"}},
                                      {"exists": { "field": "sample.Mother_ID"}}
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


def annotate_autosomal_recessive(es, index_name, doc_type_name, family_dict, annotation):

    count = 0
    actions = []

    keep_updating = True
    sample_matched = []
    while keep_updating:
        updated_count = 0
        for family_id, family in family_dict.items():
            child_id = family.get('child_id')
            # print(child_id)
            if annotation == 'vep':
                query_body = autosomal_recessive_vep_query_body_template % (child_id)
            elif annotation == 'annovar':
                query_body = autosomal_recessive_annovar_query_body_template % (child_id)
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
                sample = pop_sample_with_id(sample_array, child_id)
                tmp_id = es_id + child_id
                mendelian_diseases = sample.get('mendelian_diseases', [])
                if 'autosomal_recessive' in mendelian_diseases:
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    continue

                to_update = False
                if mendelian_diseases:
                    if 'autosomal_recessive' not in mendelian_diseases:
                        mendelian_diseases.append('autosomal_recessive')
                        to_update = True
                else:
                    to_update = True
                    sample['mendelian_diseases'] = ['autosomal_recessive']

                sample_array.append(sample)

                # print(es_id, child_id)

                if tmp_id not in sample_matched:
                    sample_matched.append(tmp_id)
                if to_update:
                    es.update(index=index_name, doc_type=doc_type_name, id=es_id,
                         body={"doc": {"sample": sample_array}})
                    updated_count += 1

        if updated_count == 0:
            keep_updating = False
    print('Found {} autosomal_recessive samples'.format(len(list(set(sample_matched)))))


def annotate_denovo(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    keep_updating = True
    sample_matched = []
    while keep_updating:
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

                es_id = hit['_id']
                sample_array = hit["_source"]["sample"]
                sample = pop_sample_with_id(sample_array, child_id)
                tmp_id = es_id + child_id
                mendelian_diseases = sample.get('mendelian_diseases', [])
                if 'denovo' in mendelian_diseases:
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    continue
                to_update = False
                if mendelian_diseases:
                    if 'denovo' not in mendelian_diseases:
                        mendelian_diseases.append('denovo')
                        print(es_id, mendelian_diseases)
                        to_update = True
                else:
                    sample['mendelian_diseases'] = ['denovo']
                    to_update = True

                sample_array.append(sample)

                if tmp_id not in sample_matched:
                    sample_matched.append(tmp_id)
                if to_update:
                    es.update(index=index_name, doc_type=doc_type_name, id=es_id,
                         body={"doc": {"sample": sample_array}})
                    updated_count += 1

        if updated_count == 0:
            keep_updating = False

    print('Found {} denovo samples'.format(len(list(set(sample_matched)))))


def annotate_autosomal_dominant(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    updated_count = 0
    keep_updating = True
    sample_matched = []
    while_loop_counter = 1
    while keep_updating:
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
                sample = pop_sample_with_id(sample_array, child_id)
                mendelian_diseases = sample.get('mendelian_diseases', [])
                tmp_id = es_id + child_id
                if 'autosomal_dominant' in mendelian_diseases:
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    continue
                if is_autosomal_dominant(sample):
                    to_update = False
                    if mendelian_diseases:
                        if 'autosomal_dominant' not in mendelian_diseases:
                            mendelian_diseases.append('autosomal_dominant')
                            print(es_id, mendelian_diseases)
                            to_update = True
                    else:
                        sample['mendelian_diseases'] = ['autosomal_dominant']
                        to_update = True

                    sample_array.append(sample)


                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    if to_update:
                        es.update(index=index_name, doc_type=doc_type_name, id=es_id,
                             body={"doc": {"sample": sample_array}})
                        updated_count += 1


        while_loop_counter += 1
        if updated_count == 0:
            keep_updating = False

    print('Found {} autosomal dominant samples'.format(len(list(set(sample_matched)))))


range_rules = {
    'hg19/GRCh37': ([60001, 2699520], [154931044, 155260560]),
    'hg38/GRCh38': ([10001, 2781479], [155701383, 156030895])
}


def annotate_x_linked_dominant(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    keep_updating = True
    sample_matched = []
    while keep_updating:
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
                sample = pop_sample_with_id(sample_array, child_id)
                tmp_id = es_id + child_id
                mendelian_diseases = sample.get('mendelian_diseases', [])
                if 'x_linked_dominant' in mendelian_diseases:
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    continue
                if is_x_linked_dominant(sample):
                    # sample['mendelian_diseases'] = 'x_linked_dominant'
                    to_update = False
                    if mendelian_diseases:
                        if 'x_linked_dominant' not in mendelian_diseases:
                            mendelian_diseases.append('x_linked_dominant')
                            print(es_id, mendelian_diseases)
                            to_update = True
                    else:
                        sample['mendelian_diseases'] = ['x_linked_dominant']
                        to_update = True

                    sample_array.append(sample)
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    if to_update:
                        es.update(index=index_name, doc_type=doc_type_name, id=es_id,
                             body={"doc": {"sample": sample_array}})
                        updated_count += 1


        if updated_count == 0:
            keep_updating = False

    print('Found {} x_linked_dominant samples'.format(len(list(set(sample_matched)))))


def annotate_x_linked_recessive(es, index_name, doc_type_name, family_dict, annotation):

    count = 0
    actions = []
    keep_updating = True
    sample_matched = []
    while keep_updating:
        updated_count = 0
        for family_id, family in family_dict.items():
            child_id = family.get('child_id')
            # print(child_id)
            if annotation == 'vep':
                query_body = x_linked_recessive_vep_query_body_template % (
                    child_id,
                    range_rules['hg19/GRCh37'][0][0],
                    range_rules['hg19/GRCh37'][0][1],
                    range_rules['hg19/GRCh37'][1][0],
                    range_rules['hg19/GRCh37'][1][1]
                )
            elif annotation == 'annovar':
                query_body = x_linked_recessive_annovar_query_body_template % (
                    child_id,
                    range_rules['hg19/GRCh37'][0][0],
                    range_rules['hg19/GRCh37'][0][1],
                    range_rules['hg19/GRCh37'][1][0],
                    range_rules['hg19/GRCh37'][1][1]
                )

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
                sample = pop_sample_with_id(sample_array, child_id)
                tmp_id = es_id + child_id
                mendelian_diseases = sample.get('mendelian_diseases', [])
                if 'x_linked_recessive' in mendelian_diseases:
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                        continue

                if is_x_linked_recessive(sample):

                    # sample['mendelian_diseases'] = 'x_linked_recessive'
                    to_update = False
                    if mendelian_diseases:
                        if 'x_linked_recessive' not in mendelian_diseases:
                            mendelian_diseases.append('x_linked_recessive')
                            print(es_id, mendelian_diseases)
                            to_update = True
                    else:
                        sample['mendelian_diseases'] = ['x_linked_recessive']
                        to_update = True

                    sample_array.append(sample)


                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)

                    if to_update:
                        es.update(index=index_name, doc_type=doc_type_name, id=es_id,
                             body={"doc": {"sample": sample_array}})
                        updated_count += 1


        if updated_count == 0:
            keep_updating = False

    print('Found {} x_linked_recessive samples'.format(len(list(set(sample_matched)))))


def annotate_x_linked_denovo(es, index_name, doc_type_name, family_dict):

    count = 0
    actions = []
    keep_updating = True
    sample_matched = []
    while keep_updating:
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
                sample = pop_sample_with_id(sample_array, child_id)
                tmp_id = es_id + child_id
                mendelian_diseases = sample.get('mendelian_diseases', [])
                if 'x_linked_denovo' in mendelian_diseases:
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                if is_x_linked_de_novo(sample):
                    mendelian_diseases = sample.get('mendelian_diseases')
                    # sample['mendelian_diseases'] = 'x_linked_de_novo'
                    to_update = False
                    if mendelian_diseases:
                        if 'x_linked_denovo' not in mendelian_diseases:
                            mendelian_diseases.append('x_linked_denovo')
                            print(type(mendelian_diseases), es_id, mendelian_diseases)
                            to_update = True
                    else:
                        sample['mendelian_diseases'] = ['x_linked_denovo']
                        to_update = True

                    sample_array.append(sample)

                    tmp_id = es_id + child_id
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    if to_update:
                        es.update(index=index_name, doc_type=doc_type_name, id=es_id,
                             body={"doc": {"sample": sample_array}})
                        updated_count += 1


        if updated_count == 0:
            keep_updating = False

    print('Found {} x_linked_denovo samples'.format(len(list(set(sample_matched)))))


def annotate_compound_heterozygous(es, index_name, doc_type_name, family_dict, annotation):

    count = 0

    keep_updating = True
    sample_matched = []
    while keep_updating:
        updated_count = 0
        for family_id, family in family_dict.items():

            child_id = family.get('child_id')
            # print(child_id)
            if annotation == 'vep':
                genes = get_vep_genes_from_es_for_compound_heterozygous(es, index_name, doc_type_name)
            elif annotation == 'annovar':
                genes = get_annovar_genes_from_es_for_compound_heterozygous(es, index_name, doc_type_name)

            for gene in genes:
                actions = []
                if annotation == 'vep':
                    query_body = compound_heterozygous_vep_query_body_template % (child_id, gene)
                elif annotation == 'annovar':
                    query_body = compound_heterozygous_annovar_query_body_template % (child_id, gene)
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
                        to_update = False
                        if mendelian_diseases:
                            if 'compound_heterozygous' not in mendelian_diseases:
                                mendelian_diseases.append('compound_heterozygous')
                                print(es_id, mendelian_diseases)
                                to_update = True
                        else:
                            sample['mendelian_diseases'] = ['compound_heterozygous']
                            to_update = True


                    sample_array.append(sample)

                    tmp_id = es_id + child_id
                    if tmp_id not in sample_matched:
                        sample_matched.append(tmp_id)
                    if to_update:
                        es.update(index=index_name, doc_type=doc_type_name, id=es_id,
                             body={"doc": {"sample": sample_array}})
                        updated_count += 1

        if updated_count == 0:
            keep_updating = False


    print('Found {} compound_heterozygous samples'.format(len(list(set(sample_matched)))))


def main():
    import datetime

    index_name = "test_4families_annovar"
    doc_type_name = "test_4families_annovar_"
    annotation = 'annovar'

    es = elasticsearch.Elasticsearch(host='199.109.192.181', port=9200)
    family_dict = get_family_dict(es, index_name, doc_type_name)
    pprint.pprint(family_dict)

    all_start_time = datetime.datetime.now()

    start_time = datetime.datetime.now()
    print('Starting annotate_autosomal_recessive', start_time)
    annotate_autosomal_recessive(es, index_name, doc_type_name, family_dict, annotation)
    print('Finished annotate_autosomal_recessive', int((datetime.datetime.now() - start_time).total_seconds()), 'seconds')

    start_time = datetime.datetime.now()
    print('Starting annotate_denovo', start_time)
    annotate_denovo(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_denovo', int((datetime.datetime.now() - start_time).total_seconds()), 'seconds')

    start_time = datetime.datetime.now()
    print('Starting annotate_autosomal_dominant', start_time)
    annotate_autosomal_dominant(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_autosomal_dominant', int((datetime.datetime.now() - start_time).total_seconds()), 'seconds')

    start_time = datetime.datetime.now()
    print('Starting annotate_x_linked_dominant', start_time)
    annotate_x_linked_dominant(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_x_linked_dominant', int((datetime.datetime.now() - start_time).total_seconds()), 'seconds')

    start_time = datetime.datetime.now()
    print('Starting annotate_x_linked_recessive', start_time)
    annotate_x_linked_recessive(es, index_name, doc_type_name, family_dict, annotation)
    print('Finished annotate_x_linked_recessive', int((datetime.datetime.now() - start_time).total_seconds()), 'seconds')

    start_time = datetime.datetime.now()
    print('Starting annotate_x_linked_denovo', start_time)
    annotate_x_linked_denovo(es, index_name, doc_type_name, family_dict)
    print('Finished annotate_x_linked_denovo', int((datetime.datetime.now() - start_time).total_seconds()), 'seconds')

    start_time = datetime.datetime.now()
    print('Starting annotate_compound_heterozygous', start_time)
    annotate_compound_heterozygous(es, index_name, doc_type_name, family_dict, annotation)
    print('Finished annotate_compound_heterozygous', int((datetime.datetime.now() - start_time).total_seconds()), 'seconds')

    print('Finished annotating all in ', int((datetime.datetime.now() - all_start_time).total_seconds()), 'seconds')


if __name__ == "__main__":
    main()
