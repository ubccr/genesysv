from elasticsearch import Elasticsearch
from elasticsearch import helpers

from pprint import pprint
import json

es = Elasticsearch(['http://199.109.193.178:9200/'])
doc = {
        'size' : 10000,
        'query': {
            'match_all' : {}
       }
   }

# doc = {
#         'size' : 10000,
#         "query": {
#             "nested" : {
#                 "path" : "sample",
#                 "query" : {
#                     "bool" : {
#                         "filter" : [
#                             { "match" : {"sample.sample_id" : "1805"} }
#                             { "match" : {"sample.sample_GT" : "1805"} }
#                         ]
#                     }
#                 }
#             }
#         }
#    }



query_string = """
{
    "_source": ["Variant", "sample"],
    "query": {
        "nested" : {
            "path" : "sample",
            "score_mode" : "none",
            "query" : {
                "bool" : {
                    "filter" : [
                        { "match" : {"sample.sample_ID" : "%s"} }
                    ],
                    "must_not" : [
                        { "match" : {"sample.sample_GT" : "0/0"} },
                        { "match" : {"sample.sample_GT" : "0|0"} },
                        { "match" : {"sample.sample_GT" : "1/1"} },
                        { "match" : {"sample.sample_GT" : "1|1"} }
                    ]
                }
            }
        }
    }
}
"""
"""
Mendelian - de novo mutations selected:
Filter by this rule: mother_genotype == '0/0' or '0|0',
                  father_genotype == '0/0' or '0|0',
                  affected child_genotype == '0/1' or '0|1' or '1|0'.
"""
mother_id = "1805"
father_id = "1847"
child_id = "4805"

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

    if not all( (father_gt, mother_gt, child_gt)):
        return None

    case_1 = case_2 = False

    # Case 1
    """
    mother_genotype == '0/1' or '0|1' or '1|0',
    father_genotype == '0/0' or '0|0',
    affected child_genotype == '0/1'  or '0|1' or '1|0'.
    """
    if  mother_gt in ['0/1', '0|1', '1|0',] and father_gt in ['0/0', '0|0',]:
        case_1 = True

    # Case 2
    """
    mother_genotype == '0/0' or '0|0',
    father_genotype == '0/1' or '0|1' or '1|0',
    affected child_genotype == '0/1'  or '0|1' or '1|0'.
    """
    if  mother_gt in ['0/0', '0|0',] and father_gt in ['0/1', '0|1', '1|0',]:
        case_2 = True

    if any((case_1, case_2)):
        return (father_gt, mother_gt, child_gt)
    else:
        return None

query = json.loads(query_string % (child_id))
autosomal_dominant_count = 0
total = 0
for ele in helpers.scan(es,
                query=query,
                scroll=u'5m',
                size=10000,
                preserve_order=False,
                index='trio_trim',
                doc_type='trio_trim'):

    result = ele['_source']
    autosomal_dominant = is_autosomal_dominant(result.get('sample'), father_id, mother_id, child_id)
    if autosomal_dominant:
        autosomal_dominant_count += 1
        # print(autosomal_dominant_count, autosomal_dominant)
    total += 1

print('Total Candidates: ', total, 'autosomal_dominant: ', autosomal_dominant_count)
