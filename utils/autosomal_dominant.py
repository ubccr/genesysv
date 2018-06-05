import json
from pprint import pprint

from elasticsearch import Elasticsearch, helpers

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
#                             { "match" : {"sample.GT" : "1805"} }
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
                        { "term" : {"sample.Sample_ID" : "%s"} },
                        { "terms" : {"sample.GT" : ["0/1", "0|1", "1|0"] }}
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
    mother_gt = father_gt = child_gt = 'N/A'
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
            if child_gt not in ['0/1', '0|1', '1|0']:
                return None

    if not all( (father_gt, mother_gt, child_gt)):
        return None

    case_1 = case_2 = False

    # Case 1
    """
    mother_genotype == '0/1' or '0|1' or '1|0',
    father_genotype == '0/0' or '0|0',
    affected child_genotype == '0/1'  or '0|1' or '1|0'.
    """
    if  mother_gt in ['0/1', '0|1', '1|0',] and father_gt == 'N/A':
        case_1 = True

    # Case 2
    """
    mother_genotype == '0/0' or '0|0',
    father_genotype == '0/1' or '0|1' or '1|0',
    affected child_genotype == '0/1'  or '0|1' or '1|0'.
    """
    if  mother_gt == 'N/A' and father_gt in ['0/1', '0|1', '1|0',]:
        case_2 = True


    if child_gt == 'N/A':
        print('How did this happen?')
        pprint(sample_array)
        return None

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
                index='test1',
                doc_type='test1_'):

    result = ele['_source']
    autosomal_dominant = is_autosomal_dominant(result.get('sample'), father_id, mother_id, child_id)
    if autosomal_dominant:
        autosomal_dominant_count += 1
        # print(autosomal_dominant_count, autosomal_dominant)
    total += 1

print('Total Candidates: ', total, 'autosomal_dominant: ', autosomal_dominant_count)
