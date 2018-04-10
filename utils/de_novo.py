from elasticsearch import Elasticsearch
from elasticsearch import helpers

from pprint import pprint
import json

es = Elasticsearch(['http://199.109.193.178:9200/'])


denovo_query_string = """
{
    "_source": ["Variant", "sample"],
    "query": {
        "nested" : {
            "path" : "sample",
            "score_mode" : "none",
            "query" : {
                "bool" : {
                    "filter" : [
                        { "term" : {"sample.sample_ID" : "%s"} }
                    ],
                    "must_not" : [
                        { "term" : {"sample.sample_GT" : "0/0"} },
                        { "term" : {"sample.sample_GT" : "0|0"} }
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

    if not all((father_gt, mother_gt, child_gt)):
        return None

    return (father_gt, mother_gt, child_gt)

denovo_query = json.loads(denovo_query_string % (child_id))
denovo_count = 0
total = 0
for ele in helpers.scan(es,
                        query=denovo_query,
                        scroll=u'5m',
                        size=1000,
                        preserve_order=False,
                        index='trio_trim',
                        doc_type='trio_trim'):

    result = ele['_source']
    denovo = is_denovo(result.get('sample'), father_id, mother_id, child_id)
    if denovo:
        denovo_count += 1
        # print(denovo_count, denovo)

    total += 1
print('Total Candidates: ', total, 'denovo: ', denovo_count)
