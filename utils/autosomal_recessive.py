from elasticsearch import Elasticsearch
from elasticsearch import helpers

from pprint import pprint
import json

es = Elasticsearch(['http://199.109.193.178:9200/'])


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
                        { "match" : {"sample.Sample_ID" : "%s"} }
                    ],
                    "should" : [
                        { "match" : {"sample.GT" : "1/1"} },
                        { "match" : {"sample.GT" : "1|1"} }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
    }
}
"""

mother_id = "1805"
father_id = "1847"
child_id = "4805"


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

query = json.loads(query_string % (child_id))
autosomal_recessive_count = 0
total = 0
for ele in helpers.scan(es,
                        query=query,
                        scroll=u'5m',
                        size=1000,
                        preserve_order=False,
                        index='test1',
                        doc_type='test1_'):

    result = ele['_source']
    autosomal_recessive = is_autosomal_recessive(
        result.get('sample'), father_id, mother_id, child_id)
    if autosomal_recessive:
        autosomal_recessive_count += 1
        # print(autosomal_recessive_count, autosomal_recessive)
    total += 1

print('Total Candidates: ', total,
      'autosomal_recessive: ', autosomal_recessive_count)
