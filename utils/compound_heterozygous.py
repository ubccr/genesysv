import json
from pprint import pprint

from elasticsearch import Elasticsearch, helpers
from natsort import natsorted

es = Elasticsearch(['http://199.109.193.178:9200/'])


def get_from_es(dataset_es_index_name,
                dataset_es_type_name,
                dataset_es_host,
                dataset_es_port,
                field_es_name,
                field_path):

    es = Elasticsearch(
        host=dataset_es_host, port=dataset_es_port)

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
        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["buckets"] if ele['key']])

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

        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body, request_timeout=120)
        return natsorted([ele['key'] for ele in results["aggregations"]["values"]["values"]["buckets"] if ele['key']])


query_string = """
{
  "_source":[
    "Variant",
    "sample"
  ],
  "query":{
    "bool":{
      "filter":[
        {
          "nested":{
            "path":"CSQ_nested",
            "query":{
              "bool":{
                "filter":[
                  {
                    "term":{
                      "CSQ_nested.CSQ_nested_SYMBOL":"%s"
                    }
                  }
                ]
              }
            }
          }
        },
        {
          "nested":{
            "path":"sample",
            "query":{
              "bool":{
                "filter":[
                  {
                    "terms":{"sample.sample_GT":["0|1", "1|0", "0/1", "1/0"]}
                  },
                  {
                    "term":{"sample.sample_ID":"%s"}
                  }
                ]
              }
            }
          }
        }
      ]
    }
  }
}
"""

mother_id = "1805"
father_id = "1847"
child_id = "4805"


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
            compound_heterozygous_variants.append(ele)
            continue

        current_gt_pair = [father_gt, mother_gt]
        current_gt_pair.reverse()
        if gt_pair_whose_reverse_to_find == current_gt_pair:
            compound_heterozygous_variants.append(ele)
            compound_heterozygous_found = True

    if compound_heterozygous_found:
        return compound_heterozygous_variants
    else:
        return False


def get_compound_heterozygous_variants(genes, index_name, type_name):
    compound_heterozygous_variants = []
    total_variants = 0
    for gene in genes:
        query = json.loads(query_string % (gene, child_id))
        variant_genotypes = []
        for ele in helpers.scan(es,
                                query=query,
                                scroll=u'5m',
                                size=1000,
                                preserve_order=False,
                                index=index_name,
                                doc_type=type_name):

            result = ele['_source']
            es_id = ele['_id']
            variant_id = result.get('Variant')
            total_variants += 1
            length_results = len(result.get('sample'))
            variant_genotype = get_genotypes(
                result.get('sample'), father_id, mother_id, child_id)

            if variant_genotype:
                variant_genotype.insert(0, variant_id)
                variant_genotype.insert(0, es_id)
                variant_genotypes.append(variant_genotype)

        if len(variant_genotypes) > 1:
            found_compound_heterozygous = are_variants_compound_heterozygous(
                variant_genotypes)
            if found_compound_heterozygous:
                for variant in found_compound_heterozygous:
                    tmp = [gene] + variant[1:]
                    print('\t'.join(tmp))
                compound_heterozygous_variants.append(
                    found_compound_heterozygous)

    return (total_variants, compound_heterozygous_variants)


index_name = type_name = 'trio_trim'
genes = get_from_es(index_name,
                    type_name,
                    '199.109.193.178',
                    '9200',
                    'CSQ_nested_SYMBOL',
                    'CSQ_nested')

(total_variants, compound_heterozygous_variants) = get_compound_heterozygous_variants(
    genes, index_name, type_name)

print('Total Genes: ', len(genes), 'Total Variants Checked: ', total_variants,
      'Compound Heterozygous Found: ', len(compound_heterozygous_variants))
