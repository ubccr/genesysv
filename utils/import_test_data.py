import pprint
import os
from core.commands.create_gui import add_required_data_to_db

add_required_data_to_db()


dir_path = os.path.dirname(os.path.realpath(__file__))
data = {}
with open(os.path.join(dir_path, 'test_data.csv'), 'r') as fp:


    for line in fp:
        if line.startswith('#'):
            continue

        Variant, CHROM, POS, dbSNP_ID, REF, ALT, VariantType, Sample_ID, Sample_GT, ExonicFunc_refGene = line.strip().split(',')

        if Variant not in data:
            tmp_dict = {
                'Variant': Variant,
                'CHROM': CHROM,
                'POS': POS,
                'dbSNP_ID': dbSNP_ID,
                'REF': REF,
                'ALT': ALT,
                'VariantType': VariantType,
                'ExonicFunc_refGene': ExonicFunc_refGene,
                'sample': []
            }
            data[Variant] = tmp_dict

        sample_dict = {
            'Sample_ID': Sample_ID,
            'Sample_GT': Sample_GT
        }

        data[Variant]['sample'].append(sample_dict)


data_array = []
for key, values in data.items():
    data_array.append(values)



import elasticsearch
es = elasticsearch.Elasticsearch(host='172.17.57.17', port=9200)
index_name = "test_data"
type_name = "test_data"
if es.indices.exists(index_name):
    es.indices.delete(index_name)
es.indices.create(index_name)
es.cluster.health(wait_for_status="yellow")



body = {
  type_name: {
    "properties": {
      "Variant": {
        "type": "keyword"
      },
      "CHROM": {
        "type": "keyword"
      },
      "POS": {
        "type": "integer"
      },
      "dbSNP_ID": {
        "type": "keyword"
      },
      "REF": {
        "type": "keyword"
      },
      "ALT": {
        "type": "keyword"
      },
      "VariantType": {
        "type": "keyword"
      },
      "ExonicFunc_refGene": {
        "type": "keyword"
      },
      "sample": {
        "type": "nested",
        "properties": {
          "sample.Sample_ID": {
            "type": "keyword"
          },
          "sample.Sample_GT": {
            "type": "keyword"
          }
        }
      }
    }
  }
}


es.indices.put_mapping(index=index_name, doc_type=type_name, body=body)


for body in data_array:
    es.index(index=index_name, doc_type=type_name, body=body)


