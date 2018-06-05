import json
from collections import defaultdict
from pprint import pprint

import elasticsearch
from django.core.management.base import BaseCommand
from elasticsearch.helpers import scan
from tqdm import tqdm

from search.models import Dataset

inheritance_field_query = json.loads(
'''
{ 
    "query" : {
        "nested" : {
            "path" : "sample",
            "query": {
                "bool" : {
                    "should" : [
                    {
                        "exists" : {
                            "field" : "sample.sample_comp-het"
                        }
                    },
                    {
                        "exists" : {
                            "field" : "sample.sample_denovo"
                        }
                    },
                    {
                        "exists" : {
                            "field" : "sample.sample_hom-recess"
                        }
                    }
                    ]
                }
            }
        }
    }
}
''')


class Command(BaseCommand):

    def add_arguments(self, parser):
        # Positional arguments
        # parser.add_argument('request_id', type=int)
        parser.add_argument(
            '--dataset',
            action='store',
            dest='dataset',
            default=False,
            #help='Delete poll instead of closing it',

        )


    def handle(self, *args, **options):

        request_obj = Dataset.objects.get(es_index_name=options['dataset'])
        es = elasticsearch.Elasticsearch(host=request_obj.es_host, port=request_obj.es_port)

        for record in scan(es,query=inheritance_field_query,index=request_obj.es_index_name, doc_type=request_obj.es_type_name): 
            cleaned_record = defaultdict(dict)
            for sample in record['_source']['sample']:
                sample.pop('sample_denovo',1)
                sample.pop('sample_hom-recess',1)
                sample.pop('sample_comp-het',1)
            cleaned_record['doc']['sample'] = record['_source']['sample']
            #print(cleaned_record)
            es.update(index=request_obj.es_index_name, 
                      doc_type=request_obj.es_type_name, 
                      id=record['_id'],
                      body=json.loads(json.dumps(cleaned_record)))
