import json

import elasticsearch
from django.core import serializers


class DownloadAllResultsAsOTUTable:
    necessary_fields = ['BMlabid', 'value', 'taxonomy', ]
    null_value = 0

    def __init__(self, search_log_obj):
        self.search_log_obj = search_log_obj
        self.query_body = json.loads(self.search_log_obj.query)
        self.BMlabids = []
        self.otu_table_dict = {}
        self.otu_table_formatted = {}

    def add_necessary_fields(self):
        for field in self.necessary_fields:
            if field not in self.query_body['_source']:
                self.query_body['_source'].append(field)

    def execute_query(self):

        self.add_necessary_fields()

        es = elasticsearch.Elasticsearch(
            host=self.search_log_obj.dataset.es_host)
        for hit in elasticsearch.helpers.scan(es,
                                              query=self.query_body,
                                              scroll=u'5m',
                                              size=1000,
                                              preserve_order=False,
                                              index=self.search_log_obj.dataset.es_index_name,
                                              doc_type=self.search_log_obj.dataset.es_type_name):
            tmp_source = hit['_source']
            es_id = hit['_id']
            inner_hits = hit.get('inner_hits')

            tmp_source['es_id'] = es_id
            if inner_hits:
                for key, value in inner_hits.items():
                    if key not in tmp_source:
                        tmp_source[key] = []
                    hits_hits_array = inner_hits[key]['hits']['hits']
                    for hit in hits_hits_array:
                        tmp_hit_dict = {}
                        for hit_key, hit_value in hit['_source'].items():
                            tmp_hit_dict[hit_key] = hit_value

                        if tmp_hit_dict:
                            tmp_source[key].append(tmp_hit_dict)

            taxonomy = tmp_source.get('taxonomy')
            value = tmp_source.get('value')
            BMlabid = tmp_source.get('BMlabid')

            if BMlabid not in self.BMlabids:
                self.BMlabids.append(BMlabid)

            if taxonomy not in self.otu_table_dict:
                self.otu_table_dict[taxonomy] = {}

            self.otu_table_dict[taxonomy].update({BMlabid: value})

    def format_otu_table(self):
        self.BMlabids.sort()

        rows = []
        header = ['#OTU ID', ]
        header.extend(self.BMlabids)
        header.append('taxonomy')
        taxonomies = sorted(self.otu_table_dict.keys())

        rows.append(header)

        for idx, taxonomy in enumerate(taxonomies):
            row = ['OTU_%d' % (idx), ]
            for BMlabid in self.BMlabids:
                row.append(self.otu_table_dict[taxonomy].get(BMlabid, self.null_value))

            row.append(taxonomy)

            rows.append(row)
        self.otu_table_formatted = rows

    def get_otu_table_dict(self):
        return self.otu_table_dict

    def get_otu_table_formatted(self):
        return self.otu_table_formatted

    def yield_rows(self):
        for row in self.otu_table_formatted:
            yield row
