import json
from collections import defaultdict
from pprint import pprint

import elasticsearch
from django.core.management.base import BaseCommand
from tqdm import tqdm

from inheritance_analysis.models import InheritanceAnalysisRequest
from msea.models import Gene
from search.models import *
from search.utils import get_from_es


class Command(BaseCommand):

    def add_arguments(self, parser):
        # Positional arguments
        # parser.add_argument('request_id', type=int)
        parser.add_argument(
            '--request_id',
            action='store',
            dest='request_id',
        )
        parser.add_argument(
            '--tab_name',
            action='store',
            dest='tab_name',
        )


    def handle(self, *args, **options):

        request_id = options['request_id']
        tab_name = options['tab_name']

        request_obj = InheritanceAnalysisRequest.objects.get(id=request_id)
        dataset_obj = request_obj.dataset
        tab_name = 'Basic'
        #print(request_obj)

        # # gl = ['ADAMTSL1','VAV3','SYNE2'] # test gene set that has complex hets
        es = elasticsearch.Elasticsearch(host=dataset_obj.es_host, port=dataset_obj.es_port)

        filter_tab_obj = FilterTab.objects.get(dataset=dataset_obj, name=tab_name)
        filter_panel_obj, _ = FilterPanel.objects.get_or_create(name='Inheritance Analysis', dataset=dataset_obj)

        if not filter_tab_obj.filter_panels.filter(id=filter_panel_obj.id):
            filter_tab_obj.filter_panels.add(filter_panel_obj)

        attribute_tab_obj = AttributeTab.objects.get(dataset=dataset_obj, name=tab_name)
        attribute_panel_obj, _ = AttributePanel.objects.get_or_create(name='Inheritance Analysis', dataset=dataset_obj)

        if not attribute_tab_obj.attribute_panels.filter(id=attribute_panel_obj.id):
            attribute_tab_obj.attribute_panels.add(attribute_panel_obj)

        ## Add exist fields

        form_type_obj = FormType.objects.get(name='ChoiceField')
        widget_type_obj = WidgetType.objects.get(name='Select')
        es_filter_type_obj = ESFilterType.objects.get(name='nested_filter_exists')


        exist_fields = (
            ("Is Complex Heterozygous?", "sample_comp-het", "sample"),
            ("Is Denovo?", "sample_denovo", "sample"),
            ("Is Homozygous Recessive?", "sample_hom-recess", "sample")
        )
        for field_display_text, field_es_name, field_path in exist_fields:
            filter_field_obj, created = FilterField.objects.get_or_create(dataset=dataset_obj,
                                                                   display_text=field_display_text,
                                                                   #in_line_tooltip=field_in_line_tooltip,
                                                                   #tooltip=field_tooltip,
                                                                   form_type=form_type_obj,
                                                                   widget_type=widget_type_obj,
                                                                   es_name=field_es_name,
                                                                   path=field_path,
                                                                   es_data_type='keyword',
                                                                   #es_text_analyzer=field_es_text_analyzer,
                                                                   es_filter_type=es_filter_type_obj,
                                                                   place_in_panel=filter_panel_obj.name)





            try:
                attribute_field_obj, _ = AttributeField.objects.get_or_create(dataset=dataset_obj,
                                                       display_text=field_display_text,
                                                       es_name=field_es_name,
                                                       path=field_path,
                                                       place_in_panel=attribute_panel_obj.name)
            except:
                pass

            if not filter_panel_obj.filter_fields.filter(id=filter_field_obj.id):
                filter_panel_obj.filter_fields.add(filter_field_obj)


            if not attribute_panel_obj.attribute_fields.filter(id=attribute_field_obj.id):
                attribute_panel_obj.attribute_fields.add(attribute_field_obj)

        ## Add Family filter fields

        form_type_obj = FormType.objects.get(name='MultipleChoiceField')
        widget_type_obj = WidgetType.objects.get(name='SelectMultiple')
        es_filter_type_obj = ESFilterType.objects.get(name='nested_filter_terms')


        family_filter_fields = (
            ("Complex Heterozygous", "sample_comp-het", "sample"),
            ("Denovo", "sample_denovo", "sample"),
            ("Homozygous Recessive?", "sample_hom-recess", "sample")
        )
        for field_display_text, field_es_name, field_path in family_filter_fields:
            filter_field_obj, created = FilterField.objects.get_or_create(dataset=dataset_obj,
                                                                   display_text=field_display_text,
                                                                   #in_line_tooltip=field_in_line_tooltip,
                                                                   #tooltip=field_tooltip,
                                                                   form_type=form_type_obj,
                                                                   widget_type=widget_type_obj,
                                                                   es_name=field_es_name,
                                                                   path=field_path,
                                                                   es_data_type='keyword',
                                                                   #es_text_analyzer=field_es_text_analyzer,
                                                                   es_filter_type=es_filter_type_obj,
                                                                   place_in_panel=filter_panel_obj.name)

            field_values = get_from_es(dataset_obj.es_index_name,
                                        dataset_obj.es_type_name,
                                        dataset_obj.es_host,
                                        dataset_obj.es_port,
                                        field_es_name,
                                        field_path)

            print(field_values)

            if field_values:
                for choice in field_values:
                    FilterFieldChoice.objects.get_or_create(filter_field=filter_field_obj, value=choice)

            if not filter_panel_obj.filter_fields.filter(id=filter_field_obj.id):
                filter_panel_obj.filter_fields.add(filter_field_obj)
