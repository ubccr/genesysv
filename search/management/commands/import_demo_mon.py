from django.core.management.base import BaseCommand
from search.models import *

EYE_COLOR_CHOICES = ( "blue", "brown", "green")

IS_ACTIVE_CHOICES = ("true","false")

class Command(BaseCommand):



    def handle(self, *args, **options):

        studies = (
            ('Demo Study', ''),
        )
        datasets = (
            # ('sim_wgs_one_node_cluster',
            #  'Static Induced Myopathy WGS (1-node Cluster)',
            #  'http://199.109.192.242:9200/',
            #  'sim',
            #  'wgs_hg19_multianno'),
            ('demo_dataset',
             'Demo Dataset',
             'demo_mon',
             'demo_mon',
             '199.109.195.45',
             '9200'),
        )

        for name, description in studies:
            Study.objects.get_or_create(name=name,
                                        description=description)

        study_obj = Study.objects.get(name='Demo Study')

        for name, description, es_index_name, es_type_name, es_host, es_port in datasets:
            dataset_object, _ = Dataset.objects.get_or_create(study=study_obj,
                                          name=name,
                                          description=description,
                                          es_index_name=es_index_name,
                                          es_type_name=es_type_name,
                                          es_host=es_host,
                                          es_port=es_port,
                                          is_public=True)



        SearchOptions.objects.get_or_create(dataset=dataset_object)

        dataset_object = Dataset.objects.get(name='demo_dataset')

        with open('./search/management/commands/data/demo_mon_filter.txt','r') as fp:
            for line in fp:
                print(line)
                if line.startswith("#"):
                    continue

                print(line)
                row = [ele.strip() for ele in line.split('\t')]
                es_name, display_name, in_line_tooltip, tooltip, default_value, form_type, widget_type, es_filter_type, path = row

                if path == 'None':
                    path = ''

                form_type_obj = FormType.objects.get(name=form_type)
                widget_type_obj = WidgetType.objects.get(name=widget_type)
                es_filter_type_obj = ESFilterType.objects.get(name=es_filter_type)

                FilterField.objects.get_or_create(
                                            dataset=dataset_object,
                                            es_name=es_name.strip(),
                                            display_name=display_name.strip(),
                                            in_line_tooltip=in_line_tooltip.strip(),
                                            tooltip=tooltip.strip(),
                                            default_value=default_value.strip(),
                                            form_type=form_type_obj,
                                            widget_type=widget_type_obj,
                                            es_filter_type=es_filter_type_obj,
                                            path=path
                                            )



        with open('./search/management/commands/data/demo_mon_attribute.txt','r') as fp:
            for line in fp:
                if line.startswith("#"):
                    continue

                print(line)
                row = [ele.strip() for ele in line.split('\t')]
                es_name, display_name, path = row

                if path == 'None':
                    path = ''

                AttributeField.objects.get_or_create(dataset=dataset_object,
                                                     es_name=es_name,
                                                     display_name=display_name,
                                                     path=path
                                            )

        eye_color_obj = FilterField.objects.get(dataset=dataset_object, es_name="eyeColor")
        for choice in EYE_COLOR_CHOICES:
            FilterFieldChoice.objects.get_or_create(filter_field=eye_color_obj, value=choice)

        is_active_obj = FilterField.objects.get(dataset=dataset_object, es_name="isActive")
        for choice in IS_ACTIVE_CHOICES:
            FilterFieldChoice.objects.get_or_create(filter_field=is_active_obj, value=choice)


        filter_tab_obj, _ = FilterTab.objects.get_or_create(dataset=dataset_object, name='Simple')

        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Demo Panel')



        attribute_tab_obj, _ = AttributeTab.objects.get_or_create(dataset=dataset_object, name='Simple')

        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Demo Panel')

       ##
        filter_panel = (
            ('index', 'filter_term'),
            ('isActive', 'filter_term'),
            ('balance', 'filter_range_lte'),
            ('balance', 'filter_range_gte'),
            ('age', 'filter_range_lte'),
            ('age', 'filter_range_gte'),
            ('eyeColor', 'filter_terms'),
            ('first', 'filter_term'),
            ('last', 'filter_term'),
            ('tag', 'filter_term'),
            ('friend_id', 'nested_filter_term'),
            ('friend_name', 'nested_filter_term'),
            ('favoriteFruit', 'filter_term'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Demo Panel')
        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)

       ##
        attribute_panel = (
            ('index', ''),
            ('isActive', ''),
            ('balance', ''),
            ('age', ''),
            ('eyeColor', ''),
            ('first', ''),
            ('last', ''),
            ('tag', ''),
            ('friend_id', 'friend'),
            ('friend_name', 'friend'),
            ('favoriteFruit', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Demo Panel')

        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_panel_obj.attribute_fields.add(attribute_field_obj)
