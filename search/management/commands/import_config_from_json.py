from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import ValidationError
from search.models import *
import elasticsearch
import json
import sys
import re


def get_from_es(dataset_es_index_name,
                dataset_es_type_name,
                dataset_es_host,
                dataset_es_port,
                field_es_name,
                field_path):


    es = elasticsearch.Elasticsearch(host=dataset_es_host, port=dataset_es_port)

    if not field_path:
        body_non_nested_template = """
            {
                "size": 0,
                "aggs" : {
                    "values" : {
                        "terms" : { "field" : "%s", "size" : 1000 }
                    }
                }
            }
        """
        body = body_non_nested_template %(field_es_name)
        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body)
        return [ele['key'] for ele in results["aggregations"]["values"]["buckets"]]


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
                            "values" : {"terms" : {"field" : "%s.%s", "size" : 1000}}
                        }
                    }
                }
            }
        """
        body = body_nested_template %(field_path,
                                      field_path,
                                      field_es_name)


        results = es.search(index=dataset_es_index_name,
                            doc_type=dataset_es_type_name,
                            body=body)
        return [ele['key'] for ele in results["aggregations"]["values"]["values"]["buckets"]]


def fetch_data_type_from_es(dataset_es_index_name,
                            dataset_es_type_name,
                            dataset_es_host,
                            dataset_es_port,
                            field_es_name,
                            field_path):

    es = elasticsearch.Elasticsearch(host=dataset_es_host, port=dataset_es_port)
    mapping = es.indices.get_mapping(index=dataset_es_index_name, doc_type=dataset_es_type_name)

    if field_path:
        try:
            return mapping[dataset_es_index_name]['mappings'][dataset_es_type_name]['properties'][field_path]['properties'][field_es_name]['type']
        except KeyError as e:
            raise(e)
    else:
        try:
            return mapping[dataset_es_index_name]['mappings'][dataset_es_type_name]['properties'][field_es_name]['type']
        except KeyError as e:
            raise(e)




def validate_and_import_data(data, import_data=False):
    # Pull study name
    try:
        study_name = data["study"]["name"]
    except KeyError as e:
        raise(e)


    # Pull study name
    study_description = data["study"].get('description', '')

    # Pull study dataset
    try:
        dataset = data["study"]["dataset"]
    except KeyError as e:
        raise(e)

    # Pull dataset name
    try:
        dataset_name = dataset["name"]
    except KeyError as e:
        raise(e)

    # Pull dataset description
    try:
        dataset_description = dataset["description"]
    except KeyError as e:
        raise(e)

    # Pull dataset es_index_name
    try:
        dataset_es_index_name = dataset["es_index_name"]
    except KeyError as e:
        raise(e)


    # Pull dataset es_type_name
    try:
        dataset_es_type_name = dataset["es_type_name"]
    except KeyError as e:
        raise(e)

    # Pull dataset es_host
    try:
        dataset_es_host = dataset["es_host"]
    except KeyError as e:
        raise(e)

    # Pull dataset es_port
    try:
        dataset_es_port = dataset["es_port"]
    except KeyError as e:
        raise(e)

    # Pull dataset is_public
    dataset_is_public = dataset.get("is_public", False)

    # Pull dataset filters
    try:
        filters = dataset["filters"]
    except KeyError as e:
        raise(e)

    # Pull dataset attributes
    try:
        attributes = dataset["attributes"]
    except KeyError as e:
        raise(e)


    print("*"*80+"\n")
    print('Study Name: %s' %(study_name))
    print('Study Description: %s' %(study_description))
    print('Dataset Name: %s' %(dataset_name))
    print('Dataset Description: %s' %(dataset_description))
    print('Dataset ES Index Name: %s' %(dataset_es_index_name))
    print('Dataset ES Type Name: %s' %(dataset_es_type_name))
    print('Dataset ES Host: %s' %(dataset_es_host))
    print('Dataset ES Port: %s' %(dataset_es_port))
    print('Dataset Is Public: %s' %(dataset_is_public))


    ### Verify Study against Django models
    study_obj, created = Study.objects.get_or_create(name=study_name, description=study_description)

    ### Verify Dataset against Django models
    dataset_obj, created = Dataset.objects.get_or_create(  study=study_obj,
                            name=dataset_name,
                            description=dataset_description,
                            es_index_name=dataset_es_index_name,
                            es_type_name=dataset_es_type_name,
                            es_host=dataset_es_host,
                            es_port=dataset_es_port,
                            is_public=dataset_is_public)

    SearchOptions.objects.get_or_create(dataset=dataset_obj)

    # Pull filter tabs
    try:
        filter_tabs = filters["tabs"]
    except KeyError as e:
        raise(e)

    if len(filter_tabs) < 1:
        raise ValueError('Expected at least one tab for filters, but "tabs" is empty!')

    for tab in filter_tabs:
        try:
            tab_name = tab["name"]
        except KeyError as e:
            raise(e)

        filter_tab_obj, _ = FilterTab.objects.get_or_create(dataset=dataset_obj, name=tab_name)

        # Pull panels
        try:
            panels = tab["panels"]
        except KeyError as e:
            raise(e)

        if len(panels) < 1:
            raise ValueError('Expected at least one panel, but "panels" is empty!')


        for panel in panels:
            # Pull panel name
            try:
                panel_name = panel["name"]
            except KeyError as e:
                raise(e)

            filter_panel_obj, _ = FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj,
                                                             name=panel_name)

            try:
                fields = panel["fields"]
            except KeyError as e:
                raise(e)

            if len(fields) < 1:
                raise ValueError('Expected at least one filter field, but "fields" is empty!')


            for idx, field in enumerate(fields, 1):
                # required fields
                try:
                    field_display_name = field["display_name"]
                except KeyError as e:
                    raise(e)

                try:
                    field_form_type = field["form_type"]
                except KeyError as e:
                    raise(e)

                try:
                    field_widget_type = field["widget_type"]
                except KeyError as e:
                    raise(e)

                try:
                    field_es_name = field["es_name"]
                except KeyError as e:
                    raise(e)

                try:
                    field_es_filter_type = field["es_filter_type"]
                except KeyError as e:
                    raise(e)

                try:
                    field_path = field["path"]
                except KeyError as e:
                    raise(e)

                ############### optional fields

                # Pull in_line_tooltip
                field_in_line_tooltip = field.get('in_line_tooltip', '')

                # Pull tooltip
                field_tooltip = field.get('tooltip', '')

                # Pull values
                field_values = field.get('values', '')

                match_status = False
                if isinstance(field_values, str):
                    match = re.search(r'python_eval(.+)', field_values)
                    if field_values == 'get_from_es()':
                        field_values = get_from_es(dataset_es_index_name,
                                            dataset_es_type_name,
                                            dataset_es_host,
                                            dataset_es_port,
                                            field_es_name,
                                            field_path)
                    elif match:
                        match_status = True
                        tmp_str = match.groups()[0]
                        try:
                            field_values = eval(tmp_str)
                        except NameError as e:
                            print('Failed to evaluate %s' %(tmp_str))
                            raise(e)



                #fetch data_type
                field_es_data_type = fetch_data_type_from_es(dataset_es_index_name,
                                        dataset_es_type_name,
                                        dataset_es_host,
                                        dataset_es_port,
                                        field_es_name,
                                        field_path)


                print("\n%s --- Filter Field" %(idx))
                print("Filter Tab Name: %s" %(tab_name))
                print("Filter Panel Name: %s" %(panel_name))
                print("Filter Display Name: %s" %(field_display_name))
                print("Filter in Line Tooltip: %s" %(field_in_line_tooltip))
                print("Filter Tooltip: %s" %(field_tooltip))
                print("Filter Form Type: %s" %(field_form_type))
                print("Filter Widget Type: %s" %(field_widget_type))
                print("Filter ES Name: %s" %(field_es_name))
                print("Filter Path: %s" %(field_path))
                print("Filter ES Filter Type: %s" %(field_es_filter_type))
                print("Filter ES Data Type: %s" %(field_es_data_type))
                print("Filter Values: %s" %(field_values))
                # print(dict(dataset=dataset_obj,
                #            display_name=field_display_name,
                #            in_line_tooltip=field_in_line_tooltip,
                #            tooltip=field_tooltip,
                #            form_type__name=field_form_type,
                #            widget_type__name=field_widget_type,
                #            es_name=field_es_name,
                #            path=field_path,
                #            es_filter_type__name=field_es_filter_type
                #            )

                form_type_obj = FormType.objects.get(name=field_form_type)
                widget_type_obj = WidgetType.objects.get(name=field_widget_type)
                es_filter_type_obj = ESFilterType.objects.get(name=field_es_filter_type)

                filter_field_obj, created = FilterField.objects.get_or_create(dataset=dataset_obj,
                                                           display_name=field_display_name,
                                                           in_line_tooltip=field_in_line_tooltip,
                                                           tooltip=field_tooltip,
                                                           form_type=form_type_obj,
                                                           widget_type=widget_type_obj,
                                                           es_name=field_es_name,
                                                           path=field_path,
                                                           es_data_type=field_es_data_type,
                                                           es_filter_type=es_filter_type_obj)


                if not filter_panel_obj.filter_fields.filter(id=filter_field_obj.id):
                    filter_panel_obj.filter_fields.add(filter_field_obj)

                if field_values:
                    for choice in field_values:
                        FilterFieldChoice.objects.get_or_create(filter_field=filter_field_obj, value=choice)



    # Pull filter tabs
    try:
        attribute_tabs = attributes["tabs"]
    except KeyError as e:
        raise(e)

    if len(attribute_tabs) < 1:
        raise ValueError('Expected at least one tab for attributes, but "tabs" is empty!')



    for tab in attribute_tabs:
        try:
            tab_name = tab["name"]
        except KeyError as e:
            raise(e)


        attribute_tab_obj, _ = AttributeTab.objects.get_or_create(dataset=dataset_obj, name=tab_name)

        # Pull panels
        try:
            panels = tab["panels"]
        except KeyError as e:
            raise(e)

        if len(panels) < 1:
            raise ValueError('Expected at least one panel, but "panels" is empty!')


        for panel in panels:
            # Pull panel name
            try:
                panel_name = panel["name"]
            except KeyError as e:
                raise(e)

            try:
                fields = panel["fields"]
            except KeyError as e:
                raise(e)

            if len(fields) < 1:
                raise ValueError('Expected at least one filter field, but "fields" is empty!')


            attribute_panel_obj, _ = AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj,
                                                             name=panel_name)

            for idx, field in enumerate(fields, 1):
                # required fields
                try:
                    field_display_name = field["display_name"]
                except KeyError as e:
                    raise(e)

                try:
                    field_es_name = field["es_name"]
                except KeyError as e:
                    raise(e)

                try:
                    field_path = field["path"]
                except KeyError as e:
                    raise(e)

                ############### optional fields

                # Pull in_line_tooltip
                field_in_line_tooltip = field.get('in_line_tooltip', '')

                # Pull tooltip
                field_tooltip = field.get('tooltip', '')


                print("\n%s --- Attribute Field" %(idx))
                print("Attribute Tab Name: %s" %(tab_name))
                print("Attribute Panel Name: %s" %(panel_name))
                print("Attribute Display Name: %s" %(field_display_name))
                print("Attribute ES Name: %s" %(field_es_name))


                attribute_field_obj, created = AttributeField.objects.get_or_create(dataset=dataset_obj,
                                                           display_name=field_display_name,
                                                           es_name=field_es_name,
                                                           path=field_path)


                if not attribute_panel_obj.attribute_fields.filter(id=attribute_field_obj.id):
                    attribute_panel_obj.attribute_fields.add(attribute_field_obj)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('json_file_path', nargs='+', type=str)

    def handle(self, *args, **options):
        json_file_path = options['json_file_path'][0]

        # Load JSON file
        try:
            file_data = open(json_file_path, 'r').read()
        except IOError as e:
            raise IOError('Could not open %s' %(json_file_path))

        # Validate JSON file data
        try:
            data = json.loads(file_data)
        except ValueError as e:
            raise IOError('JSON file: %s is not a valid JSON file.\n%s' %(json_file_path, e))


        validate_and_import_data(data, import_data=False)
