import argparse
import json
import os
import re
from collections import deque
from operator import itemgetter
from pprint import pprint
from urllib.parse import parse_qs

from utils import *


def generate_es_mapping(hostname,
                        port,
                        index,
                        type_name,
                        non_nested_fields_dict,
                        nested_fields_dict):

    # pprint(nested_fields_dict)
    mapping = {'properties': {}}

    for field in non_nested_fields_dict.keys():
        es_field_name = non_nested_fields_dict[field]['es_field_name']
        es_field_datatype = non_nested_fields_dict[field]['es_field_datatype']
        es_field_text_analyzer = non_nested_fields_dict[
            field].get('es_field_text_analyzer')
        es_null_value = non_nested_fields_dict[field].get('es_null_value')
        mapping['properties'][es_field_name] = {'type': es_field_datatype}
        if es_field_text_analyzer:
            mapping['properties'][es_field_name][
                'analyzer'] = es_field_text_analyzer

    for field in nested_fields_dict.keys():

        es_nested_path = nested_fields_dict[field]['es_nested_path']
        mapping['properties'][es_nested_path] = {'type': 'nested',
                                                 'properties': {}}

        nested_fields = nested_fields_dict[field]['nested_fields']

        for nested_field in nested_fields.keys():
            es_field_name = nested_fields[nested_field]['es_field_name']
            es_field_datatype = nested_fields[
                nested_field]['es_field_datatype']
            es_field_text_analyzer = nested_fields[
                nested_field].get('es_field_text_analyzer')

            tmp_nested_dict = {}
            tmp_nested_dict[es_field_name] = {'type': es_field_datatype}
            mapping['properties'][es_nested_path]['properties'][
                es_field_name] = {'type': es_field_datatype}
            if es_field_text_analyzer:
                mapping['properties'][es_nested_path]['properties'][
                    es_field_name]['analyzer'] = es_field_text_analyzer

    # write files

    index_settings = {}
    index_settings["settings"] = {
        "number_of_shards": 5,
        "number_of_replicas": 0,
        "refresh_interval": "-1"
    }

    dir_path = os.path.dirname(os.path.realpath(__file__))
    create_filename = os.path.join(
        dir_path,  'es_scripts', 'create_index_%s_and_put_mapping_%s.sh' % (index, type_name))
    delete_filename = os.path.join(
        dir_path,  'es_scripts', 'delete_index_%s.sh' % (index))

    with open(create_filename, 'w') as fp:
        # curl -XPUT 'localhost:9200/twitter?pretty' -H 'Content-Type:
        # application/json' -d'
        fp.write("curl -XPUT \'%s:%s/%s?pretty\' -H \'Content-Type: application/json\' -d\'\n" %
                 (hostname, port, index))
        json.dump(index_settings, fp, sort_keys=True, indent=2,
                  ensure_ascii=False)
        fp.write("\'\n")
        fp.write("curl -XPUT \'%s:%s/%s/_mapping/%s?pretty\' -H \'Content-Type: application/json\' -d\'\n" %
                 (hostname, port, index, type_name))
        json.dump(mapping, fp, sort_keys=True, indent=2,
                  ensure_ascii=False)
        fp.write("\'")

    with open(delete_filename, 'w') as fp:
        # curl -XPUT 'localhost:9200/twitter?pretty' -H 'Content-Type:
        # application/json' -d'
        fp.write("curl -XDELETE \'%s:%s/%s?pretty\'" % (hostname, port, index))

    return mapping


def main():
    # parser = argparse.ArgumentParser(prog='PROG', usage='%(prog)s [options]')
    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required named arguments')
    required.add_argument(
        "--hostname", help="Elasticsearch hostname", required=True)
    required.add_argument("--port", type=int,
                          help="Elasticsearch port", required=True)
    required.add_argument(
        "--index", help="Elasticsearch index name", required=True)
    required.add_argument(
        "--type", help="Elasticsearch doc type name", required=True)
    required.add_argument("--info", help="VCF Information", required=True)
    args = parser.parse_args()

    if not os.path.exists(args.info):
        raise IOError(
            "VCF information file does not exist at location: %s" % (args.info))

    vcf_mapping = json.load(open(args.info, 'r'))

    all_fields = vcf_mapping.get('FIXED_FIELDS')
    all_fields.update(vcf_mapping.get('INFO_FIELDS'))
    all_fields.update(vcf_mapping.get('CUSTOM_FIELDS'))

    non_nested_fields_dict = {}
    nested_fields_dict = {}

    for key in all_fields.keys():
        if not all_fields[key].get('is_parsed'):
            continue
        if all_fields[key].get('es_field_datatype'):
            non_nested_fields_dict[key] = all_fields[key]
        elif all_fields[key].get('es_nested_path'):
            nested_fields_dict[key] = all_fields[key]

    nested_fields_dict['sample'] = vcf_mapping.get('FORMAT_FIELDS')
    # pprint(nested_fields_dict)

    # pprint(integer_field_dict)
    mapping = generate_es_mapping(args.hostname,
                                  args.port,
                                  args.index,
                                  args.type,
                                  non_nested_fields_dict,
                                  nested_fields_dict)
    pprint(mapping)


if __name__ == "__main__":
    main()

# python prepare_elasticsearch_for_import.py --hostname 199.109.192.65
# --port 9200 --index sim --type wes --labels None --info
# inspect_output_for_sim_wes.txt
