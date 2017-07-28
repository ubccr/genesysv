import argparse
from tqdm import tqdm
from utils import *
from pprint import pprint
import json
import sys
import copy
import os

### Global STATIC Variables
HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'

DONE = OKGREEN + '[DONE]' + ENDC
OK = OKGREEN + '[OK]' + ENDC
FAILED = FAIL + '[FAILED]' + ENDC
MISSING = FAIL + '[MISSING]' + ENDC
ONE = OKGREEN + '[1]' + ENDC
TWO = OKGREEN + '[2]' + ENDC
WARNING = WARNING + '[WARNING]' + ENDC
ERROR = FAIL + 'ERROR:' + ENDC


def main():
    """
    This script inspects a VCF file for all available INFO and FORMAT fields. It then checks those fields against
    a list of recognized fields. Unrecognized fields are marked. Next, it parses the --n xxx number of lines to
    check if the fields have any data and also determine the most likely Elasticsearch data type for unrecognized fields.

    """
    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required named arguments')
    required.add_argument("--index", help="Elasticsearch index name", required=True)
    required.add_argument("--type", help="Elasticsearch doc type name", required=True)
    required.add_argument("--vcf", help="VCF file to import", required=True)
    required.add_argument("--labels", help="Cohort labels separated by commas, e.g., \"control, case\" or \"None\" for no label", required=True)
    args = parser.parse_args()


    no_lines = 200000
    vcf_filename = args.vcf

    default_vcf_mapping = json.load(open('default_vcf_mappings.json', 'r'))
    recognized_info_fields = list(default_vcf_mapping['INFO_FIELDS'])
    recognized_format_fields = list(default_vcf_mapping['FORMAT_FIELDS']['nested_fields'])

    # Parse info fields
    msg = '\nParsing VCF file for available INFO fields'.ljust(80,'.')
    print(msg, end=" ")

    info_fields = parse_fields('INFO', vcf_filename)
    print(OK)

    msg = '%d INFO fields found in VCF file' %(len(list(info_fields)))
    msg = msg.ljust(80,'.')
    print(msg, end="")
    print(OK)

    unrecognized_info_fields = sorted(list(set(info_fields.keys()) - set(recognized_info_fields)))
    if unrecognized_info_fields:
        print('The following INFO fields were not recognized: \n%s' %('\n'.join(unrecognized_info_fields)))
        msg = 'This script will try to determine their data type.'.ljust(80,'.')
        print(msg, end="")
        print(WARNING)
    else:
        msg = 'All parsed INFO fields are recognized.'.ljust(80,'.')
        print(msg, end="")
        print(OK)


    # Parse format (sample) fields
    msg = '\nParsing VCF file for available FORMAT (sample) fields'.ljust(80,'.')
    print(msg, end=" ")

    format_fields = parse_fields('FORMAT', vcf_filename)
    print(OK)

    msg = '%d FORMAT (sample) fields found in VCF file' %(len(list(format_fields)))
    msg = msg.ljust(80,'.')
    print(msg, end="")
    print(OK)


    unrecognized_format_fields = sorted(list(set(format_fields.keys()) - set(recognized_format_fields)))
    if unrecognized_format_fields:
        print('The following FORMAT fields were not recognized: \n%s' %('\n'.join(unrecognized_format_fields)))
        msg = 'This script will try to determine their data type.'.ljust(80,'.')
        print(msg, end="")
        print(WARNING)
    else:
        msg = 'All parsed FORMAT fields are recognized.'.ljust(80,'.')
        print(msg, end="")
        print(OK)


    # Inspect variants for empty info fields and determine data type for unrecognized info fields
    line_no = 0
    unrecognized_info_field_type = {}
    unrecognized_format_field_type = {}
    info_field_with_data = deque()

    with open(vcf_filename, 'r') as fp:
        for line in tqdm(fp, total=no_lines):
            if line.startswith('##'):
                continue

            if line.startswith('#CHROM'):
                line = line[1:]
                header = line.strip().split('\t')
                format_column_idx = header.index('FORMAT')
                continue

            if line_no > no_lines:
                break

            data = dict(zip(header, line.strip().split('\t')))
            info =  data['INFO']

            for ele in info.split(';'):

                if '=' not in ele:
                    # maybe a exist only field?
                    if ele in unrecognized_info_fields and not unrecognized_info_field_type.get(ele):
                        unrecognized_info_field_type[ele] = {
                            "es_field_datatype": "boolean",
                            "es_field_name": ele,
                            "is_exists_only": True,
                            "is_parsed" : True
                        }
                        info_field_with_data.append(ele)
                    elif ele not in info_field_with_data and default_vcf_mapping['INFO_FIELDS'].get(ele) and default_vcf_mapping['INFO_FIELDS'][ele].get('is_exists_only'):
                        info_field_with_data.append(ele)

                    continue

                key, value = ele.split('=')

                if value == '.':
                    continue

                if key not in info_field_with_data:
                    info_field_with_data.append(key)

                # if key == 'VARB':
                #     print(key, value)

                if key in unrecognized_info_fields and not unrecognized_info_field_type.get(key):
                    value_type = determine_es_datatype(value)
                    unrecognized_info_field_type[key] = {
                            "es_field_datatype": value_type,
                            "es_field_name": key.replace('.','_'),
                            "is_parsed" : True
                        }

                if unrecognized_info_field_type.get(key) and unrecognized_info_field_type[key].get('es_field_datatype') == 'integer':
                    value_type = determine_es_datatype(value)

                    if value_type == 'float':
                        unrecognized_info_field_type[key] = {
                            "es_field_datatype": value_type,
                            "es_field_name": key.replace('.','_'),
                            "is_parsed" : True
                        }


            if unrecognized_format_fields:
                format_fields_in_line = data['FORMAT']
                samples = line.strip().split('\t')[format_column_idx+1:]
                for sample in samples:
                    sample_dict = dict(zip(format_fields_in_line.split(':'), sample.split(':')))
                    for sample_key, sample_val in sample_dict.items():
                        if sample_key not in unrecognized_format_fields:
                            continue
                        if sample_val == '.':
                            continue
                        for s_val in sample_val.split(','):
                            if sample_key in unrecognized_format_fields and not unrecognized_format_field_type.get(sample_key):
                                s_val_type = determine_es_datatype(s_val)
                                unrecognized_format_field_type[sample_key] = {
                                            "es_field_datatype": s_val_type,
                                            "es_field_name": sample_key.replace('.','_'),
                                        }

                            if unrecognized_format_field_type.get(sample_key) and unrecognized_format_field_type[sample_key].get('es_field_datatype') == 'integer':
                                s_val_type = determine_es_datatype(s_val)
                                if s_val_type == 'float':
                                    unrecognized_format_field_type[sample_key] = {
                                            "es_field_datatype": s_val_type,
                                            "es_field_name": sample_key.replace('.','_'),
                                        }

            line_no += 1

    msg = '%d of %d INFO fields have data' %(len(info_field_with_data), len(recognized_info_fields))
    msg = msg.ljust(80,'.')
    print(msg, end="")
    if len(info_field_with_data) == len(recognized_info_fields):
        print(OK)
    elif len(info_field_with_data) > len(recognized_info_fields):
        print(ERROR)
        sys.exit(-1)
    else:
        print(WARNING)


    info_fields_to_skip = list(set(recognized_info_fields)-set(info_field_with_data))

    vcf_fields = {}
    vcf_fields['INFO_FIELDS'] = {}

    for field in info_fields_to_skip:
        if default_vcf_mapping['INFO_FIELDS'].get(field):
            vcf_fields['INFO_FIELDS'][field] = default_vcf_mapping['INFO_FIELDS'][field]
            vcf_fields['INFO_FIELDS'][field].update({'is_parsed': False})

    for field in info_field_with_data:
        if default_vcf_mapping['INFO_FIELDS'].get(field):
            info = default_vcf_mapping['INFO_FIELDS'][field]
            if info.get('is_non_nested_label_field') and args.labels != "None":
                es_field_datatype = default_vcf_mapping['INFO_FIELDS'][field]['es_field_datatype']
                for label in args.labels.split(','):
                    field_name_with_label = "%s___label___%s" %(field, label.strip())
                    es_field_name = field_name_with_label
                    tmp_dict = { 'es_field_name': es_field_name,
                                 'es_field_datatype': es_field_datatype,
                                 'is_parsed': True
                            }
                    vcf_fields['INFO_FIELDS'][field_name_with_label] = tmp_dict
            elif info.get('is_nested_label_field'):
                    # pprint(default_vcf_mapping['INFO_FIELDS'][field])
                    vcf_fields['INFO_FIELDS'][field] = default_vcf_mapping['INFO_FIELDS'][field]
                    vcf_fields['INFO_FIELDS'][field].update({'is_parsed': True})
            else:
                vcf_fields['INFO_FIELDS'][field] = default_vcf_mapping['INFO_FIELDS'][field]
                vcf_fields['INFO_FIELDS'][field].update({'is_parsed': True})

    ### extract FIXED FIELDS
    FIXED_FIELDS = {}
    for key in default_vcf_mapping['FIXED_FIELDS'].keys():
        if default_vcf_mapping['FIXED_FIELDS'][key].get('label'):
            if args.labels == 'None':
                FIXED_FIELDS[key] = default_vcf_mapping['FIXED_FIELDS'][key]['no_label']
            else:
                FIXED_FIELDS[key] = default_vcf_mapping['FIXED_FIELDS'][key]['label']
        else:
            FIXED_FIELDS[key] = default_vcf_mapping['FIXED_FIELDS'][key]
        FIXED_FIELDS[key].update({'is_parsed': True})
        # print(key)



    ### Used FORMAT Fields
    vcf_format_fields = list(format_fields)
    format_fields = copy.deepcopy(default_vcf_mapping['FORMAT_FIELDS'])
    for field in recognized_format_fields:
        if field in ['sample_ID', 'sample_label']:
            continue
        if field not in vcf_format_fields:
            format_fields['nested_fields'].pop(field)



    vcf_fields['INFO_FIELDS'].update(unrecognized_info_field_type)
    vcf_fields['FIXED_FIELDS']= FIXED_FIELDS
    vcf_fields['CUSTOM_FIELDS']= default_vcf_mapping['CUSTOM_FIELDS']


    vcf_fields['FORMAT_FIELDS']= format_fields
    vcf_fields['FORMAT_FIELDS']['nested_fields'].update(unrecognized_format_field_type)

    if args.labels == 'None':
        vcf_fields['FORMAT_FIELDS']['nested_fields'].pop('sample_label')


    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.path.join(dir_path, 'es_scripts')

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    with open(os.path.join(dir_path, 'inspect_output_for_%s_%s.txt' %(args.index, args.type)), 'w') as outfile:
        json.dump(vcf_fields, outfile, sort_keys = True, indent = 2,
               ensure_ascii = True)



if __name__ == '__main__':
    main()


# python inspect_vcf.py --n 100000 --index sim --type wes --vcf 20170419_SIM_WES_CASE.hg19_multianno.vcf --labels None

