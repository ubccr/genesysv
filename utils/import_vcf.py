import argparse
from collections import deque, Counter
from datetime import datetime
import elasticsearch
from elasticsearch import helpers
import hashlib
import json
import math
import os
from pprint import pprint
import re
import requests
import statistics
import sys
import time
from tqdm import tqdm
from utils import *

# Global Variables

GLOBAL_NO_VARIANTS_PROCESSED = 0
GLOBAL_NO_VARIANTS_CREATED = 0
GLOBAL_NO_VARIANTS_UPDATED = 0
GLOBAL_NO_VARIANTS_FAILED = 0
GLOBAL_NO_VARIANTS_SKIPPED = 0


#@profile
def set_data(es, index_name, type_name, vcf_filename, vcf_mapping, vcf_label, **kwargs):

    is_bulk = kwargs.get('is_bulk')
    update = kwargs.get('update')

    global GLOBAL_NO_VARIANTS_PROCESSED
    global GLOBAL_NO_VARIANTS_CREATED
    global GLOBAL_NO_VARIANTS_UPDATED
    global GLOBAL_NO_VARIANTS_FAILED
    global GLOBAL_NO_VARIANTS_SKIPPED


    format_fields = vcf_mapping.get('FORMAT_FIELDS').get('nested_fields')
    fixed_fields = vcf_mapping.get('FIXED_FIELDS')
    info_fields = vcf_mapping.get('INFO_FIELDS')

    int_format_fields = set([key for key in format_fields.keys() if format_fields[key].get('es_field_datatype') == 'integer'])
    float_format_fields = set([key for key in format_fields.keys() if format_fields[key].get('es_field_datatype') == 'float'])

    null_fields = [(key, info_fields[key].get('null_value')) for key in info_fields.keys() if 'null_value' in info_fields[key]]
    overwrite_fields = [(key, info_fields[key].get('overwrites')) for key in info_fields.keys() if 'overwrites' in info_fields[key]]
    exist_only_fields = set([key for key in info_fields.keys() if 'is_exists_only' in info_fields[key]])
    parse_with_fields = {info_fields[key].get('parse_with'): key  for key in info_fields.keys() if 'parse_with' in info_fields[key]}

    no_lines = estimate_no_variants_in_file(vcf_filename, 200000)
    # no_lines = 2000
    time_now = datetime.now()
    print('Importing an estimated %d variants into Elasticsearch' %(no_lines))
    header_found = False
    exception_vcf_line_io_mode = 'w'
    exception_filename = 'import_exceptions_for_%s' %(os.path.basename(vcf_filename))
    exception_divider = '-'*120+'\n\n'
    # with open(vcf_filename, 'r') as fp:
    fp = get_file_handle(vcf_filename)
    # for line in tqdm(fp, total=no_lines):
    for line in tqdm(fp, total=no_lines):
        line = line.strip()

        # if GLOBAL_NO_VARIANTS_PROCESSED > no_lines:
        #     break

        if "Consequence annotations from Ensembl VEP. Format:" in line and 'CSQ' in line:
            _, CSQ_fields = line.strip().split('Consequence annotations from Ensembl VEP. Format:')
            CSQ_fields = CSQ_fields[:-1].strip()
            CSQ_fields = CSQ_fields.split('|')

        if "Functional annotations:" in line and 'ANN' in line:
            _, ANN_fields = line.strip().split('Functional annotations:')
            str_ANN_fields = re.match(r"##INFO=<?(.+)>", line).groups()[0].strip().split(':')[1].replace('\'','').replace('\"', '').strip()
            ANN_fields = [ele.strip() for ele in str_ANN_fields.split('|')]

        if "Predicted loss of function effects for this variant. Format:" in line and 'LOF' in line:
            _, LOF_fields = line.strip().split('Predicted loss of function effects for this variant. Format:')
            str_LOF_fields = re.match(r"##INFO=<?(.+)>", line).groups()[0].strip().split(':')[1].replace('\'','').replace('\"', '').strip()
            LOF_fields = [ele.strip() for ele in str_LOF_fields.split('|')]

        if "Predicted nonsense mediated decay effects for this variant. Format:" in line and 'NMD' in line:
            _, NMD_fields = line.strip().split('Predicted nonsense mediated decay effects for this variant. Format:')
            str_NMD_fields = re.match(r"##INFO=<?(.+)>", line).groups()[0].strip().split(':')[1].replace('\'','').replace('\"', '').strip()
            NMD_fields = [ele.strip() for ele in str_NMD_fields.split('|')]

        if line.startswith('##'):
            continue

        if not header_found:
            if line.startswith('#CHROM'):
                line = line[1:]
                header = line.split('\t')
                sample_start = header.index('FORMAT') + 1
                samples = header[sample_start:]
                header_found = True
                continue

        data = dict(zip(header, line.split('\t')))
        info = data['INFO'].split(';')

        info_dict = parse_info_fields(info, FIELDS_TO_SKIP)

        content = {}

        try:
            CHROM = data['CHROM']
            POS = int(data['POS'])
            REF = data['REF']
            ALT = data['ALT']
            ID = data['ID']

            content['CHROM'] = CHROM
            content['POS'] = POS
            content['REF'] = REF
            content['ALT'] = ALT
            if ID != '.':
                content['ID'] = data['ID']

            if vcf_label != 'None':
                content['AC_case'] = 0
                content['AF_case'] = 0
                content['AN_case'] = 0
                content['AC_control'] = 0
                content['AF_control'] = 0
                content['AN_control'] = 0

            es_id = get_es_id(CHROM, POS, REF, ALT, index_name, type_name)

            fields_to_update = None
            if update:
                es_id_exists = es.exists(index=index_name, doc_type=type_name, id=es_id)
                if es_id_exists:
                    fields_to_update = es.get(index=index_name, doc_type=type_name, id=es_id, _source_include=RUN_DEPENDENT_FIELDS)['_source']

            ### Samples
            sample_array = deque()
            FORMAT = data['FORMAT']
            format_fields_for_current_line = FORMAT.split(':')
            gt_location = format_fields_for_current_line.index('GT')
            for sample in samples:
                # pass
                sample_content = {}
                sample_values = data.get(sample)
                sample_values = sample_values.split(':')

                if sample_values[gt_location] in ['./.']: #, '0/0', '0|0']:
                    continue

                sample_content['sample_ID'] = sample

                for idx, key_format_field in enumerate(format_fields_for_current_line):
                    key_format_field_sample = 'sample_%s' %(key_format_field)
                    key_value = sample_values[idx]
                    if key_format_field in int_format_fields:
                        if ',' in key_value:
                            sample_content[key_format_field_sample] = [int(s_val) for s_val in key_value.split(',')]
                        else:
                            if key_value not in ['.']:
                                sample_content[key_format_field_sample] = int(key_value)

                    elif key_format_field in float_format_fields:
                        if ',' in key_value:
                            sample_content[key_format_field_sample] = [float(s_val) for s_val in key_value.split(',') if not math.isnan(float(s_val))]
                        else:
                            if key_value not in ['.'] and not math.isnan(float(key_value)):
                                sample_content[key_format_field_sample] = float(key_value)
                    else:
                        if key_value not in ['.']:
                            sample_content[key_format_field_sample] = key_value



                if not vcf_label == 'None':
                    sample_content['sample_label'] = vcf_label
                sample_array.appendleft(sample_content)

            if fields_to_update:
                GLOBAL_NO_VARIANTS_UPDATED += 1
                GLOBAL_NO_VARIANTS_PROCESSED += 1

                fields_to_update['sample'].extend(sample_array)

                if vcf_label != 'None':
                    AC_label = 'AC_%s' %(vcf_label)
                    AF_label = 'AF_%s' %(vcf_label)
                    AN_label = 'AN_%s' %(vcf_label)
                    fields_to_update[AC_label] = int(info_dict.get('AC'))
                    fields_to_update[AF_label] = float(info_dict.get('AF'))
                    fields_to_update[AN_label] = int(info_dict.get('AN'))
                    fields_to_update['FILTER'].extend([{'FILTER_label': vcf_label, 'FILTER_value': data['FILTER']}])
                    if not math.isnan(float(data['QUAL'])):
                        fields_to_update['QUAL'].extend([{'QUAL_label': vcf_label, 'QUAL_value': float(data['QUAL'])}])
                    for field in RUN_DEPENDENT_INFO_FIELDS:
                        if not info_dict.get(field):
                            continue
                        if info_dict[field] == 'nan':
                            continue
                        label_field_name = "%s_label" %(field)
                        value_field_name = "%s_value" %(field)
                        es_field_datatype =  info_fields[field]['nested_fields'][value_field_name]['es_field_datatype']
                        if not fields_to_update.get(field):
                            fields_to_update[field] = []
                        if es_field_datatype == 'integer':
                            fields_to_update[field].extend([{label_field_name: vcf_label, value_field_name: int(info_dict[field])}])
                        elif es_field_datatype == 'float' and not math.isnan(float(info_dict[field])):
                            fields_to_update[field].extend([{label_field_name: vcf_label, value_field_name: float(info_dict[field])}])
                        else:
                            fields_to_update[field].extend([{label_field_name: vcf_label, value_field_name: info_dict[field]}])

                else:
                    fields_to_update['FILTER'].extend([{'FILTER_value': data['FILTER']}])
                    if not math.isnan(float(data['QUAL'])):
                        fields_to_update['QUAL'].extend([{'QUAL_value': float(data['QUAL'])}])
                    for field in RUN_DEPENDENT_INFO_FIELDS:
                        if not info_dict.get(field):
                            continue
                        if info_dict[field] == 'nan':
                            continue
                        value_field_name = "%s_value" %(field)
                        es_field_datatype =  info_fields[field]['nested_fields'][value_field_name]['es_field_datatype']
                        if not fields_to_update.get(field):
                            fields_to_update[field] = []
                        if es_field_datatype == 'integer':
                            fields_to_update[field].extend([{value_field_name: int(info_dict[field])}])
                        elif es_field_datatype == 'float' and not math.isnan(float(info_dict[field])):
                            fields_to_update[field].extend([{value_field_name: float(info_dict[field])}])
                        else:
                            fields_to_update[field].extend([{value_field_name: info_dict[field]}])

                if is_bulk:
                    action = {
                        "_op_type": 'update',
                        "_index": index_name,
                        "_type": type_name,
                        "_id": es_id,
                        "doc": fields_to_update
                    }

                    yield action
                else:
                    yield fields_to_update

                continue

            if sample_array:
                content['sample'] = list(sample_array)
            else:
                GLOBAL_NO_VARIANTS_SKIPPED += 1
                continue

            if ALT == '.':
                content['VariantType'] = 'INDEL'
            elif len(ALT) == 1 and len(REF) == 1 and ALT != '.' and REF != '.':
                content['VariantType'] = 'SNV'
            else:
                content['VariantType'] = 'INDEL'

            content['Variant'] = '%s-%d-%s-%s' %(CHROM, POS, REF[:10], ALT[:10])

            if vcf_label != 'None':
                AC_label = 'AC_%s' %(vcf_label)
                AF_label = 'AF_%s' %(vcf_label)
                AN_label = 'AN_%s' %(vcf_label)
                info_dict[AC_label] = info_dict.pop('AC')
                info_dict[AF_label] = info_dict.pop('AF')
                info_dict[AN_label] = info_dict.pop('AN')
                content['FILTER'] = [{'FILTER_label': vcf_label, 'FILTER_value': data['FILTER']}]
                if data['QUAL'] != '.' and not math.isnan(float(data['QUAL'])):
                    content['QUAL'] = [{'QUAL_label': vcf_label, 'QUAL_value': float(data['QUAL'])}]
                for field in RUN_DEPENDENT_INFO_FIELDS:
                    if not info_dict.get(field):
                        continue
                    if info_dict[field] == 'nan':
                        continue
                    label_field_name = "%s_label" %(field)
                    value_field_name = "%s_value" %(field)
                    es_field_datatype =  info_fields[field]['nested_fields'][value_field_name]['es_field_datatype']
                    if es_field_datatype == 'integer':
                        content[field] = [{label_field_name: vcf_label, value_field_name: int(info_dict[field])}]
                    elif es_field_datatype == 'float' and not math.isnan(float(info_dict[field])):
                        content[field] = [{label_field_name: vcf_label, value_field_name: float(info_dict[field])}]
                    else:
                        content[field] = [{label_field_name: vcf_label, value_field_name: info_dict[field]}]
            else:
                content['FILTER'] = [{'FILTER_value': data['FILTER']}]
                if not math.isnan(float(data['QUAL'])):
                    content['QUAL'] = [{'QUAL_value': float(data['QUAL'])}]
                for field in RUN_DEPENDENT_INFO_FIELDS:
                    if not info_dict.get(field):
                        continue
                    if info_dict[field] == 'nan':
                        continue
                    value_field_name = "%s_value" %(field)
                    es_field_datatype =  info_fields[field]['nested_fields'][value_field_name]['es_field_datatype']
                    if es_field_datatype == 'integer':
                        content[field] = [{value_field_name: int(info_dict[field])}]
                    elif es_field_datatype == 'float' and not math.isnan(float(info_dict[field])):
                        content[field] = [{value_field_name: float(info_dict[field])}]
                    else:
                        content[field] = [{value_field_name: info_dict[field]}]

            for key, val in null_fields:
                content[key] = val

            for info_key in info_fields.keys():

                if info_key.startswith('CSQ'):
                    continue

                if info_key.startswith('ANN'):
                    continue

                if info_key.startswith('LOF'):
                    continue

                if info_key.startswith('NMD'):
                    continue

                if info_key in FIELDS_TO_SKIP:
                    continue

                if info_fields[info_key].get('is_nested_label_field'):
                    continue

                if not info_dict.get(info_key):
                    continue

                es_field_name = info_fields[info_key].get('es_field_name', '')
                es_field_datatype = info_fields[info_key].get('es_field_datatype', '')

                if info_key in exist_only_fields and es_field_datatype == 'boolean':
                    content[es_field_name] = True
                    continue

                val = info_dict.get(info_key)
                if val == 'nan':
                    continue

                if es_field_datatype == 'integer':
                    if ',' in val:
                        val = [int(ele) for ele in val.split(',')]
                    else:
                        val = int(val)
                    content[es_field_name] = val
                    continue
                elif es_field_datatype == 'float':
                    if ',' in val:
                        val = [float(ele) for ele in val.split(',') if not math.isnan(float(ele))]
                    else:
                        val = float(val)
                        if not math.isnan(val):
                            content[es_field_name] = val
                    continue
                elif es_field_datatype in ['keyword', 'text'] :
                    if info_fields[info_key].get('value_mapping'):
                        value_mapping = info_fields[info_key].get('value_mapping')
                        val = value_mapping.get(val, val)

                    if info_fields[info_key].get('parse_function'):
                        parse_function = eval(info_fields[info_key].get('parse_function'))
                        val = parse_function(val)
                        content[es_field_name] = val
                        continue
                    else:
                        content[es_field_name] = val
                        continue

                ### deal with nested fields
                if info_fields[info_key].get('shares_nested_path'):
                    # print(info_key)
                    shares_nested_path = info_fields[info_key].get('shares_nested_path')
                    es_field_name = info_fields[shares_nested_path].get('es_nested_path')
                    parse_function = eval(info_fields[info_key].get('parse_function'))
                    # print(es_field_name, val, parse_function)
                    val = {info_key: val}

                    if parse_with_fields.get(info_key):
                        parse_with_field_name = parse_with_fields.get(info_key)
                        val.update({parse_with_field_name: info_dict.get(parse_with_field_name)})
                    val = parse_function(val)
                    # print(es_field_name, val)
                    if es_field_name in content:
                        content[es_field_name].extend(val)
                        continue
                    else:
                        content[es_field_name] = val
                        continue


                clinvar_input_dict = {}
                if info_fields[info_key].get('es_nested_path'):

                    ## special case for clinvar
                    if info_key == 'CLNDBN' and val != '.':

                        clinvar_input_dict = {'CLNDBN' : info_dict['CLNDBN']}
                        if info_dict.get('CLINSIG'):
                            clinvar_input_dict['CLINSIG'] = info_dict['CLINSIG']

                        if info_dict.get('CLNACC'):
                            clinvar_input_dict['CLNACC'] = info_dict['CLNACC']

                        if info_dict.get('CLNDSDB'):
                            clinvar_input_dict['CLNDSDB'] = info_dict['CLNDSDB']

                        if info_dict.get('CLNDSDBID'):
                            clinvar_input_dict['CLNDSDBID'] = info_dict['CLNDSDBID']

                        clinvar_output_dict = clinvar_parser(clinvar_input_dict)
                        # pprint(clinvar_output_dict)
                        content['clinvar'] = clinvar_output_dict
                        continue
                    elif info_key in ['CLNACC', 'CLINSIG', 'CLNDSDB', 'CLNDSDBID']:
                        continue

                    parse_function = eval(info_fields[info_key].get('parse_function'))
                    es_field_name = info_fields[info_key].get('es_nested_path')
                    val = parse_function(val)
                    if es_field_name in content:
                        content[es_field_name].extend(val)
                        continue
                    else:
                        content[es_field_name] = val
                        continue

            if info_dict.get('CSQ'):
                CSQ_info = CSQ_parser(CSQ_fields, info_dict.get('CSQ'))
                info_dict.pop('CSQ')
                content.update(CSQ_info)

            if info_dict.get('ANN'):
                ANN_info = ANN_parser(ANN_fields, info_dict.get('ANN'))
                info_dict.pop('ANN')
                content.update(ANN_info)

            if info_dict.get('LOF'):
                LOF_info = LOF_parser(LOF_fields, info_dict.get('LOF'))
                info_dict.pop('LOF')
                content.update(LOF_info)


            if info_dict.get('NMD'):
                NMD_info = NMD_parser(NMD_fields, info_dict.get('NMD'))
                info_dict.pop('NMD')
                content.update(NMD_info)

            for overwrite_key, orig_key in overwrite_fields:
                es_overwrite_key = info_fields[overwrite_key].get('es_field_name')
                es_orig_key = info_fields[orig_key].get('es_field_name')
                if es_overwrite_key in content:
                    content[es_orig_key] = content[es_overwrite_key]

            if content.get('refGene'):
                content['refGene'] = prune_array('refGene_symbol', content['refGene'])
            if content.get('ensGene'):
                content['ensGene'] = prune_array('ensGene_gene_id', content['ensGene'])

            GLOBAL_NO_VARIANTS_CREATED += 1
            GLOBAL_NO_VARIANTS_PROCESSED += 1

            if is_bulk:
                action = {
                    "_op_type": 'index',
                    "_index": index_name,
                    "_type": type_name,
                    "_source": content,
                    "_id": es_id,
                }
                yield action
            else:
                yield content

        except Exception as e:
            error_msg = 'Error on line %s %s %s' %(sys.exc_info()[-1].tb_lineno, type(e).__name__,e)
            with open(exception_filename, exception_vcf_line_io_mode) as fp:
                fp.write(error_msg+'\n')
                fp.write(line+'\n')
                fp.write(exception_divider)

            if exception_vcf_line_io_mode == 'w':
                exception_vcf_line_io_mode = 'a'

            GLOBAL_NO_VARIANTS_FAILED += 1
    fp.close()

def main():
    global GLOBAL_NO_VARIANTS_PROCESSED
    global GLOBAL_NO_VARIANTS_CREATED
    global GLOBAL_NO_VARIANTS_UPDATED
    global GLOBAL_NO_VARIANTS_FAILED
    global GLOBAL_NO_VARIANTS_SKIPPED

    start_time = datetime.now()

    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required named arguments')
    required.add_argument("--hostname", help="Elasticsearch hostname", required=True)
    required.add_argument("--port", type=int, help="Elasticsearch port", required=True)
    required.add_argument("--index", help="Elasticsearch index name", required=True)
    required.add_argument("--type", help="Elasticsearch doc type name", required=True)
    required.add_argument("--label", help="Cohort labels, e.g., \"control, case\" or \"None\"", required=True)
    required.add_argument("--update", help="Initial Import, e.g., \"True\" or \"False\"", required=True)
    required.add_argument("--vcf", help="VCF file to import", required=True)
    required.add_argument("--mapping", help="VCF mapping", required=True)
    args = parser.parse_args()

    if not os.path.exists(args.vcf):
        raise IOError("VCF file does not exist at location: %s" %(args.vcf))

    if not os.path.exists(args.mapping):
        raise IOError("VCF information file does not exist at location: %s" %(args.mapping))

    # --hostname 199.109.192.65
    # --port 9200
    # --index sim
    # --type wes
    # --label None
    # --vcf 20170419_SIM_WES_CASE.hg19_multianno.vcf
    # --mapping inspect_output_for_sim_wes.txt

    vcf_label = args.label
    vcf_filename = args.vcf
    vcf_mapping = json.load(open(args.mapping, 'r'))
    update = args.update
    if update == 'True':
        update = True
    elif update == 'False':
        update = False


    es = elasticsearch.Elasticsearch(host=args.hostname, port=args.port)
    index_name = args.index
    type_name = args.type
    es.cluster.health(wait_for_status='yellow')
    es.indices.put_settings(index=index_name, body={"refresh_interval": "-1"})

    # for line_count, data in enumerate(set_data(es, index_name,
    #                     type_name,
    #                     vcf_filename,
    #                     vcf_mapping,
    #                     vcf_label,
    #                     is_bulk=True,
    #                     update=update)):
    #     pass
    #     # pprint(data)
    # #     # es.index(index=index_name, doc_type=type_name, body=data)


    no_variants_processed, errors = helpers.bulk(es, set_data(es, index_name,
                                                type_name,
                                                vcf_filename,
                                                vcf_mapping,
                                                vcf_label,
                                                is_bulk=True,
                                                update=update),
                                        chunk_size=500,
                                        # max_chunk_bytes=5.12e+8,
                                        request_timeout=600,
                                        stats_only=True)


    # for success, info in helpers.parallel_bulk(es, set_data(es, index_name,
    #                                             type_name,
    #                                             vcf_filename,
    #                                             vcf_mapping,
    #                                             vcf_label,
    #                                             is_bulk=True,
    #                                             update=update),
    #                                         thread_count=1,
    #                                     chunk_size=10000,
    #                                     # max_chunk_bytes=5.12e+8,
    #                                     request_timeout=120
    #                                     # stats_only=True
    #                                     ):
    #     if not success: print('Doc failed', info)

    vcf_import_end_time = datetime.now()
    # update refresh interval
    es.indices.put_settings(index=index_name, body={"refresh_interval": "1s"})

    # if update:
    # print('\nIndexing %d variants in Elasticsearch' %(GLOBAL_NO_VARIANTS_PROCESSED))
    # previous_count = current_count = es.count(index_name, doc_type=type_name)['count']

    # pbar = tqdm(total=GLOBAL_NO_VARIANTS_PROCESSED)
    # while current_count < GLOBAL_NO_VARIANTS_PROCESSED:
    #     current_count = int(es.count(index_name, doc_type=type_name)['count'])
    #     difference = current_count-previous_count
    #     previous_count = current_count
    #     pbar.update(difference)
    #     time.sleep(1)
    # pbar.close()



    end_time = datetime.now()
    sys.stdout.flush()
    print('\nVCF import started at %s' %(start_time))
    print('VCF import ended at %s' %(vcf_import_end_time))
    print('VCF importing took %s' %(vcf_import_end_time-start_time))
    # if update:
    #     print('Elasticsearch indexing took %s' %(end_time-vcf_import_end_time))
    print('Importing and indexing VCF took %s' %(end_time-start_time))
    print("Number of variants processed:", GLOBAL_NO_VARIANTS_PROCESSED)
    print("Number of variants created:", GLOBAL_NO_VARIANTS_CREATED)
    print("Number of variants updated:", GLOBAL_NO_VARIANTS_UPDATED)
    print("Number of variants failed indexing:", GLOBAL_NO_VARIANTS_FAILED)
    print("Number of variants skipped:", GLOBAL_NO_VARIANTS_SKIPPED)




if __name__ == "__main__":
    main()





