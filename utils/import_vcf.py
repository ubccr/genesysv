import argparse
import os
import json
import re
from collections import deque
from pprint import pprint
from tqdm import tqdm
from datetime import datetime
import sys
import statistics
import time
import elasticsearch
from elasticsearch import helpers
import re
from collections import Counter
import asyncio
import functools
import requests
from import_celery.tasks import post_data


GLOBAL_NO_VARIANTS_PROCESSED = 0
GLOBAL_NO_VARIANTS_CREATED = 0
GLOBAL_NO_VARIANTS_UPDATED = 0

class VCFException(Exception):
    """Raise for my specific kind of exception"""
    def __init__(self, message, *args):
        self.message = message # without this you may get DeprecationWarning
        # Special attribute you desire with your Error,
        # perhaps the value that caused the error?:
        # allow users initialize misc. arguments as any other builtin Error
        super(VCFException, self).__init__(message, *args)

def prune_array(key, input_array):
    key_count = Counter([ele[key] for ele in input_array])

    output_array = []
    for ele in input_array:
        tmp_key = ele[key]
        if key_count[tmp_key] == 1:
            output_array.append(ele)
        elif key_count[tmp_key] > 1:
            if len(ele) > 1:
                output_array.append(ele)

    return output_array

def estimate_no_variants_in_file(filename, no_lines_for_estimating):
    no_lines = 0
    size_list = deque()

    with open(filename, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue

            if no_lines_for_estimating < no_lines:
                break

            size_list.appendleft(sys.getsizeof(line))

            no_lines += 1

    filesize = os.path.getsize(filename)

    no_variants = int(filesize/statistics.median(size_list))

    return no_variants

def get_es_id_result(es, index_name, type_name, CHROM, POS, REF, ALT):
    query_body = """
        {
            "size": 10,
            "query" : {
                "bool" : {
                    "must" : [
                        { "term": { "CHROM": "%s" }},
                        { "term": { "POS": "%s" }},
                        {  "term": { "REF": "%s" }},
                        {  "term": { "ALT": "%s" }}
                    ]
                }

            }
        }
    """

    body = query_body %(CHROM, POS, REF, ALT)
    results = es.search(index=index_name, doc_type=type_name, body=body)
    total = results['hits']['total']

    if total == 1:
        es_id = results['hits']['hits'][0]['_id']
        result = results['hits']['hits'][0]['_source']
        return (es_id, 'Success', result)
    elif total > 1:
        return ('Multiple', 'Multiple Records Found', None)
    elif total == 0:
        return (None, 'No Record Found', None)


def CHROM_parser(input_string):
    return input_string.lower().replace('chr','').strip()

def gwasCatalog_parser(input_string):
    return input_string.replace('|', ' ')

def CLINSIG_parser(input_string):
    return re.split(',|\|',input_string)

def GTEx_V6_tissue_parser(input_string):
    return input_string.replace('|', ' ')

def GTEx_V6_gene_parser(input_string):
    return input_string.replace('|', ' ')

def Gene_refGene_parser(relevant_info_fields):

    pattern = r'^dist=[a-zA-Z0-9]+;dist=[a-zA-Z0-9]+$'

    Gene_refGene = relevant_info_fields['Gene.refGene']
    symbol = ' '.join(re.split('[;,]', Gene_refGene))

    tmp_content_array = []

    if relevant_info_fields.get('GeneDetail.refGene'):

        GeneDetail_refGene = convert_escaped_chars(relevant_info_fields.get('GeneDetail.refGene'))

        if re.match(pattern, GeneDetail_refGene):
            tmp_content = {}
            tmp_content['refGene_symbol'] = symbol
            tmp_content['refGene_distance_to_gene'] = GeneDetail_refGene
            tmp_content_array.append(tmp_content)

        elif ':' in GeneDetail_refGene:
            for record in GeneDetail_refGene.split(','):
                # print(record)
                tmp_content = {}
                for ele in record.split(':'):
                    if ele.startswith('N'):
                        tmp_content['refGene_refgene_id'] = ele
                    elif ele.startswith('exon'):
                        tmp_content['refGene_location'] = ele
                    elif ele.startswith('c.'):
                        tmp_content['refGene_cDNA_change'] = ele

                tmp_content['refGene_symbol'] = symbol
                # print(tmp_content)
                tmp_content_array.append(tmp_content)

    else:
        tmp_content = {}
        tmp_content['refGene_symbol'] = symbol
        tmp_content_array.append(tmp_content)

    return tmp_content_array


def Gene_ensGene_parser(relevant_info_fields):

    pattern = r'^dist=[a-zA-Z0-9]+;dist=[a-zA-Z0-9]+$' # pattern to detect dist
    Gene_ensGene = relevant_info_fields["Gene.ensGene"]

    gene_id = ' '.join(re.split('[;,]', Gene_ensGene))

    tmp_content_array = []

    if relevant_info_fields.get('GeneDetail.ensGene'):

        GeneDetail_ensGene = convert_escaped_chars(relevant_info_fields.get('GeneDetail.ensGene'))


        if re.match(pattern, GeneDetail_ensGene):
            tmp_content = {}
            tmp_content['ensGene_gene_id'] = gene_id
            tmp_content['ensGene_distance_to_gene'] = GeneDetail_ensGene
            tmp_content_array.append(tmp_content)

        elif ':' in GeneDetail_ensGene:
            for record in GeneDetail_ensGene.split(','):
                tmp_content = {}
                for ele in record.split(':'):
                    if ele.startswith('ENST'):
                        tmp_content['ensGene_transcript_id'] = ele
                    elif ele.startswith('exon'):
                        tmp_content['ensGene_location'] = ele
                    elif ele.startswith('c.'):
                        tmp_content['ensGene_cDNA_change'] = ele

                tmp_content['ensGene_gene_id'] = gene_id

                tmp_content_array.append(tmp_content)

    else:
        tmp_content = {}
        tmp_content['ensGene_gene_id'] = gene_id
        tmp_content_array.append(tmp_content)

    return tmp_content_array

def AAChange_refGene_parser(AAChange_refGene):

    tmp_content_array = []
    AAChange_refGene = AAChange_refGene.split(',')

    for ele in AAChange_refGene:
        if not ele:
            continue
        if ele.lower() == 'unknown':
            continue
        tmp_content = {}
        tmp_tmp = ele.split(':')
        if len(tmp_tmp) == 5:
            tmp_content['refGene_symbol'] = tmp_tmp[0]
            tmp_content['refGene_refgene_id'] = tmp_tmp[1]
            tmp_content['refGene_location'] = tmp_tmp[2]
            tmp_content['refGene_cDNA_change'] = tmp_tmp[3]
            tmp_content['refGene_aa_change'] = tmp_tmp[4]
        elif len(tmp_tmp) == 4:
            tmp_content['refGene_symbol'] = tmp_tmp[0]
            tmp_content['refGene_refgene_id'] = tmp_tmp[1]
            tmp_content['refGene_location'] = tmp_tmp[2]
            tmp_content['refGene_cDNA_change'] = tmp_tmp[3]
        elif len(tmp_tmp) == 3:
            tmp_content['refGene_symbol'] = tmp_tmp[0]
            tmp_content['refGene_refgene_id'] = tmp_tmp[1]
            tmp_content['refGene_location'] = tmp_tmp[2]
        else:
            print(ele)
            raise VCFException('Length of refGene is not 3, 4, or 5')
        tmp_content_array.append(tmp_content)

    return tmp_content_array

def AAChange_ensGene_parser(AAChange_ensGene):

    tmp_content_array = []
    AAChange_ensGene = AAChange_ensGene.split(',')

    for ele in AAChange_ensGene:
        if not ele:
            continue
        if ele.lower() == 'unknown':
            continue
        tmp_content = {}
        tmp_tmp = ele.split(':')
        if len(tmp_tmp) == 5:
            tmp_content['ensGene_gene_id'] = tmp_tmp[0]
            tmp_content['ensGene_transcript_id'] = tmp_tmp[1]
            tmp_content['ensGene_location'] = tmp_tmp[2]
            tmp_content['ensGene_cDNA_change'] = tmp_tmp[3]
            tmp_content['ensGene_aa_change'] = tmp_tmp[4]
        elif len(tmp_tmp) == 4:
            tmp_content['ensGene_gene_id'] = tmp_tmp[0]
            tmp_content['ensGene_transcript_id'] = tmp_tmp[1]
            tmp_content['ensGene_location'] = tmp_tmp[2]
            tmp_content['ensGene_cDNA_change'] = tmp_tmp[3]
        elif len(tmp_tmp) == 3:
            tmp_content['ensGene_gene_id'] = tmp_tmp[0]
            tmp_content['ensGene_transcript_id'] = tmp_tmp[1]
            tmp_content['ensGene_location'] = tmp_tmp[2]
        else:
            print(ele)
            raise VCFException('Length of ensGene is not 3, 4, or 5')
        tmp_content_array.append(tmp_content)

    return tmp_content_array



def convert_escaped_chars(input_string):
    input_string = input_string.replace("\\x3b", ";")
    input_string = input_string.replace("\\x2c", ",")
    input_string = input_string.replace("\\x3d", "=")

    return input_string



#@profile
def set_data(es, index_name, type_name, vcf_filename, vcf_mapping, vcf_label, **kwargs):

    is_bulk = kwargs.get('is_bulk')
    initial_import = kwargs.get('initial_import')


    global GLOBAL_NO_VARIANTS_PROCESSED
    global GLOBAL_NO_VARIANTS_CREATED
    global GLOBAL_NO_VARIANTS_UPDATED


    format_fields = vcf_mapping.get('FORMAT_FIELDS').get('nested_fields')
    fixed_fields = vcf_mapping.get('FIXED_FIELDS')
    info_fields = vcf_mapping.get('INFO_FIELDS')

    int_format_fields = set([key for key in format_fields.keys() if format_fields[key].get('es_field_datatype') == 'integer'])
    float_format_fields = set([key for key in format_fields.keys() if format_fields[key].get('es_field_datatype') == 'float'])

    null_fields = [(key, info_fields[key].get('null_value')) for key in info_fields.keys() if 'null_value' in info_fields[key]]
    overwrite_fields = [(key, info_fields[key].get('overwrites')) for key in info_fields.keys() if 'overwrites' in info_fields[key]]
    exist_only_fields = set([key for key in info_fields.keys() if 'is_exists_only' in info_fields[key]])
    parse_with_fields = {info_fields[key].get('parse_with'): key  for key in info_fields.keys() if 'parse_with' in info_fields[key]}

    fields_to_skip = set(['ALLELE_END', 'ANNOVAR_DATE', 'END'])
    fields_to_skip_when_comparing_es_data = set(['sample', 'AC_', 'AF_', 'AN_', 'FILTER', 'QUAL'])

    no_lines = estimate_no_variants_in_file(vcf_filename, 200000)
    # no_lines = 100000
    time_now = datetime.now()
    print('Importing an estimated %d variants into Elasticsearch' %(no_lines))
    header_found = False
    with open(vcf_filename, 'r') as fp:
        for line in tqdm(fp, total=no_lines):
        # for no_line, line in enumerate(fp, 1):
            line = line.strip()

            if GLOBAL_NO_VARIANTS_PROCESSED > no_lines:
                break

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



            info_dict = {}
            for ele in info:
                if ele in fields_to_skip:
                    continue
                if '=' in ele:
                    key, val = (ele.split('=')[0], ''.join(ele.split('=')[1:]))
                    if val != '.':
                        info_dict[key] = convert_escaped_chars(val)
                else:
                    info_dict[ele] = True

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

                    if sample_values[gt_location] == './.':
                        continue

                    sample_content['sample_ID'] = sample

                    for idx, key_format_field in enumerate(format_fields_for_current_line):
                        key_format_field_sample = f'sample_{key_format_field}'
                        key_value = sample_values[idx]
                        if key_format_field in int_format_fields:
                            if ',' in key_value:
                                sample_content[key_format_field_sample] = [int(s_val) for s_val in key_value.split(',')]
                            else:
                                if key_value not in ['.']:
                                    sample_content[key_format_field_sample] = int(key_value)


                        elif key_format_field in float_format_fields:
                            if ',' in key_value:
                                sample_content[key_format_field_sample] = [float(s_val) for s_val in key_value.split(',')]
                            else:
                                if key_value not in ['.']:
                                    sample_content[key_format_field_sample] = float(key_value)
                        else:
                            if key_value not in ['.']:
                                sample_content[key_format_field_sample] = key_value



                    if not vcf_label == 'None':
                        sample_content['sample_label'] = vcf_label
                    sample_array.appendleft(sample_content)

                if sample_array:
                    # pprint(sample_array)
                    content['sample'] = list(sample_array)


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
                    info_fields[AC_label] = info_dict.pop('AC')
                    info_fields[AF_label] = info_dict.pop('AF')
                    info_fields[AN_label] = info_dict.pop('AN')
                    content['FILTER'] = [{'FILTER_cohort': vcf_label, 'FILTER_status': data['FILTER']}]
                    content['QUAL'] = [{'QUAL_cohort': vcf_label, 'QUAL_score': float(data['QUAL'])}]
                else:
                    content['FILTER'] = [{'FILTER_status': data['FILTER']}]
                    content['QUAL'] = [{'QUAL_score': float(data['QUAL'])}]
                    # content['FILTER_status'] = data['FILTER']
                    # content['QUAL_score'] = float(data['QUAL'])


                for key, val in null_fields:
                    content[key] = val

                for info_key in info_fields.keys():

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
                            val = [float(ele) for ele in val.split(',')]
                        else:
                            val = float(val)
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


                    if info_fields[info_key].get('es_nested_path'):
                        parse_function = eval(info_fields[info_key].get('parse_function'))
                        es_field_name = info_fields[info_key].get('es_nested_path')
                        val = parse_function(val)
                        if es_field_name in content:
                            content[es_field_name].extend(val)
                            continue
                        else:
                            content[es_field_name] = val
                            continue




                for overwrite_key, orig_key in overwrite_fields:
                    es_overwrite_key = info_fields[overwrite_key].get('es_field_name')
                    es_orig_key = info_fields[orig_key].get('es_field_name')
                    if es_overwrite_key in content:
                        content[es_orig_key] = content[es_overwrite_key]


                content['refGene'] = prune_array('refGene_symbol', content['refGene'])
                content['ensGene'] = prune_array('ensGene_gene_id', content['ensGene'])



                if initial_import:
                    GLOBAL_NO_VARIANTS_CREATED += 1
                    GLOBAL_NO_VARIANTS_PROCESSED += 1
                    if is_bulk:
                        action = {
                            "_op_type": 'index',
                            "_index": index_name,
                            "_type": type_name,
                            "_source": content
                        }
                        yield action
                    else:
                        yield '{"index" : {}}'
                        yield json.dumps(content)
                else:

                    es_id, es_msg, data = get_es_id_result(es, index_name, type_name, content['CHROM'], content['POS'], content['REF'], content['ALT'])


                    if es_id == None:
                        GLOBAL_NO_VARIANTS_CREATED += 1
                        GLOBAL_NO_VARIANTS_PROCESSED += 1
                        if is_bulk:
                            action = {
                                "_op_type": 'index',
                                "_index": index_name,
                                "_type": type_name,
                                "_source": content
                            }

                            yield action
                        else:
                            yield content
                    elif es_id:

                        ### START verify data

                        for key in list(content):
                            if fields_to_skip_when_comparing_es_data:
                                for ele in keys_to_skip:
                                    if key.startswith(ele):
                                        continue


                            if key in ['refGene', 'ensGene']:
                                if not compare_array_dictionaries(content[key], data[key]):
                                    msg = "Error in file: %s\nKey: %s\nData from file: %s\nCurrent data: %s\n" %(full_path,
                                                                                                                  key,
                                                                                                                  data[key],
                                                                                                                  content[key])
                                    raise VCFException(msg)

                            elif key in data:
                                if data[key] != content[key]:
                                    msg = "Error in file: %s\nKey: %s\nData from file: %s\nCurrent data: %s\n" %(full_path,
                                                                                                                  key,
                                                                                                                  data[key],
                                                                                                                  content[key])
                                    raise VCFException(msg)
                        ### END verify data


                        ### Update original data
                        ### UPDATE: FILTER, QUAL, sample
                        ### ADD: AC_, AF_, AN_
                        for ele in content['FILTER']:
                            if ele not in data['FILTER']:
                                data['FILTER'].append(ele)

                        for ele in content['QUAL']:
                            if ele not in data['QUAL']:
                                data['QUAL'].append(ele)

                        for ele in content['sample']:
                            if ele not in data['sample']:
                                data['sample'].append(ele)

                        for key in list(content):
                            if key not in data:
                                data[key] = content[key]

                        content = data

                        GLOBAL_NO_VARIANTS_UPDATED += 1
                        GLOBAL_NO_VARIANTS_PROCESSED += 1
                        if is_bulk:
                            action = {
                                "_op_type": 'update',
                                "_index": index_name,
                                "_type": type_name,
                                "_id": es_id,
                                "doc": content
                            }

                            yield action
                        else:
                            yield content

                        # pprint(action)
                        # print(content['Variant'])
                        GLOBAL_NO_VARIANTS_UPDATED += 1

                    elif es_msg == 'Multiple Records Found':
                        msg = "Multiple Records Found for: CHROM: %s -- POS: %s -- REF: %s -- ALT: %s" %(content['CHROM'],
                                                                                                         content['POS'],
                                                                                                         content['REF'],
                                                                                                         content['ALT'])
                        raise VCFException(msg)
                    else:
                        raise VCFException('This should never happen')
            except Exception as e:
                print(e)
                print(line)
                continue



# def post_data(hostname, port, index, type, filename):
#     with open(filename,'rb') as payload:
#         headers = {'content-type': 'application/x-www-form-urlencoded'}
#         r = requests.post(f'http://{hostname}:{port}/{index}/{type}/_bulk?refresh=false&pretty',
#                           data=payload, verify=False, headers=headers)

#     json_data = r.json()
#     took = json_data['took']
#     errors = json_data['errors']
#     # print(took, errors)


def main():
    global GLOBAL_NO_VARIANTS_PROCESSED
    start_time = datetime.now()
    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required named arguments')
    required.add_argument("--hostname", help="Elasticsearch hostname", required=True)
    required.add_argument("--port", type=int, help="Elasticsearch port", required=True)
    required.add_argument("--index", help="Elasticsearch index name", required=True)
    required.add_argument("--type", help="Elasticsearch doc type name", required=True)
    required.add_argument("--label", help="Cohort labels, e.g., \"control, case\" or \"None\"", required=True)
    required.add_argument("--initial", help="Initial Import, e.g., \"True\" or \"False\"", required=True)
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
    initial_import = args.initial
    if initial_import == 'True':
        initial_import = True
    elif initial_import == 'False':
        initial_import = False

    es = elasticsearch.Elasticsearch(host=args.hostname, port=args.port)
    index_name = args.index
    type_name = args.type
    es.cluster.health(wait_for_status='yellow')
    es.indices.put_settings(index=index_name, body={"refresh_interval": "-1"})



    file_count = 1
    for line_count, data in enumerate(set_data(es, index_name,
                        type_name,
                        vcf_filename,
                        vcf_mapping,
                        vcf_label,
                        is_bulk=False,
                        initial_import=initial_import)):

        if line_count == 0:
            filename = f'output_{file_count}.json'
            output_file = open(filename, 'w')

        elif line_count % 20000 == 0:
            output_file.close()
            post_data.delay(args.hostname, args.port, index_name, type_name, filename)
            #
            file_count += 1
            filename = f'output_{file_count}.json'
            output_file = open(filename, 'w')


        output_file.write(f'{data}\n')
    #finally:
    output_file.close()
    post_data.delay(args.hostname, args.port, index_name, type_name, filename)
        # pprint(data)
        # es.index(index=index_name, doc_type=type_name, body=data)


    # no_variants_processed, errors = helpers.bulk(es, set_data(es, index_name,
    #                                             type_name,
    #                                             vcf_filename,
    #                                             vcf_mapping,
    #                                             vcf_label,
    #                                             is_bulk=True,
    #                                             initial_import=initial_import),
    #                                     chunk_size=10000,
    #                                     # max_chunk_bytes=5.12e+8,
    #                                     request_timeout=600,
    #                                     stats_only=True)


    # for success, info in helpers.parallel_bulk(es, set_data(es, index_name,
    #                                             type_name,
    #                                             vcf_filename,
    #                                             vcf_mapping,
    #                                             vcf_label,
    #                                             is_bulk=True,
    #                                             initial_import=initial_import),
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

    # if initial_import:
    #     print('\nIndexing %d variants in Elasticsearch' %(GLOBAL_NO_VARIANTS_PROCESSED))
    #     previous_count = current_count = es.count(index_name, doc_type=type_name)['count']

    #     pbar = tqdm(total=GLOBAL_NO_VARIANTS_PROCESSED)
    #     while current_count < GLOBAL_NO_VARIANTS_PROCESSED:
    #         current_count = int(es.count(index_name, doc_type=type_name)['count'])
    #         difference = current_count-previous_count
    #         previous_count = current_count
    #         pbar.update(difference)
    #         time.sleep(1)
    #     pbar.close()



    end_time = datetime.now()
    sys.stdout.flush()
    print('\nVCF import started at %s' %(start_time))
    print('VCF import ended at %s' %(vcf_import_end_time))
    print('VCF importing took %s' %(vcf_import_end_time-start_time))
    # if initial_import:
    #     print('Elasticsearch indexing took %s' %(end_time-vcf_import_end_time))
    print('Importing and indexing VCF took %s' %(end_time-start_time))
    print("Number of variants processed:", GLOBAL_NO_VARIANTS_PROCESSED)
    print("Number of variants created:", GLOBAL_NO_VARIANTS_CREATED)
    print("Number of variants updated:", GLOBAL_NO_VARIANTS_UPDATED)




if __name__ == "__main__":
    main()





