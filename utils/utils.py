import binascii
import gzip
import hashlib
import os
import re
import statistics
import sys
from collections import Counter, defaultdict, deque


def AA_parser(input_string):
    output_array = []
    tmp_dict = {}
    AA, REF, ALT, IndelType = input_string.split('|')
    if AA and AA != '.':
        tmp_dict["AA_AA"] = AA
    if REF and REF != '.':
        tmp_dict["AA_REF"] = REF
    if ALT and ALT != '.':
        tmp_dict["AA_ALT"] = ALT
    if IndelType and IndelType != '.':
        tmp_dict["AA_IndelType"] = IndelType

    if tmp_dict:
        output_array.append(tmp_dict)

    return output_array


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


def add_prefix_to_keys_in_dictionary(annotations_array, prefix):
    output = []
    for ele in annotations_array:
        output_dict = {}
        for key, value in ele.items():
            new_key = prefix + '_' + key
            output_dict[new_key] = value
        if output_dict:
            output.append(output_dict)
    return output


def ANN_parser(fields, input_string):
    annotations_array = extract_piped_fields(fields, input_string)

    for ele in annotations_array:
        for key in ANN_FIELDS_TO_SPLIT:
            if key in ele:
                value = ele.get(key)
                k1, k2 = key.split('/')
                v1, v2 = value.split('/')
                ele[k1.strip()] = v1.strip()
                ele[k2.strip()] = v2.strip()

                ele.pop(key)

    for ele in annotations_array:
        for field in ANN_INTEGER_FIELDS:
            if ele.get(field):
                ele[field] = int(ele[field])

    for ele in annotations_array:
        for field in ANN_FIELDS_TO_SPLIT_BY_AMPERSAND:
            if ele.get(field) and '&' in ele.get(field):
                ele[field] = ele.get(field).split('&')

    output_dict = {}
    for field in ANN_NON_NESTED_FIELDS:
        field_name = "ANN_" + field
        values = [ele.get(field)
                  for ele in annotations_array if ele.get(field)]
        if not values:
            continue
        if type(values[0]) is list and values.count(values[0]) > 1 and values.count(values[0]) == len(values):
            output_dict[field_name] = values[0]
        elif type(values[0]) is not list and values.count(values[0]) > 1 and values.count(values[0]) == len(values):
            output_dict[field_name] = values[0]
        elif len(values) == 1:
            output_dict[field_name] = values[0]
        else:
            print('ERROR: ', field, values)

    nested_dicts = []
    for ele in annotations_array:
        nested_dicts.append({key: ele.get(key)
                             for key in ANN_MODIFIED_NESTED_FIELDS if ele.get(key)})

    output_dict['ANN_nested'] = add_prefix_to_keys_in_dictionary(
        nested_dicts, 'ANN_nested')
    return output_dict


def CHROM_parser(input_string):
    return input_string.lower().replace('chr', '').strip()


def clinvar_parser(input_dict):
    output = []
    CLINSIG_split = re.split(',+|\|+', input_dict['CLINSIG'])
    CLNDBN_split = re.split(',+|\|+', input_dict['CLNDBN'])
    CLNACC_split = re.split(',+|\|+', input_dict['CLNACC'])
    # if input_dict.get('CLNDSDB'):
    #     CLNDSDB_split = re.split('\|', input_dict['CLNDSDB'])
    # else:
    #     CLNDSDB_split = None

    # if input_dict.get('CLNDSDBID'):
    #     CLNDSDBID_split = re.split('\|', input_dict['CLNDSDBID'])
    # else:
    #     CLNDSDBID_split = None

    for i in range(len(CLINSIG_split)):
        CLINSIG = CLINSIG_split[i]
        CLNDBN = CLNDBN_split[i]
        CLNACC = CLNACC_split[i]

        # try:
        #     CLNDSDB = CLNDSDB_split[i]
        # except:
        #     print('ERROR: CLNDSDB')
        #     pprint(input_dict)
        #     CLNDSDB = None

        # if CLNDSDBID_split:
        #     print(CLNDSDBID_split)
        #     CLNDSDBID = CLNDSDBID_split[i]
        # else:
        #     CLNDSDBID = None

        output_dict = {
            'clinvar_CLINSIG': CLINSIG,
            'clinvar_CLNDBN': CLNDBN,
            'clinvar_CLNACC': CLNACC,
        }
        # if CLNDSDB:
        #     output_dict['clinvar_CLNDSDB'] = CLNDSDB
        # if CLNDSDBID:
        #     output_dict['clinvar_CLNDSDBID'] = CLNDSDBID

        output.append(output_dict)

    return output


def convert_escaped_chars(input_string):
    input_string = input_string.replace("\\x3b", ";")
    input_string = input_string.replace("\\x2c", ",")
    input_string = input_string.replace("\\x3d", "=")
    input_string = input_string.replace(',_', '_')

    return input_string


def cosmic70_parser(input_string):
    output_array = []
    tmp_dict = {}
    for ele in input_string.split(';'):
        key, value = ele.split('=')
        value = value.split(',')
        key = "cosmic70_%s" % (key)

        tmp_dict[key] = value

    output_array.append(tmp_dict)
    return output_array


def CSQ_parser(fields, input_string):

    # treat the following fields as non-nested and shared between comma
    # separated annotations.

    CSQ_FIELDS_TO_SPLIT_BY_AMPERSAND = sorted(list(set(
        fields) - set(CSQ_FLOAT_FIELDS + CSQ_INTEGER_FIELDS + CSQ_FIELDS_TO_SKIP_SPLITTING_BY_AMPERSAND)))

    annotations_array = extract_piped_fields(fields, input_string)

    for ele in annotations_array:
        for field in CSQ_FLOAT_FIELDS:
            if ele.get(field):
                if '&' in ele[field]:
                    ele[field] = float(ele[field].split('&')[0])
                else:
                    ele[field] = float(ele[field])

        for field in CSQ_INTEGER_FIELDS:
            if ele.get(field):
                if '&' in ele[field]:
                    ele[field] = int(ele[field].split('&')[0])
                else:
                    ele[field] = int(ele[field])

        for field in CSQ_FIELDS_TO_SPLIT_BY_AMPERSAND:
            if ele.get(field) and '&' in ele.get(field):
                ele[field] = ele.get(field).split('&')

        for field in CSQ_FIELDS_TO_SPLIT_INTO_START_END_BY_DASH:
            if ele.get(field):
                value = ele.get(field)
                if '-' in value:
                    start, end = value.split('-')
                    int_start = is_int(start)
                    int_end = is_int(end)

                    if int_start:
                        ele[field + '_start'] = int_start

                    if int_end:
                        ele[field + '_end'] = int_end
                else:
                    int_value = is_int(value)
                    if int_value:
                        ele[field + '_start'] = int_value
                    if int_value:
                        ele[field + '_end'] = int_value

                ele.pop(field)

        for field in CSQ_FIELDS_TO_PARSE_INTO_PREDICTION_AND_SCORE:
            if ele.get(field):
                results = re.match(CSQ_REGEX, ele.get(field))
                prediction = results.group(1)
                score = float(results.group(2))
                ele[field + '_prediction'] = prediction
                ele[field + '_score'] = score
                ele.pop(field)

    output_dict = {}
    for field in CSQ_NON_NESTED_FIELDS:
        field_name = "CSQ_" + field
        values = [ele.get(field)
                  for ele in annotations_array if ele.get(field)]
        if not values:
            continue
        if type(values[0]) is list and values.count(values[0]) > 1 and values.count(values[0]) == len(values):
            output_dict[field_name] = values[0]
        elif type(values[0]) is not list and values.count(values[0]) > 1 and values.count(values[0]) == len(values):
            output_dict[field_name] = values[0]
        elif len(values) == 1:
            output_dict[field_name] = values[0]
        else:
            print('ERROR: ', field, values)
            print(input_string)

    nested_dicts = []
    for ele in annotations_array:
        nested_dicts.append({key: ele.get(key)
                             for key in CSQ_MODIFIED_NESTED_FIELDS if ele.get(key)})

    output_dict['CSQ_nested'] = add_prefix_to_keys_in_dictionary(
        nested_dicts, 'CSQ_nested')
    return output_dict


def determine_es_datatype(value):
    if isfloat(value):
        return "float"
    elif isint(value):
        return "integer"
    else:
        return "keyword"


def estimate_no_variants_in_file(filename, no_lines_for_estimating):
    no_lines = 0
    size_list = deque()

    # with open(filename, 'r') as fp:
    fp = get_file_handle(filename)
    for line in fp:
        if line.startswith('#'):
            continue

        if no_lines_for_estimating < no_lines:
            break

        size_list.appendleft(sys.getsizeof(line))

        no_lines += 1

    filesize = os.path.getsize(filename)

    no_variants = int(filesize / statistics.median(size_list))
    fp.close()
    return no_variants


def extract_piped_fields(fields, input_string, strip_parentheses=False):
    elements = []

    for ele in input_string.split(','):
        if strip_parentheses:
            ele = ele[1:-1]
        field_dict = dict(zip(fields, ele.split('|')))
        field_dict = {k: v for k, v in field_dict.items() if v}
        field_dict = replace_char_in_dict_key(field_dict, '.', '_')
        field_dict = replace_char_in_dict_key(field_dict, ' ', '')
        elements.append(field_dict)

    return elements


def Gene_ensGene_parser(relevant_info_fields):

    # pattern to detect dist
    pattern = r'^dist=[a-zA-Z0-9]+;dist=[a-zA-Z0-9]+$'
    Gene_ensGene = relevant_info_fields["Gene.ensGene"]

    gene_id = ' '.join(re.split('[;,]', Gene_ensGene))

    tmp_content_array = []

    if relevant_info_fields.get('GeneDetail.ensGene'):

        GeneDetail_ensGene = convert_escaped_chars(
            relevant_info_fields.get('GeneDetail.ensGene'))

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


def Gene_refGene_parser(relevant_info_fields):

    pattern = r'^dist=[a-zA-Z0-9]+;dist=[a-zA-Z0-9]+$'

    Gene_refGene = relevant_info_fields['Gene.refGene']
    symbol = ' '.join(re.split('[;,]', Gene_refGene))

    tmp_content_array = []

    if relevant_info_fields.get('GeneDetail.refGene'):

        GeneDetail_refGene = convert_escaped_chars(
            relevant_info_fields.get('GeneDetail.refGene'))

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


def get_es_id(CHROM, POS, REF, ALT, ID, index_name, type_name):
    es_id = '%s-%s-%s-%s-%s-%s-%s' % (CHROM,
                                      POS, REF, ALT, ID, index_name, type_name)
    es_id = es_id.encode('utf-8')
    es_id = hashlib.sha224(es_id).hexdigest()

    return es_id


def get_file_handle(filepath):

    if is_gz_file(filepath):
        fh = gzip.open(filepath, 'rt')
    else:
        fh = open(filepath, 'r')

    return fh


def GTEx_V6_gene_parser(input_string):
    return input_string.split('|')


def GTEx_V6_tissue_parser(input_string):
    return input_string.split('|')


def gwasCatalog_parser(input_string):
    return input_string.split('=')[1].split(',')


def ICGC_Occurrence_parser(input_string):
    output = []
    for occurance in input_string.split(','):
        tmp_dict = {}
        ICGC_Occurrence_cancer_name, ICGC_Occurrence_mutation_allele_count, ICGC_Occurrence_total_allele_count, ICGC_Occurrence_total_allele_frequency = occurance.split(
            '|')
        tmp_dict['ICGC_Occurrence_cancer_name'] = ICGC_Occurrence_cancer_name
        tmp_dict['ICGC_Occurrence_mutation_allele_count'] = int(
            ICGC_Occurrence_mutation_allele_count)
        tmp_dict['ICGC_Occurrence_total_allele_count'] = int(
            ICGC_Occurrence_total_allele_count)
        tmp_dict['ICGC_Occurrence_total_allele_frequency'] = float(
            ICGC_Occurrence_total_allele_frequency)

        output.append(tmp_dict)
    return output


def isfloat(value):
    try:
        val_float = float(value)
        try:
            val_int = int(value)
            if val_float == val_int:
                return False
        except ValueError:
            return True
    except ValueError:
        return False


def isint(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def is_int(string):
    try:
        return int(string)
    except ValueError:
        return None


def is_gz_file(filepath):
    """
    Source: https://stackoverflow.com/questions/3703276/how-to-tell-if-a-file-is-gzip-compressed
    """
    with open(filepath, 'rb') as test_f:
        return binascii.hexlify(test_f.read(2)) == b'1f8b'


def LOF_parser(fields, input_string):
    annotations_array = extract_piped_fields(
        fields, input_string, strip_parentheses=True)

    for ele in annotations_array:
        for field in LOF_FLOAT_FIELDS:
            if ele.get(field):
                if '&' in ele[field]:
                    ele[field] = float(ele[field].split('&')[0])
                else:
                    ele[field] = float(ele[field])

        for field in LOF_INTEGER_FIELDS:
            if ele.get(field):
                if '&' in ele[field]:
                    ele[field] = int(ele[field].split('&')[0])
                else:
                    ele[field] = int(ele[field])

    output_dict = {'LOF': add_prefix_to_keys_in_dictionary(
        annotations_array, 'LOF')}
    return output_dict


def NMD_parser(fields, input_string):
    annotations_array = extract_piped_fields(
        fields, input_string, strip_parentheses=True)

    for ele in annotations_array:
        for field in NMD_FLOAT_FIELDS:
            if ele.get(field):
                if '&' in ele[field]:
                    ele[field] = float(ele[field].split('&')[0])
                else:
                    ele[field] = float(ele[field])

        for field in NMD_INTEGER_FIELDS:
            if ele.get(field):
                if '&' in ele[field]:
                    ele[field] = int(ele[field].split('&')[0])
                else:
                    ele[field] = int(ele[field])

    output_dict = {'NMD': add_prefix_to_keys_in_dictionary(
        annotations_array, 'NMD')}
    return output_dict


def parse_fields(name, vcf_filename):
    fields = {}
    fp = get_file_handle(vcf_filename)
    for line_no, line in enumerate(fp, 1):
        if line.startswith("#CHROM"):
            break
        if not line.startswith("##%s" % (name)):
            continue

        field_id, description = parse_field_id_and_description(line.strip())
        fields[field_id] = description

    fp.close()
    return fields


def parse_info_fields(info, info_fields_to_skip):
    info_dict = {}
    for ele in info:
        ele_split = ele.split('=')
        if ele_split[0] in info_fields_to_skip:
            continue
        if '=' in ele:
            key, val = (ele_split[0], ''.join(ele_split[1:]))
            if val != '.':
                info_dict[key] = convert_escaped_chars(val)
        else:
            info_dict[ele] = True
    return info_dict


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


def parse_field_id_and_description(input_str):
    regexp_get_between_brackets = r'\<(.+)\>'
    regexp_get_id = r'ID=([^,]+)'
    regexp_get_description = r'Description=(.+)'

    try:
        fields = re.search(regexp_get_between_brackets, input_str).groups()[0]
    except ValueError:
        raise ValueError(
            'Could not find anything between brackets for input:\n%s' % (input_str))

    try:
        field_id = re.search(regexp_get_id, fields).groups()[0]
    except ValueError:
        raise ValueError('Could not parse "ID" from "%s"' % (fields))

    try:
        description = re.search(regexp_get_description, fields).groups()[0]
    except ValueError:
        raise ValueError('Could not parse "Description" from "%s"' % (fields))

    # remove quotes
    if description.startswith("\"") and description.endswith("\""):
        description = description[1:-1]

    return field_id, description


def replace_char_in_dict_key(input_dict, char_to_be_replaced, new_char):
    output_dict = {}
    for key, val in input_dict.items():
        if char_to_be_replaced in key:
            key = key.replace(char_to_be_replaced, new_char)
        output_dict[key] = val

    return output_dict


class VCFException(Exception):
    """Raise for my specific kind of exception"""

    def __init__(self, message, *args):
        self.message = message
        super(VCFException, self).__init__(message, *args)


def write_benchmark_results(filename, vcf_file, import_time, es_index_time, total_time):
    with open(filename, 'a') as fh:
        fh.write('\t'.join((vcf_file, import_time, es_index_time, total_time)))
        fh.write('\n')


CSQ_NON_NESTED_FIELDS = [
    'AA_AF',
    'AF',
    'AFR_AF',
    'AMR_AF',
    'EAS_AF',
    'EA_AF',
    'EUR_AF',
    'Existing_variation',
    'MAX_AF',
    'MAX_AF_POPS',
    'PHENO',
    'PUBMED',
    'SAS_AF',
    'SOMATIC',
    'VARIANT_CLASS',
    'gnomAD_AF',
    'gnomAD_AFR_AF',
    'gnomAD_AMR_AF',
    'gnomAD_ASJ_AF',
    'gnomAD_EAS_AF',
    'gnomAD_FIN_AF',
    'gnomAD_NFE_AF',
    'gnomAD_OTH_AF',
    'gnomAD_SAS_AF',
]

CSQ_NESTED_FIELDS = [
    'Allele',
    'APPRIS',
    'Amino_acids',
    'BIOTYPE',
    'CANONICAL',
    'CCDS',
    'CDS_position',
    'CLIN_SIG',
    'Codons',
    'Consequence',
    'DISTANCE',
    'DOMAINS',
    'ENSP',
    'EXON',
    'FLAGS',
    'Feature',
    'Feature_type',
    'Gene',
    'GENE_PHENO',
    'HGNC_ID',
    'HGVS_OFFSET',
    'HGVSc',
    'HGVSp',
    'HIGH_INF_POS',
    'IMPACT',
    'INTRON',
    'MOTIF_NAME',
    'MOTIF_POS',
    'MOTIF_SCORE_CHANGE"',
    'PolyPhen',
    'Protein_position',
    'SIFT',
    'STRAND',
    'SWISSPROT',
    'SYMBOL',
    'SYMBOL_SOURCE',
    'TREMBL',
    'TSL',
    'UNIPARC',
    'cDNA_position',
]

CSQ_FLOAT_FIELDS = [
    'AA_AF',
    'AF',
    'AFR_AF',
    'AMR_AF',
    'EAS_AF',
    'EA_AF',
    'EUR_AF',
    'MAX_AF',
    'SAS_AF',
    'gnomAD_AF',
    'gnomAD_AFR_AF',
    'gnomAD_AMR_AF',
    'gnomAD_ASJ_AF',
    'gnomAD_EAS_AF',
    'gnomAD_FIN_AF',
    'gnomAD_NFE_AF',
    'gnomAD_OTH_AF',
    'gnomAD_SAS_AF',
]

CSQ_INTEGER_FIELDS = ['DISTANCE', 'MOTIF_POS', ]

CSQ_FIELDS_TO_SKIP_SPLITTING_BY_AMPERSAND = ['PHENO', 'SOMATIC']

CSQ_FIELDS_TO_SPLIT_INTO_START_END_BY_DASH = [
    'CDS_position', 'Protein_position', 'cDNA_position']

CSQ_FIELDS_TO_PARSE_INTO_PREDICTION_AND_SCORE = [
    'PolyPhen', 'SIFT']  # assume -- prediction(score) -- format

CSQ_MODIFIED_NESTED_FIELDS = [
    'APPRIS',
    'Amino_acids',
    'BIOTYPE',
    'CANONICAL',
    'CCDS',
    # 'CDS_position',
    'CDS_position_start',
    'CDS_position_end',
    'CLIN_SIG',
    'Codons',
    'Consequence',
    'DISTANCE',
    'DOMAINS',
    'ENSP',
    'EXON',
    'FLAGS',
    'Feature',
    'Feature_type',
    'Gene',
    'GENE_PHENO',
    'HGNC_ID',
    'HGVS_OFFSET',
    'HGVSc',
    'HGVSp',
    'HIGH_INF_POS',
    'IMPACT',
    'INTRON',
    'MOTIF_NAME',
    'MOTIF_POS',
    'MOTIF_SCORE_CHANGE"',
    'PolyPhen_prediction',
    'PolyPhen_score',
    'Protein_position_start'
    'Protein_position_end',
    'SIFT_prediction',
    'SIFT_score',
    # 'PolyPhen',
    # 'Protein_position',
    # 'SIFT',
    'STRAND',
    'SWISSPROT',
    'SYMBOL',
    'SYMBOL_SOURCE',
    'TREMBL',
    'TSL',
    'UNIPARC',
    'cDNA_position_start',
    'cDNA_position_end',
]

CSQ_REGEX = r"^(\w+)\(([0-9]+(?:\.[0-9]+)?)\)$"

ANN_INTEGER_FIELDS = [
    'cDNA_pos',
    'cDNA_length',
    'CDS_pos',
    'CDS_length',
    'AA_pos',
    'AA_length',
    'Distance',
]

ANN_MODIFIED_NESTED_FIELDS = [
    'Allele',
    'AA_pos',
    'AA_length',
    'Annotation',
    'Annotation_Impact',
    'CDS_pos',
    'CDS_length',
    'Distance',
    'ERRORS/WARNINGS/INFO',
    'Feature_ID',
    'Feature_Type',
    'Gene_ID',
    'Gene_Name',
    'HGVS_c',
    'HGVS_p',
    'Rank',
    'Transcript_BioType',
    'cDNA_pos',
    'cDNA_length',
]

ANN_NON_NESTED_FIELDS = []

ANN_FIELDS_TO_SPLIT = ['cDNA_pos/cDNA_length',
                       'CDS_pos/CDS_length', 'AA_pos/AA_length']

ANN_FIELDS_TO_SPLIT_BY_AMPERSAND = ['ERRORS/WARNINGS/INFO', ]

LOF_FLOAT_FIELDS = ['Percent_of_transcripts_affected', ]

LOF_INTEGER_FIELDS = ['Number_of_transcripts_in_gene', ]

NMD_FLOAT_FIELDS = ['Percent_of_transcripts_affected', ]

NMD_INTEGER_FIELDS = ['Number_of_transcripts_in_gene', ]

FIELDS_TO_SKIP = ['ALLELE_END', 'ANNOVAR_DATE', 'END', ]

RUN_DEPENDENT_FIXED_FIELDS = ['FILTER', 'QUAL']

RUN_DEPENDENT_INFO_FIELDS = [
    'BaseQRankSum',
    'ClippingRankSum',
    'DP',
    'InbreedingCoeff',
    'MLEAC',
    'MLEAF',
    'MQ',
    'MQ0',
    'MQRankSum',
    'QD',
    'ReadPosRankSum',
    'SOR',
    'VQSLOD',
    'culprit'
]

RUN_DEPENDENT_FIELDS = RUN_DEPENDENT_FIXED_FIELDS + \
    RUN_DEPENDENT_INFO_FIELDS + ['sample']
