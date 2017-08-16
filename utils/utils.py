import re
from collections import defaultdict, deque, Counter
import hashlib
import sys
import os
import statistics

def convert_escaped_chars(input_string):
    input_string = input_string.replace("\\x3b", ";")
    input_string = input_string.replace("\\x2c", ",")
    input_string = input_string.replace("\\x3d", "=")

    return input_string

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

def determine_es_datatype(value):
    if isfloat(value):
        return "float"
    elif isint(value):
        return "integer"
    else:
        return "keyword"

def parse_fields(name, vcf_filename):
    fields = {}
    with open(vcf_filename, "r") as fp:
        for line_no, line in enumerate(fp , 1):
            if line.startswith("#CHROM"):
                break
            if not line.startswith("##%s" %(name)):
                continue

            field_id, description = parse_field_id_and_description(line.strip())
            fields[field_id] = description

    return fields

def parse_field_id_and_description(input_str):


    import re
    regexp_get_between_brackets = r'\<(.+)\>'
    regexp_get_id = r'ID=([^,]+)'
    regexp_get_description = r'Description=(.+)'

    try:
        fields = re.search(regexp_get_between_brackets, input_str).groups()[0]
    except:
        raise ValueError('Could not find anything between brackets for input:\n%s' %(input_str))

    try:
        field_id = re.search(regexp_get_id, fields).groups()[0]
    except:
        raise ValueError('Could not parse "ID" from "%s"' %(fields))

    try:
        description = re.search(regexp_get_description, fields).groups()[0]
    except:
        raise ValueError('Could not parse "Description" from "%s"' %(fields))

    ### remove quotes
    if description.startswith("\"") and description.endswith("\""):
        description = description[1:-1]

    return field_id, description

class VCFException(Exception):
    """Raise for my specific kind of exception"""
    def __init__(self, message, *args):
        self.message = message # without this you may get DeprecationWarning
        # Special attribute you desire with your Error,
        # perhaps the value that caused the error?:
        # allow users initialize misc. arguments as any other builtin Error
        super(VCFException, self).__init__(message, *args)

def get_es_id(CHROM, POS, REF, ALT, index_name, type_name):
    es_id = '%s%s%s%s%s%s' %(CHROM, POS, REF, ALT, index_name, type_name)
    es_id = es_id.encode('utf-8')
    es_id = hashlib.sha224(es_id).hexdigest()

    return es_id

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
