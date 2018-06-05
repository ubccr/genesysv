import argparse
from collections import deque
from pprint import pprint

import elasticsearch
from tqdm import tqdm

from utils import *

# Global STATIC Variables
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
    required.add_argument(
        "--hostname", help="Elasticsearch hostname", required=True)
    required.add_argument("--port", type=int,
                          help="Elasticsearch port", required=True)
    required.add_argument(
        "--index", help="Elasticsearch index name", required=True)
    required.add_argument(
        "--type", help="Elasticsearch doc type name", required=True)
    required.add_argument("--vcf", help="VCF file to check", required=True)
    args = parser.parse_args()

    hostname = args.hostname
    port = args.port
    index_name = args.index
    type_name = args.type
    vcf_pathname = args.vcf

    es = elasticsearch.Elasticsearch(host=hostname, port=port)

    no_lines = estimate_no_variants_in_file(vcf_pathname, 200000)
    line_count = 0
    header_found = False

    variants_found = 0
    varaints_missing = 0
    variant_ids = deque()
    variant_information = deque([], maxlen=10)
    with open(vcf_pathname, 'r') as fh:
        for line in tqdm(fh, total=no_lines, mininterval=60):
            line = line.strip()

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

            es_id = get_es_id(CHROM, POS, REF, ALT, ID, index_name, type_name)

            variant_information.append((str(es_id), str(CHROM), str(
                POS), str(REF), str(ALT), index_name, type_name, line))

            if es_id in variant_information:
                print("###", 'Duplicate Variant', es_id)
                print(line)

            variant_information.append(es_id)

            variant_ids.append(es_id)

            es_id_exists = es.exists(
                index=index_name, doc_type=type_name, id=es_id)
            if es_id_exists:
                variants_found += 1
            else:
                varaints_missing += 1
                print("###", 'Missing Variant', es_id)
                print(line)

            line_count += 1

    print(variants_found, varaints_missing)
    print(variants_found, varaints_missing, len(
        variant_ids), len(set(variant_ids)))

    # duplicate = set([variant for variant in variant_ids if variant_ids.count(variant) > 1])

    # print(duplicate)

    # fh = open('arrays.txt', 'w')

    # for variant in variant_information:
    #     fh.write("%s\n" % ','.join(variant))


if __name__ == '__main__':
    main()
