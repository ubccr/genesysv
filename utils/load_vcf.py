#!/usr/bin/env python
from __future__ import print_function
import os
import copy
import sys
import subprocess
from subprocess import check_output
import multiprocessing
import gzip
import argparse
from pprint import pprint
import multiprocessing
import logging
import math
import re
from pathlib import Path
from collections import defaultdict
from collections import OrderedDict
import json
from collections import deque
import elasticsearch
from collections import deque
from elasticsearch import helpers
import time
from make_gui import make_gui_config, make_gui
import utils
import sqlite3
from utils import *
import django

absproject_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(absproject_path) #here store is root folder(means parent).
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gdw.settings")
django.setup()

from core.models import Dataset
from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import ValidationError
from core.models import *
from core.models import *
from core.utils import get_values_from_es


parser = argparse.ArgumentParser(description='Parse vcf file(s) and create ElasticSearch mapping and index from the parsed data')
required = parser.add_argument_group('required named arguments')
required.add_argument("--vcf", help="Annovar or VEP annotated input vcf file. Must be compressed with bgzip and indexed with grabix", required=True)
required.add_argument("--tmp_dir", help="Temporory directory to store intermediate files", required=True)
required.add_argument("--annot", help="Type of variant consequence annotation. Valid values are 'annovar' or 'vep'", required=True)
required.add_argument("--hostname", help="ElasticSearch hostname", required=True)
required.add_argument("--port", help="ElasticSearch host port number", required=True)
required.add_argument("--index", help="ElasticSearch index name", required=True)
required.add_argument("--study_name", help="Name of the project", required=True)
required.add_argument("--dataset_name", help="Name of the dataset", required=True)
required.add_argument("--assembly", help="Reference genome assembly version used in the project, valid value is any of 'hg19|hg38|GRCh37|GRCh38'", required=True)
required.add_argument("--num_cores", help="Number of cpu cores to use. Default to the number of cpu cores of the system", required=False)
required.add_argument("--ped", help="Pedigree file in the format of '#Family Subject Father  Mother  Sex     Phenotype", required=False)
required.add_argument("--control_vcf", help="vcf file from control study. Must be compressed with bgzip and indexed with grabix", required=False)
required.add_argument("--interval_size", help="Genomic interval size (bp) for loading case/control vcf. Default is 5000000. Choose a smaller number if low in physical memory", required=False)
required.add_argument("--webserver_port", help="Port number for webser to explore variant data", required=False)
parser.add_argument("--debug", help="Run in single CPU mode for debugging purposes", action="store_true")
parser.add_argument("--cleanup", help="Remove temporary .json files under --tmp_dir after being indexed", action="store_true")
parser.add_argument("--skip_parsing", help="Skip the parsing process, directly go to the indexing and GUI creating step. Useful when parsing was successful but indexing failed for various reasons", action="store_true")
parser.add_argument("--gui_only", help="Only create GUI config. Used in situations where the paring and indexing were finished successfuly, but the final GUI creation failed", action="store_true")

args = parser.parse_args()

# global variables
num_cpus = args.num_cores
if (num_cpus is None):
	num_cpus = multiprocessing.cpu_count()
else:
	num_cpus = int(args.num_cores)

hostname = args.hostname
port = args.port
webserver_port = args.webserver_port
if not webserver_port:
	webserver_port = 8000

vcf = args.vcf
control_vcf = args.control_vcf
tmp_dir = args.tmp_dir
annot = args.annot
index_name = args.index
type_name = index_name + '_'
study = args.study_name
dataset_name = args.dataset_name
ped = args.ped
interval_size = args.interval_size
debug = args.debug
cleanup = args.cleanup
skip_parsing = args.skip_parsing
gui_only = args.gui_only
assembly = args.assembly

if not assembly in ['hg19', 'hg38', 'GRCh37', 'GRCh38']:
	print("Invalid assembly value. Supported values are 'hg19|hg38|GRCh37|GRCh38'")
	sys.exit(2)


excluded_list = ['AA', 'ANNOVAR_DATE', 'MQ0', 'DB', 'POSITIVE_TRAIN_SITE', 'NEGATIVE_TRAIN_SITE', 'culprit']
cohort_specific = ['AC', 'AF', 'AN', 'BaseQRankSum', 'DP', 'GQ_MEAN', 'GQ_STDDEV', 'HWP', 'MQRankSum', 'NCC', 'MQ', 'ReadPosRankSum', 'QD', 'VQSLOD']

def check_commandline(vcf, control_vcf, annot):
	# check if valid annotation type is specified
	if annot == 'vep':
		annot_type = 'vep'
	elif annot == 'annovar':
		annot_type = 'annovar'
	else:
		print("Unsupported annotation type: %s" % annot)
		sys.exit(2)

	# check if valid vcf file specified
	vcf = os.path.abspath(vcf)
	out = check_output(["grabix", "check", vcf])

	if str(out.decode('ascii').strip()) != 'yes':
		print("Invalid vcf file. Please provide a bgzipped/grabix indexed vcf file.")
		sys.exit(2)

	if control_vcf:
		control_vcf = os.path.abspath(control_vcf)
		out = check_output(["grabix", "check", control_vcf])

		if str(out.decode('ascii').strip()) != 'yes':
			print("Invalid control_vcf file. Please provide a bgzipped/grabix indexed vcf file.")
			sys.exit(2)
	# check if tabix index files exist for case/control studies
	if control_vcf:
		tbi_file_case = Path(os.path.abspath(vcf) + '.tbi')
		tbi_file_control = Path(os.path.abspath(control_vcf) + '.tbi')

		if tbi_file_case.exists() and tbi_file_control.exists():
			pass
		else:
			print("Tabix indexed file(s) not found. Please index the vcf file(s) with tabix.")
			sys.exit(2)

def process_ped_file(ped_file):
	ped_info = {}

	with open(ped_file, 'r') as pd:
		for line in pd.readlines():
			if line.startswith('#'):
				continue

			family, subject, father, mother, sex, phenotype, *_ = line.split()
			ped_info[subject] = dict(zip(["family", "father", "mother", "sex", "phenotype"], [family, father, mother, sex, phenotype]))

	return(ped_info)

def process_vcf_header(vcf):
	info_dict = defaultdict()
	format_dict = defaultdict()
	contig_dict = defaultdict()
	csq_dict = {}
	num_header_lines = 0
	csq_fields = []
	col_header = []
	chr2len = {}

	# compile some patterns
	p = re.compile(r'^##.*?=<(.*)>$')
	p1 = re.compile(r'^.*?ID=(.*?),.*')
	p2 = re.compile(r'^.*?Type=(.*?),.*')
	p3 = re.compile(r'^.*?\"(.*?)\".*')
	p4 = re.compile(r'^##contig.*?length=(\d+),assembly=(.*)>')
	p5 = re.compile(r'^##reference=.*?(19|hg19|hg38|GRCh37|GRCh38|b37|hs37d5|v37_decoy)\.fa.*')

	with gzip.open(vcf, 'rt') as fp:
		while True:
			line = fp.readline()
			if line.startswith('#CHROM'):
				col_header = line.strip().split("\t")
				col_header[0] = re.sub('#', '', col_header[0])
				num_header_lines += 1
				break

			num_header_lines += 1

			id_ = p1.match(line)
			type_ = p2.match(line)
			desc_ = p3.match(line)
			contig_ = p4.match(line)
			if id_:
				if type_:
					if desc_:
						if line.startswith('##INFO'):
							# Annovar put VERYTHING as string type, so correct it
							if id_.group(1).startswith('gnomAD_') or id_.group(1).startswith('ExAC_') or id_.group(1).endswith('score') or id_.group(1).endswith('SCORE') or id_.group(1).endswith('_frequency') or id_.group(1).startswith('CADD') and id_.group(1).endswith('score') or id_.group(1).startswith('Eigen-') or id_.group(1).startswith('GERP++'):
								info_dict[id_.group(1)] = {'type' : 'float', 'Description' : desc_.group(1)}
							else:
								info_dict[id_.group(1).replace('.', '_')] = {'type' : type_.group(1).lower(), 'Description' : desc_.group(1)}
						elif line.startswith('##FORMAT'):
							if id_.group(1) == 'PL': # make this as sting type
								format_dict[id_.group(1)] = {'type' : "string", 'Description' : desc_.group(1)}
							else:
								format_dict[id_.group(1)] = {'type' : type_.group(1).lower(), 'Description' : desc_.group(1)}
						else:
							pass
							#print("Should not reach here %s" % line)
					else:
						print("header1 %s", line)
						continue

				else:
					if contig_:
						contig_dict[id_.group(1)] = {'length' : contig_.group(1), 'assembly' : contig_.group(2)}

	if 'CSQ' in info_dict:
		val = info_dict['CSQ']['Description']
		csq_fields = val.split("|")
		csq_fields[0] = re.sub('Consequence annotations from Ensembl VEP. Format: ', '', csq_fields[0])

		# make a CSQ name to type dict
		csq_dict = {key: {'type' : 'keyword', 'null_value' : 'NA'} for key in csq_fields }
		{csq_dict[key].update({'type' : 'float', 'null_value' : -999.99}) for key in csq_dict if key.endswith('_AF') or key == 'AF' or key.startswith('CADD')}
		{csq_dict[key].update({'type' : 'integer', 'null_value' : -999}) for key in csq_dict if key == 'DISTANCE'}
		{csq_dict[key].update({'type' : 'keyword', 'null_value' : 'NA'}) for key in csq_dict if key.endswith('position')}

		# partition keys into local and global space
		# at this moment, we have to hard code the feature list to include in each of the lists
		csq_local = ['Consequence', 'IMPACT', 'SYMBOL', 'Gene', 'Feature_type', 'Feature', 'BIOTYPE', 'EXON', 'INTRON', 'HGVSc', 'HGVSp', 'cDNA_position', 'CDS_position', 'Protein_position', 'Amino_acids', 'Codons', 'DISTANCE', 'STRAND', 'HGNC_ID', 'SWISSPROT', 'DOMAINS', 'miRNA', 'SIFT', 'PolyPhen', 'RadialSVM_score', 'RadialSVM_pred', 'LR_score', 'LR_pred']
		csq_global = ['Existing_variation', 'AF', 'AFR_AF', 'AMR_AF', 'EAS_AF', 'EUR_AF', 'SAS_AF', 'AA_AF', 'EA_AF', 'gnomAD_AF', 'gnomAD_AFR_AF', 'gnomAD_AMR_AF', 'gnomAD_ASJ_AF', 'gnomAD_EAS_AF', 'gnomAD_FIN_AF', 'gnomAD_NFE_AF', 'gnomAD_OTH_AF', 'gnomAD_SAS_AF', 'MAX_AF', 'MAX_AF_POPS', 'EUR', 'CLIN_SIG', 'SOMATIC', 'CADD_PHRED', 'CADD_RAW', 'CADD_raw', 'CADD_phred']

		csq_dict_local = {key:val for key, val in csq_dict.items() if key in csq_local}
		csq_dict_global = {key:val for key, val in csq_dict.items() if key in csq_global}

	# get chromosome length
	valid_chrs = range(1, 23)
	valid_chrs = [str(item) for item in valid_chrs ]
	valid_chrs.append('X')
	valid_chrs.append('Y')
	valid_chrs.append('M')

	for key in contig_dict:
		if key in valid_chrs:
			chr2len[key] = int(contig_dict[key]['length'])
	if annot == 'vep':
		return([num_header_lines, csq_fields, col_header, chr2len, info_dict, format_dict, contig_dict, csq_dict_local, csq_dict_global])
	elif annot == 'annovar':
		return([num_header_lines, col_header, chr2len, info_dict, format_dict, contig_dict])

def process_vcf_data(vcf, number_of_lines_to_read, vcf_info):
	line_count = 0
	key_type_dict = {}
	key_type_dict_csq = {}
	key_type_dict_format = {}

	with gzip.open(vcf, 'rt') as fp:
		while True:
			line = fp.readline()
			if line.startswith("#"):
				continue

			line_count += 1
			col_data = line.strip().split("\t")

  			# parse INFO field
			info_fields = col_data[7].split(";")

			# parse FORMAT field
			format_fields = col_data[8].split(":")

			info_dict = {item.split("=")[0]:item.split("=")[1] for item in info_fields if '=' in item}


			for key, val in info_dict.items():
				if key == 'CSQ':
					val2 = val.split('|')
					csq_dict_ = dict(zip(vcf_info['csq_fields'], val2))

					for k, v in csq_dict_.items():
						if v != '':
							if isfloat(v):
								key_type_dict_csq.update({k: {"type": "float", "null_value": -999.99}})
							elif isint(v):
								if k in key_type_dict_csq:
									if 'type' in key_type_dict_csq[k]:
										if key_type_dict_csq[k]['type'] == "float":
											continue # float overwrite integer type
										else:
											key_type_dict_csq.update({k: { "type": "integer", "null_value": -999}})
								else:
									key_type_dict_csq.update({k: { "type": "integer", "null_value": -999}})
							else:
								# test if compound values, i.e. comma or ampersand separated values
								if ',' in v:
									tmp = v.split(',')[0]
									if isfloat(tmp):
										key_type_dict_csq.update({k: { "type": "float", "null_value": -999.99}})
									elif isint(tmp):
										key_type_dict_csq.update({k: { "type": "integer", "null_value": -999}})
									else:
										key_type_dict_csq.update({k: {"type": "keyword", "null_value": 'NA'}})
								elif '&' in v:
									tmp = v.split('&')[0]
									if isfloat(tmp):
										key_type_dict_csq.update({k: { "type": "float", "null_value": -999.99}})
									elif  isint(tmp):
										key_type_dict_csq.update({k: { "type": "integer", "null_value": -999}})
									else:
										key_type_dict_csq.update({k: {"type": "keyword", "null_value": 'NA'}})
								else:
									key_type_dict_csq.update({k: {"type": "keyword", "null_value": 'NA'}})

				else:
					if isfloat(val):
						key_type_dict.update({key: { "type": "float", "null_value": -999.99}})
					elif  isint(val):
						if key in key_type_dict:
							if 'type' in key_type_dict[key]:
								if key_type_dict[key]['type'] == "float":
									continue
						else:
							key_type_dict.update({key: {"type": "integer", "null_value": -999}})
					else:
						if ',' in val:
							tmp = val.split(',')[0]
							if isfloat(tmp):
								key_type_dict.update({key: { "type": "float", "null_value": -999.99}})
							elif  isint(tmp):
								key_type_dict.update({key: {"type": "integer", "null_value": -999}})
							else:
								key_type_dict.update({key: {"type": "keyword", "null_value": 'NA'}})
						elif '&' in val:
							tmp = val.split('&')[0]
							if  isfloat(tmp):
								key_type_dict.update({key: { "type": "float", "null_value": -999.99}})
							elif isint(tmp):
								key_type_dict.update({key: {"type": "integer", "null_value": -999}})
							else:
								key_type_dict.update({key: {"type": "keyword", "null_value": 'NA'}})
						else:
							key_type_dict.update({key: {"type": "keyword", "null_value": 'NA'}})
			for i in range(9, len(vcf_info['col_header'])):

				format_dict = dict(zip(format_fields, col_data[i].split(':')))

				for key, val in format_dict.items():

					if val != '.':
						if ',' in val:
							tmp = val.split(',')[0]
							if  isfloat(tmp):
								key_type_dict_format.update({key: { "type": "float", "null_value": -999.99}})
							elif isint(tmp):
								key_type_dict_format.update({key: {"type": "integer", "null_value": -999}})
							else:
								key_type_dict_format.update({key: {"type": "keyword", "null_value": 'NA'}})
						else:
							if key.endswith('GT'):
								key_type_dict_format.update({key: {"type": "keyword"}})
							elif isfloat(val):
								key_type_dict_format.update({key: { "type": "float", "null_value": -999.99}})
							elif isint(tmp):
								if key in key_type_dict_format:
									if "type" in key_type_dict_format[key]:
										if key_type_dict_format[key]["type"] == "float":
											continue
										else:
											key_type_dict_format.update({key: {"type": "integer", "null_value": -999}})
								else:
									key_type_dict_format.update({key: {"type": "integer", "null_value": -999}})
							else:
								key_type_dict_format.update({key: {"type": "keyword", "null_value": 'NA'}})

			if line_count > 2000:
				break

	# update vcf_info
	tmp_dict = copy.deepcopy(vcf_info)
	for key, val in tmp_dict['info_dict'].items():
		if key in key_type_dict:
			vcf_info['info_dict'][key].update(key_type_dict[key])
	if annot == 'vep':
		for key, val in tmp_dict['csq_dict_local'].items():
			if key in key_type_dict_csq:
				vcf_info['csq_dict_local'][key].update(key_type_dict_csq[key])
		for key, val in tmp_dict['csq_dict_global'].items():
			if key in key_type_dict_csq:
				vcf_info['csq_dict_global'][key].update(key_type_dict_csq[key])
	for key, val in tmp_dict['format_dict'].items():
	 	if key in key_type_dict_format:
	 		vcf_info['format_dict'][key].update(key_type_dict_format[key])

	return(vcf_info)

def parse_vcf(vcf, interval, outfile, vcf_info):
	p = multiprocessing.current_process()

	# divide interval into smaller chunks to minimize memory footprint
	chunk_size = 5000
	start = interval[0]
	num_variants_processed = 0

	logfile = re.sub('json', 'log', outfile)
	log = open(logfile, 'w')

	with open(outfile, 'w') as f:
		while True:
			if start < interval[1]:
				end = start + chunk_size - 1
				if end >= interval[1]:
					end = interval[1]

				command = ["grabix", "grab", vcf, str(start), str(end)]
				output = check_output(command)
				try:
					output = output.decode('latin1') #ascii')
				except ValueError:
					log.write("decoding error: %s, %s\n" % (start, end))
					start = end + 1
					continue
				# remove the header lines from output
				lines = output.splitlines()
				variant_lines = lines[vcf_info['num_header_lines']:]

				process_line_data(variant_lines, log, f, vcf_info)

				num_variants_processed += end - start + 1

				print("Pid %s: processed %d variants" % (p.pid, num_variants_processed))

				# update start and end positions
				start = end + 1

				if start >= interval[1]:
					break

def parse_info_fields(info_fields, result, log, vcf_info, group = ''):
	p = re.compile(r'^(.*?)\((.*)\)') # for parsing SIFT and PolyPhen predition and score
	tag_fields = [item for item in info_fields if not '=' in item]
	for tag in tag_fields:
		result[tag] = 'Yes'

	tmp = [info for info in info_fields if '=' in info]
	tmp_dict = {d[0].replace('.', '_'):d[1] for d in [item.split('=') for item in tmp]}
	for item in excluded_list:
		if item in tmp_dict:
			del tmp_dict[item]

	for key, val in tmp_dict.items():
		if key not in  vcf_info['info_dict']:
			log.write("Key not exists: %s" % key)
			continue
		if key == 'CSQ' and annot == 'vep':
			# VEP annotation repeated the variant specific features, such as MAF, so move them to globol space.
			# Only keey gene and consequence related info in the nested structure
			csq_list = []
			info_csq = val.split(',')

			for csq in info_csq:
				csq2	= csq.split('|')
				csq_dict2 = dict(zip(vcf_info['csq_fields'], csq2)) # map names to values for CSQ annotation sub-fields

				# partition csq_dict2 into global and local space
				csq_dict2_local = {key:val for key, val in csq_dict2.items() if key in vcf_info['csq_dict_local']}
				csq_dict2_global = {key:val for key, val in csq_dict2.items() if key in vcf_info['csq_dict_global']}

				tmp_dict = {}

				for key2, val2 in csq_dict2_local.items():

					if key2 in ['SIFT', 'PolyPhen']:
						m = p.match(val2)
						if m:
							tmp_dict[key2 + '_pred'] = m.group(1)
							tmp_dict[key2 + '_score'] = float(m.group(2))
						else: # empty value or only pred or score are included in vep annotation
							if val2 =='':
								tmp_dict[key2 + '_pred'] = 'NA'
								tmp_dict[key2 + '_score'] = -999.99
							else:
								try:
									x = float(val2)
									tmp_dict[key2 + '_score'] = x
									tmp_dict[key2 + '_pred'] = 'NA'
								except ValueError:
									tmp_dict[key2 + '_score'] = -999.99
									tmp_dict[key2 + '_pred'] = val2
									log.write("Value error: %s, %s\n" % (key2, val2))
								continue

					elif vcf_info['csq_dict_local'][key2]['type'] == 'integer':
						if val2 == '':
							csq_dict2_local[key2] = -999
						else:
							try:
								csq_dict2_local[key2] = int(csq_dict2_local[key2])
							except ValueError:
								log.write("Casting to int error: %s, %s\n" % (key2, val2))
								continue
					elif key2 == 'Consequence':
						if val2 == '':
							csq_dict2_local[key2] = 'NA'
						else:
							tmp = val2.split('&')
							if len(tmp) > 1:
								csq_dict2_local[key2] = tmp
							else:
								csq_dict2_local[key2] = tmp[0]
					else:
						if val2 == '':
							csq_dict2_local[key2] = 'NA'
						else:
							csq_dict2_local[key2] = val2

				tmp_dict2 = {}

				for key2, val2 in csq_dict2_global.items():
					if vcf_info['csq_dict_global'][key2]['type'] == 'integer':
						if val2 == '':
							tmp_dict2[key2] = -999
						else:
							try:
								tmp_dict2[key2] = int(csq_dict2_global[key2])
							except ValueError:
								log.write("Casting to int error: %s, %s\n" % (key2, val2))
								continue

					elif vcf_info['csq_dict_global'][key2]['type'] == 'float':
						if val2 == '':
							tmp_dict2[key2] = -999.99
						else:
							tmp = val2.split('&')
							if len(tmp) > 1:
								tmp_dict2[key2] = [float(item) for item in tmp]
							else:
								tmp_dict2[key2] = float(val2)
					else:
						if key2 == "AF":
							if val2 == '':
								tmp_dict2[key2] = -999.99
							else:
								tmp = val2.split('&')
								if len(tmp) > 1:
									tmp_dict2[key2] = [float(item) for item in tmp]
								else:
									tmp_dict2[key2] = tmp[0]
						elif key2 == 'SOMATIC':
							continue
						elif key2 == 'Existing_variation':
							if val2 != '':
								tmp_variants = val2.split('&')
								cosmic_ids = [item for item in tmp_variants if item.startswith('COSM')]
								dbsnp_ids = [item for item in tmp_variants if item.startswith('rs')]
								if len(cosmic_ids) > 0:
									if len(cosmic_ids) > 1:
										result['COSMIC_ID'] = cosmic_ids
									else:
										result['COSMIC_ID'] = cosmic_ids[0]
								else:
									result['COSMIC_ID'] = None
								if len(dbsnp_ids) > 0:
									if len(dbsnp_ids) > 1:
										result['dbSNP_ID'] = dbsnp_ids # use array value
									else:
										result['dbSNP_ID'] = dbsnp_ids[0] # use scalar value
								else:
									if 'dbSNP_ID' in result and result['dbSNP_ID'] is not None:
										continue
									else:
										result['dbSNP_ID'] = None
							else:
								result['COSMIC_ID'] = None
								if 'dbSNP_ID' in result and result['dbSNP_ID'] is not None:
									continue
								else:
									result['dbSNP_ID'] = None
						elif key2 in ['CLIN_SIG', 'MAX_AF_POPS'] and val2 != '':
							tmp = val2.split('&')
							if len(tmp) > 1:
								result[key2] = tmp
							else:
								result[key2] = tmp[0]
						else:
							if val2 == '':
								result[key2] = 'NA'
							else:
								# need to know delimiters used in order to split
								result[key2] = val2

				del csq_dict2_local['SIFT']
				del csq_dict2_local['PolyPhen']
				csq_dict2_local.update(tmp_dict)
				csq_list.append(csq_dict2_local)

			result['CSQ_nested'] = (csq_list)
			result.update(tmp_dict2)

		elif key == 'AAChange_refGene':
			aac_list = []
			aac_dict = {}

			if val == '.' or val == 'UNKNOWN':
				aac_dict['RefSeq'] = 'NA'
				aac_dict['exon_id_rg'] = 'NA'
				aac_dict['cdna_change_rg'] = 'NA'
				aac_dict['aa_change_rg'] = 'NA'
				aac_list.append(aac_dict)
			else:
				val_list = val.split(',')
				for subval in val_list:
					gene, refseq, exon, *cdna_aa = subval.split(':')
					aac_dict['RefSeq'] = refseq
					aac_dict['exon_id_rg'] = exon

					if len(cdna_aa) == 2:
						aac_dict['cdna_change_rg'] = cdna_aa[0]
						aac_dict['aa_change_rg'] = cdna_aa[1]
					else:
						aac_dict['cdna_change_rg'] = 'NA'
						aac_dict['aa_change_rg'] = 'NA'
					aac_list.append(aac_dict)

			result[key] = aac_list
		elif key == 'AAChange_ensGene':
			aac_list = []
			aac_dict = {}
			if val == '.' or val == 'UNKNOWN':
				aac_dict['EnsembleTranscriptID'] = 'NA'
				aac_dict['exon_id_eg'] = 'NA'
				aac_dict['cdna_change_eg'] = 'NA'
				aac_dict['aa_change_eg'] = 'NA'
				aac_list.append(aac_dict)
			else:
				val_list = val.split(',')
				for subval in val_list:
					gene, transcript, exon, *cdna_aa = subval.split(':')
					aac_dict['EnsembleTranscriptID'] = transcript
					aac_dict['exon_id_eg'] = exon
					if len(cdna_aa) == 2:
						aac_dict['cdna_change_eg'] = cdna_aa[0]
						aac_dict['aa_change_eg'] = cdna_aa[1]
					else:
						aac_dict['cdna_change_eg'] = 'NA'
						aac_dict['aa_change_eg'] = 'NA'

					aac_list.append(aac_dict)
			result[key] = aac_list

		elif vcf_info['info_dict'][key]['type'] == 'integer':
			if val == '.':
				val = -999
			if key == 'CIPOS' or key == 'CIEND':
				if key in cohort_specific:
					result[key + group] = val # keep as is (i.e. string type)
				else:
					result[key] = val
			else:
				if key == 'FS':
					result[key + group] = val # keep FS interger string as is
				else:
					try:
						result[key + group] = int(val)
						if math.isnan(int(val)):
							if key in cohort_specific:
								result[key + group] = -999
							else:
								result[key] = -999
					except ValueError:
						log.write("Interger parsing problem: %s, %s\n" % (key, val))
						continue

		elif vcf_info['info_dict'][key]['type'] == 'float':
			if '++' in key:
 				key = key.replace('++', 'plusplus')
			try:
				x = float(val)
				if math.isnan(x):
					if key in cohort_specific:
						result[key + group] = -999.99
					else:
						result[key] = -999.99

				elif math.isinf(x):
					if key in cohort_specific:
						result[key + group] = 9999.99
					else:
						result[key] = 9999.99
				else:
					if key in cohort_specific:
						result[key + group] = x
					else:
						result[key] = x
			except ValueError:
				if key in cohort_specific:
					result[key + group] = - 999.99
				else:
					result[key] = -999.99
				#log.write("Parsing problem: %s %s. value is assigned with -999.99\n" % (key, val))
				continue
		elif 'snp' in key:
			if key == 'snp138NonFlagged':
				if val == '.':
					result[key] = 'NA'
				else:
					result[key] = val
			else:
				if val == '.':
					val = 'NA'
				if 'dbSNP_ID' in result and result['dbSNP_ID'] is not None:
					continue
				else:
					result['dbSNP_ID'] = val
		elif key == 'dbSNP_ID':
			continue # skip because Annovar does not populate this field for unknown reason
		elif key == 'COSMIC_ID':
			continue # same reason as above
		elif key in ['CLINSIG', 'CLNDBN', 'CLNDSDBID', 'CLNDSDB', 'CLNACC'] and val != '.':
		  	result[key] = val.split('|')
		elif 'cosmic' in key:
			if val == '.':
				cosmic_id = 'NA'
				occurrence = 'NA'
			else:
				cosmic_id, occurrence = val.split("\\x3b")
				cosmic_id = cosmic_id.split('\\x3d')[1]
				occurrence = occurrence.split('\\x3d')[1]

			tmp = cosmic_id.split(',')
			if len(tmp) > 1:
				result['COSMIC_ID'] = tmp
			else:
				result['COSMIC_ID'] = tmp[0]
			tmp = occurrence.split(',')
			if len(tmp) > 1:
				result['COSMIC_Occurrence'] = tmp
			else:
				result['COSMIC_Occurrence'] = tmp[0]
		elif key == 'VT':
			result['VariantType'] = val

		else: # other string type
			if val =='.':
				val = 'NA'
			else:
				val = val.replace('\\x3d', '=')
				val = val.replace('\\x3b',';')
			if key in cohort_specific:
				result[key + group] = val
			else:
				result[key] = val

	return(result)

def parse_sample_info(result, format_fields, sample_info, log, vcf_info, group = ''):
	sample_data_array = []

	for sample_id, sample_data in sample_info.items():
		sample_data_dict = {}

		# do not waste time and storage for no GT
		if sample_data.startswith('.|.') or sample_data.startswith('./.') or sample_data.startswith('0|.') or sample_data.startswith('.|0') or sample_data.startswith('0/.'):
			continue
		# skip parsing hom_ref GT if no ped file is specified to save time and disk space
		if not ped and (sample_data.startswith('0/0') or sample_data.startswith('0|0')):
			continue

		format_fields = format_fields if isinstance(format_fields, list) else [format_fields]
		tmp = sample_data.split(':')
		sample_sub_info_dict = dict(zip(format_fields, tmp))

		# handle comma-delimited numeric values
		for (key, val) in sample_sub_info_dict.items():
			if key in ['GT', 'PGT', 'PID']:
				if val == '.':
					sample_data_dict[key] = 'NA'
				else:
					sample_data_dict[key] = val
			elif ',' in val:
				sub_items = val.split(',')
				if key == 'AD':
					sample_data_dict['AD_ref'], sample_data_dict['AD_alt'], *_ = sub_items
					sample_data_dict['AD_ref'] = int(sample_data_dict['AD_ref'])
					sample_data_dict['AD_alt'] = int(sample_data_dict['AD_alt'])
				elif key == 'PL':
					sample_data_dict[key] = val
				else:
					log.write("Unknown type: %s, %s\n" % (key, val))
					continue
			elif key == 'DP':
				if val == '.':
					sample_data_dict[key] = -999
				else:
					sample_data_dict[key] = int(val)
			else:
				if val == '.':
					sample_data_dict[key] = 'NA'
				else:
					if vcf_info['format_dict'][key]['type'] == 'float':
						sample_data_dict[key] = float(val)
					elif vcf_info['format_dict'][key]['type'] == 'integer':
						sample_data_dict[key] = int(val)
					else:
						log.write("Unknown type: %s, %s\n" % (key, val))
						continue

		# add information from ped file
		if ped and sample_id in  vcf_info['ped_info']:
			sample_data_dict['Family_ID'] = vcf_info['ped_info'][sample_id]['family']
			sample_data_dict['Father_ID'] = vcf_info['ped_info'][sample_id]['father']
			sample_data_dict['Mother_ID'] = vcf_info['ped_info'][sample_id]['mother']
			sample_data_dict['Sex'] = vcf_info['ped_info'][sample_id]['sex']
			sample_data_dict['Phenotype'] = vcf_info['ped_info'][sample_id]['phenotype']

			# caculate additional fields
			father_id = vcf_info['ped_info'][sample_id]['father']
			if father_id in sample_info:
				father_data = sample_info[father_id]
				father_gt = father_data.split(':')[0]
			else:
				father_gt = 'NA'

			mother_id = vcf_info['ped_info'][sample_id]['mother']
			if mother_id in sample_info:
				mother_data = sample_info[mother_id]
				mother_gt = mother_data.split(':')[0]
			else:
				mother_gt = 'NA'

			if father_id in vcf_info['ped_info']:
				father_phenotype = vcf_info['ped_info'][father_id]['phenotype']
			else:
				father_phenotype = 'NA'
			if mother_id in vcf_info['ped_info']:
				mother_phenotype = vcf_info['ped_info'][mother_id]['phenotype']
			else:
				mother_phenotype = 'NA'

			sample_data_dict['Father_Genotype'] = father_gt
			sample_data_dict['Mother_Genotype'] = mother_gt
			sample_data_dict['Mother_Phenotype'] = mother_phenotype
			sample_data_dict['Father_Phenotype'] = father_phenotype

		sample_data_dict['Sample_ID'] = sample_id
		if group != '':
			sample_data_dict['group'] = re.sub('_', '', group)

		sample_data_array.append(sample_data_dict)

	result['sample'] = sample_data_array

	return(result)

def process_line_data(variant_lines, log, f, vcf_info):
	for line in variant_lines:
		result = OrderedDict()
		col_data = line.strip().split("\t")
		data_fixed = dict(zip(vcf_info['col_header'][:7], col_data[:7]))
		result['Variant'] = "_".join([data_fixed['CHROM'], data_fixed['POS'], data_fixed['REF'][:10], data_fixed['ALT'][:10]])

		# in the first 8 field of vcf format, POS and QUAL are of non-string type, so convert them to the right type
		data_fixed['POS'] = int(data_fixed['POS'])
		data_fixed['QUAL'] = float(data_fixed['QUAL'])

		# FIlTER field may contain multiple valuse, so parse them
		tmp = data_fixed['FILTER'].split(';')
		if len(tmp) > 1:
			data_fixed['FILTER'] = tmp
		else:
			data_fixed['FILTER'] = tmp[0]

		if data_fixed['ID'] == '.':
			data_fixed['ID'] = 'NA'
		else:
			if data_fixed['ID'].startswith('rs'):
				result['dbSNP_ID'] = data_fixed['ID']

		result.update(data_fixed)

		# get variant type
		if data_fixed['REF'] in ['G','A','T','C'] and data_fixed['ALT'] in ['G','A','T','C']:
			result['VariantType'] = 'SNV'
		else:
			result['VariantType'] = 'INDEL'

		# parse INFO field
		info_fields = col_data[7].split(";")

		# parse FORMAT field
		format_fields = col_data[8].split(":")

		# parse INFO field
		result = parse_info_fields(info_fields, result, log, vcf_info)

		# parse sample related data
		sample_info = dict(zip(vcf_info['col_header'][9:], col_data[9:]))

		result = parse_sample_info(result, format_fields, sample_info, log, vcf_info)

		json.dump({"_index" : index_name, "_type" : type_name, "_source" : result}, f, ensure_ascii=True)
		f.write("\n")


def process_single_cohort(vcf, vcf_info):

	# get the total number of variants in the input vcf
	out = check_output(["grabix", "size", vcf])
	total_lines = int(out.decode('latin1').strip())

	# calculate number of variants each cpu core need to process
	num_lines_per_proc = math.ceil(total_lines/num_cpus)

	# create a intervals list for distributing variants into each of the processes
	intervals = []
	line_start = 1
	line_end = num_lines_per_proc + line_start

	# get the interval list
	while True:
		if (line_start < total_lines):
			line_end = line_start + num_lines_per_proc - 1
			if line_end >= total_lines:
				line_end = total_lines

			interval = [line_start, line_end]
			intervals.append(interval)
			line_start = line_end + 1

			if (line_end >= total_lines):
				break

	# to be used to hold the process ids for the join() function
	processes = []
	output_json = []

	if debug:
		for intev in intervals: # debug
			output_file = 'tmp/output_' + str(intev) + '.json'
			parse_vcf(vcf, intev, output_file, vcf_info)
			output_json.append(output_file)
	else:
 		# dispatch subtasks to each of the processes
		for i in range(num_cpus):
			output_file = os.path.join(tmp_dir, os.path.basename(vcf) + '.chunk_' + str(i) + '.json')
			proc = multiprocessing.Process(target=parse_vcf, args=[vcf, intervals[i], output_file, vcf_info])
			proc.start()
			processes.append(proc)
			output_json.append(output_file)

	 	# wait for all the processes to finish
		for proc in processes:
			proc.join()
			print("Process %s finished ..." % proc.pid)

	return(output_json)

def process_case_control(case_vcf, control_vcf, vcf_info):
	batch_size = 1000000 # reduce this number if memory is an issue
	if interval_size:
		batch_size = interval_size

	batch_list = []
	output_json = []
	processes = []

	for chrom, length in vcf_info['chr2len'].items():
		start = 1
		while True:
			if (start < length):
				end = start + batch_size - 1
				batch = chrom + ':' + str(start) + '-' + str(end)
				if (end >= length):
					batch = chrom + ':' + str(start) + '-' + str(length)

				batch_list.append(batch)

				start = end + 1
				if (start >= length):
					break

	# calculate number of batches each cpu need to process
	batches_per_cpu = math.ceil(len(batch_list)/num_cpus)
	batch_start = 0

	if debug:
		output_file = 'tmp/output_case_control_' + str(batch_list[0]) + '.json'
		parse_case_control(case_vcf, control_vcf, [batch_list[0]], output_file, vcf_info)
		output_json.append(output_file)
	else:
		for i in range(num_cpus):
			batch_end = batch_start + batches_per_cpu
			if batch_end > len(batch_list):
				batch_end = len(batch_list)

			output_file = os.path.join(tmp_dir, os.path.basename(control_vcf) + '.chunk_' + str(i) + '.json')
			proc = multiprocessing.Process(target=parse_case_control, args=[case_vcf, control_vcf, batch_list[batch_start:batch_end], output_file, vcf_info])
			proc.start()
			processes.append(proc)
			output_json.append(output_file)

			batch_start = batch_end + 1

		# wait for all the processes to finish
		for proc in processes:
			proc.join()
			print("Process %s finished ..." % proc.pid)

	return(output_json)

def parse_case_control(case_vcf, control_vcf, batch_sub_list, outfile, vcf_info):

	p = multiprocessing.current_process()

	logfile = re.sub('json', 'log', outfile)
	log = open(logfile, 'w')

	batch_count = 0
	total_batches = len(batch_sub_list)

	with open(outfile, 'w') as f:
		for batch in batch_sub_list:
			batch_count += 1
			data_dict = defaultdict()
			data_dict['_case'] = {}
			data_dict['_control'] = {}

	 		# get a chunck of line from each of the vcf files
			output_case = check_output(["tabix", case_vcf, batch])
			output_control = check_output(["tabix", control_vcf, batch])

			if len(output_case) + len(output_control) == 0:
				print("Empty batch %s" % batch)
				continue

			print("Pid %s processing batch  %s, %d of %d"% (p.pid, batch, batch_count, total_batches))

			output_case = output_case.decode('latin1')
			output_control = output_control.decode('latin1')

			lines_case = output_case.splitlines()
			lines_control = output_control.splitlines()

			for line in lines_case:
				col_data = line.strip().split("\t")
				v_id = '_'.join([col_data[0], col_data[1], col_data[3], col_data[4]])
				data_dict['_case'][v_id] = col_data
			for line in lines_control:
				col_data = line.strip().split("\t")
				v_id = '_'.join([col_data[0], col_data[1], col_data[3], col_data[4]])
				data_dict['_control'][v_id] = col_data

			result = defaultdict()
			seen = {}

			counter = 0
			for group in ['_case', '_control']:
				for v_id in data_dict[group]:
					tmp = {}
					tmp2 = {}

					data_fixed = dict(zip(vcf_info['col_header'][:7], data_dict[group][v_id][:7]))


					if v_id in seen: # alread found in case
						# parse INFO field
						info_fields = data_dict[group][v_id][7].split(";")
						result_info = parse_info_fields(info_fields, tmp, log, vcf_info, group)
						result[v_id].update(result_info)

						# parse FORMAT field
						format_fields = data_dict[group][v_id][8].split(":")

						# parse sample related data
						sample_info = dict(zip(vcf_info['col_header'][9:], data_dict[group][v_id][9:]))
						result_sample = parse_sample_info(tmp2, format_fields, sample_info, log, vcf_info, group=group)
						result[v_id]['sample'].extend(result_sample['sample'])

						result[v_id]['QUAL' + group] = float(data_fixed['QUAL'])
						result[v_id]['FILTER' + group] = data_fixed['FILTER']
					else:
						seen[v_id] = True

						# make a short format of variant IDs, i.e. keep at most 9 bases for indels
						variant = '_'.join([data_fixed['CHROM'], data_fixed['POS'], data_fixed['REF'][:10], data_fixed['ALT'][:10]])

						result[v_id] = {}
						result[v_id]['Variant'] = variant
						result[v_id]['CHROM'] = data_fixed['CHROM']
						result[v_id]['POS'] = int(data_fixed['POS'])
						result[v_id]['ID'] = data_fixed['ID']
						result[v_id]['REF'] = data_fixed['REF']
						result[v_id]['ALT'] = data_fixed['ALT']

						if data_fixed['ID'].startswith('rs'):
							result[v_id]['dbSNP_ID'] = data_fixed['ID']
						else:
						 	result[v_id]['dbSNP_ID'] = None # boolean filters can not use 'NA'

						# QUAL and FILTER field
						result[v_id]['QUAL' + group] = float(data_fixed['QUAL'])
						result[v_id]['FILTER' + group] = data_fixed['FILTER']

						if data_fixed['REF'] in ['G','A','T','C'] and data_fixed['ALT'] in ['G','A','T','C']:
							result[v_id]['VariantType'] = 'SNV'
						else:
							result[v_id]['VariantType'] = 'INDEL'

						# parse INFO field
						info_fields = data_dict[group][v_id][7].split(";")
						result_info = parse_info_fields(info_fields, tmp, log, vcf_info, group)
						result[v_id].update(result_info)

						# parse FORMAT field
						format_fields = data_dict[group][v_id][8].split(":")

						# parse sample related data
						sample_info = dict(zip(vcf_info['col_header'][9:], data_dict[group][v_id][9:]))
						result_sample = parse_sample_info(tmp2, format_fields, sample_info, log, vcf_info, group=group)
						result[v_id].update(result_sample)

			for v_id in result:
				json.dump({"_index" : index_name, "_type" : type_name, "_source": result[v_id]}, f, ensure_ascii=True)
				f.write("\n")

		print("Pid %s: finished processing %s, batch %d of %d" % (p.pid, batch, batch_count, total_batches))

def make_es_mapping(vcf_info):
	info_dict2 = vcf_info['info_dict']
	format_dict2 = vcf_info['format_dict']

	mapping = defaultdict()
	mapping[type_name] = {}
	mapping[type_name]["properties"] = {}
	p = re.compile(r'snp\d+')

	if annot == 'vep':
		csq_dict_global =vcf_info['csq_dict_global']
		csq_dict_local = vcf_info['csq_dict_local']
		vcf_info['csq_dict_local'].update({'SIFT_pred' : {"type" : "keyword", "null_value" : 'NA'}})
		vcf_info['csq_dict_local'].update({'SIFT_score' : {"type" : "float", "null_value" : -999.99}})
		vcf_info['csq_dict_local'].update({'PolyPhen_pred' : {"type" : "keyword", "null_value" : 'NA'}})
		vcf_info['csq_dict_local'].update({'PolyPhen_score' : {"type" : "float", "null_value" : -999.99}})
		if 'PolyPhen' in csq_dict_local:
			del csq_dict_local['PolyPhen']
		if 'SIFT' in csq_dict_local:
			del csq_dict_local['SIFT']
		if 'SOMATIC' in csq_dict_global:
			del csq_dict_global['SOMATIC']
		excluded_list.append('CSQ')

		# define mapping for other variables
		for key in csq_dict_local:
			if key.endswith('score'):
				csq_dict_local[key] = {"type" : "float", "null_value" : -999.99}
			else:
				csq_dict_local[key] =  {"type" : "keyword", "null_value" : 'NA'}
		for key in csq_dict_global:
			if key.startswith('CADD') or key.endswith('score') or key.endswith('_AF') or key == 'AF':
				csq_dict_global[key] = {"type" : "float", "null_value" : -999.99}
			else:
				csq_dict_global[key] = {"type" : "keyword", "null_value" : 'NA'}

		csq_annot = {"type" : "nested", "properties" : csq_dict_local} #vcf_info['csq_dict_local']}
		mapping[type_name]["properties"]["CSQ_nested"] = csq_annot

		# variables used for boolean type need to have None value if empty
		mapping[type_name]["properties"].update({"COSMIC_ID" : {"type" : "keyword"}, "dbSNP_ID" : {"type" : "keyword"}})
		if 'Existing_variation' in csq_dict_global:
			del csq_dict_global['Existing_variation']
		mapping[type_name]["properties"].update(csq_dict_global)

	elif annot == 'annovar':
		ensGene_dict = {"EnsembleTranscriptID" : {"type" : "keyword", "null_value" : 'NA'}}
		ensGene_dict.update({"exon_id_eg" : {"type" : "keyword", "null_value" : 'NA'}})
		ensGene_dict.update({"cdna_change_eg" : {"type" : "keyword", "null_value" : 'NA'}})
		ensGene_dict.update({"aa_change_eg" : {"type" : "keyword", "null_value" : 'NA'}})
		refGene_dict = {"RefSeq" : {"type" : "keyword", "null_value" : 'NA'}}
		refGene_dict.update({"exon_id_rg" : {"type" : "keyword", "null_value" : 'NA'}})
		refGene_dict.update({"cdna_change_rg" : {"type" : "keyword", "null_value" : 'NA'}})
		refGene_dict.update({"aa_change_rg" : {"type" : "keyword", "null_value" : 'NA'}})
		refGene_annot = {"type" : "nested", "properties" : refGene_dict}
		ensGene_annot = {"type" : "nested", "properties" : ensGene_dict}
		mapping[type_name]["properties"]['AAChange_refGene'] = refGene_annot
		mapping[type_name]["properties"]['AAChange_ensGene'] = ensGene_annot

		mapping[type_name]["properties"].update({"COSMIC_Occurrence" : {"type" : "keyword", "null_value" : 'NA'}})

		excluded_list.append('AAChange_refGene')
		excluded_list.append('AAChange_ensGene')
		excluded_list.append('ANNOVAR_DATE')

	tmp_keys = info_dict2.keys()

	for key in tmp_keys:
		if 'Description' in info_dict2[key]:
			del info_dict2[key]['Description']
		if '++' in key:
			tmp = key.replace('++', 'plusplus')
			info_dict2[tmp] = info_dict2[key]
			del info_dict2[key]
	for key in format_dict2:
		if 'Description' in format_dict2[key]:
			del format_dict2[key]['Description']

	keys = [key	for key in info_dict2]
	keys = [x for x in keys if (x in utils.SUMMARY_STATISTICS_FIELDS or x in utils.VARIANT_QUALITY_RELATED_FIELDS) and x not in excluded_list]

	# Perhaps we have to hand made a list of attributes that are meaningful to have "_case" and "_control" appended
	if control_vcf:
		for key in keys:

			info_dict2[key + '_case'] = info_dict2[key]
			info_dict2[key + '_control'] = info_dict2[key]
			print("Problem! %s\n" % key)
			del info_dict2[key]
			format_dict2.update({'group' : {"type" : "keyword"}})
		# add QUAL and FILTER
		info_dict2['QUAL_case'] = {"type" : "float"}
		info_dict2['QUAL_control'] = {"type" : "float"}
		info_dict2['FILTER_case'] = {"type" : "keyword"}
		info_dict2['FILTER_control'] = {"type" : "keyword"}

	for key in excluded_list:
		if key in info_dict2:
			del info_dict2[key]

	# add null_value tags:
	for key in info_dict2:
		if info_dict2[key]['type'] == 'integer':
			if key == 'CIPOS' or key == 'CIEND':
				info_dict2[key] = { "type" : "keyword", "null_value" : 'NA'}
			else:
				info_dict2[key]["null_value"] = -999
		elif info_dict2[key]['type'] == 'float':
			info_dict2[key]["null_value"] = -999.99
		elif info_dict2[key]['type'] == 'flag':
			info_dict2[key]['type'] = 'keyword'
			info_dict2[key]["null_value"] = 'No'
		else:
			if key in ['dbSNP_ID', 'COSMIC_ID', 'snp138NonFlagged'] or re.search(p, key):
 				info_dict2[key]= {'type' : 'keyword'}
			else:
				info_dict2[key]= {'type' : 'keyword', 'null_value' : 'NA'}

	info_dict2['Variant'] = {"type" : "keyword"}
	info_dict2['VariantType'] = {"type" : "keyword"}

	for key in format_dict2:
		if format_dict2[key]['type'] == 'string':
			format_dict2[key] = {'type' : 'keyword', "null_value" : 'NA'}
		elif format_dict2[key]['type'] == 'integer':
			format_dict2[key]["null_value"]  = -999
		elif format_dict2[key]['type'] == 'float':
			format_dict2[key]["null_value"]  = -999.99

	format_dict2['Sample_ID'] =  {'type' : 'keyword'}
	if 'AD' in format_dict2:
		format_dict2.update({'AD_ref' : {"type" : "integer", "null_value" : -999}})
		format_dict2.update({'AD_alt' : {"type" : "integer", "null_value" : -999}})
		del format_dict2['AD']

	if ped:
		format_dict2.update({'Family_ID' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Father_ID' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Mother_ID' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Sex' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Phenotype' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Father_Phenotype' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Mother_Phenotype' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Father_Genotype' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Mother_Genotype' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'Mother_Genotype' : {'type' : 'keyword', 'null_value' : 'NA'}})
		format_dict2.update({'mendelian_diseases' : {'type' : 'keyword'}})

	# first 7 columns
	fixed_dict = {"CHROM" : {"type" : "keyword"}, "ID" : {"type" : "keyword", "null_value" : "NA"}, "POS" : {"type" : "integer"},
				"REF" : {"type" : "keyword"}, "ALT" : {"type" : "keyword"}, "FILTER" : {"type" : "keyword"}, "QUAL" : {"type" : "float"}}
	if control_vcf:
		fixed_dict = {"CHROM" : {"type" : "keyword"}, "ID" : {"type" : "keyword", "null_value" : "NA"}, "POS" : {"type" : "integer"},
				"REF" : {"type" : "keyword"}, "ALT" : {"type" : "keyword"}}
	mapping[type_name]["properties"].update(fixed_dict)
	mapping[type_name]["properties"].update(info_dict2)


	mapping[type_name]["properties"]["sample"] = {}
	sample_annot = {"type" : "nested", "properties" : format_dict2}
	mapping[type_name]["properties"]["sample"].update(sample_annot)

	#remove features that have been appended with  '_case' and '_control'
	case_control_features = [key for key in mapping[type_name]["properties"] if key.endswith('_control')]
	features_to_remove = [re.sub('_control', '', item) for item in case_control_features]
	mapping[type_name]["properties"] = {key:val for key, val in mapping[type_name]["properties"].items() if key not in features_to_remove }


	index_settings = {}
	index_settings["settings"] = {
		"number_of_shards": 7,
		"number_of_replicas": 1,
		"refresh_interval": "1s",
		"index.mapping.ignore_malformed": True,
		"index.write.wait_for_active_shards": 1,
		"index.mapping.ignore_malformed": "true",
		"index.merge.policy.max_merge_at_once": 7,
		"index.merge.scheduler.max_thread_count": 7,
		"index.merge.scheduler.max_merge_count": 7,
		"index.merge.policy.floor_segment": "100mb",
		"index.merge.policy.segments_per_tier": 25,
		"index.merge.policy.max_merged_segment": "10gb"
	}

	dir_path = os.path.dirname(os.path.realpath(__file__))
	create_index_script = os.path.join(dir_path,  'scripts', 'create_index_%s_and_put_mapping.sh' % index_name)
	mapping_file = os.path.join(dir_path,  'scripts', '%s_mapping.json' % index_name)

	with open(create_index_script, 'w') as fp:
		fp.write("curl -XPUT \'%s:%s/%s?pretty\' -H \'Content-Type: application/json\' -d\'\n" % (hostname, port, index_name))
		json.dump(index_settings, fp, sort_keys=True, indent=2, ensure_ascii=False)
		fp.write("\'\n")
		fp.write("curl -XPUT \'%s:%s/%s/_mapping/%s?pretty\' -H \'Content-Type: application/json\' -d\'\n" % (hostname, port, index_name, type_name))
		json.dump(mapping, fp, sort_keys=True, indent=2, ensure_ascii=False)
		fp.write("\'")

	with open(mapping_file, 'w') as fp:
		json.dump(mapping, fp, sort_keys=True, indent=2, ensure_ascii=False)

	return(create_index_script, mapping_file)



if __name__ == '__main__':
	t0 = time.time() # get program start time
	t1 = time.time()

	dir_path = os.path.dirname(os.path.realpath(__file__))
	create_index_script = os.path.join(dir_path,  'scripts', 'create_index_%s_and_put_mapping.sh' % index_name)
	mapping_file = os.path.join(dir_path,  'scripts', '%s_mapping.json' % index_name)
	out_vcf_info = os.path.basename(vcf).replace('.vcf.gz', '') + '_vcf_info.json'
	out_vcf_info = os.path.join(os.getcwd(),  'config', out_vcf_info)
	output_files = []

	es = elasticsearch.Elasticsearch( host=hostname, port=port, request_timeout=180, max_retries=10, timeout=120, read_timeout=400)
	es.cluster.health(wait_for_status='yellow')

	# append assembly version to dataset name
	dataset_name += '_' + assembly

	# make sure the destination dataset not exists
	conn = sqlite3.connect('db.sqlite3')
	c = conn.cursor()


	query = "DELETE FROM core_dataset WHERE name = '" + dataset_name + "'"
	try:
		c.execute(query)
	except Exception as e:
		print("Sqlite error: %s" % e)

	conn.commit()
	conn.close()

	if gui_only:
		gui_mapping = os.path.join("config", type_name + '_gui_config.json')
		make_gui(es, hostname, port, index_name, study, dataset_name, type_name, gui_mapping)
	else:
		case_control = False
		if control_vcf:
			case_control = True

		if not skip_parsing:
			check_commandline(vcf, control_vcf, annot)

			# read and process vcf header section to get various field names and data types
			rv = process_vcf_header(vcf)


			if annot == 'vep':
				vcf_info = dict(zip([ 'num_header_lines', 'csq_fields', 'col_header', 'chr2len', 'info_dict', 'format_dict', 'contig_dict', 'csq_dict_local', 'csq_dict_global'], rv))
			elif annot == 'annovar':
				vcf_info = dict(zip([ 'num_header_lines', 'col_header', 'chr2len', 'info_dict', 'format_dict', 'contig_dict'], rv))

			if control_vcf:
				rv2 = process_vcf_header(control_vcf)
				vcf_info2 = dict(zip([ 'num_header_lines', 'csq_fields', 'col_header', 'chr2len', 'info_dict', 'format_dict', 'contig_dict', 'csq_dict_local', 'csq_dict_global'], rv2))
				vcf_info['info_dict'] = {**vcf_info['info_dict'], **vcf_info2['info_dict']}

			# read 5000 lines of data to verify data types for each field extracted from vcf header by the above function
			vcf_info = process_vcf_data(vcf, 5000, vcf_info)


			with open(out_vcf_info, 'w') as f:
				json.dump(vcf_info, f, sort_keys=True, indent=4, ensure_ascii=True)

			# insert pedegree data if ped file is specified
			if ped:
				ped_info = process_ped_file(ped)
				vcf_info['ped_info'] = ped_info

			# determine which work flow to choose, i.e. single cohort or case-control analysis
			if control_vcf:
				output_files = process_case_control(vcf, control_vcf, vcf_info)
			else:
				output_files = process_single_cohort(vcf, vcf_info)

			t1 = time.time()
			total = t1-t0

			print("Finished parsing vcf file in %s seconds, now creating ElasticSearch index ..." % total)


			create_index_script, mapping_file = make_es_mapping(vcf_info)

		else:

			for i in range(num_cpus):
				output_file = os.path.join(tmp_dir, os.path.basename(vcf) + '.chunk_' + str(i) + '.json')
				output_files.append(output_file)


		# prepare for elasticsearch
		if es.indices.exists(index_name):
			print("deleting '%s' index..." % index_name)
			res = es.indices.delete(index = index_name)
			print("response: '%s'" % res)

		print("creating '%s' index..." % index_name)
		res = check_output(["bash", create_index_script])
		print("Response: '%s'" % res.decode('ascii'))


		for infile in output_files:
			print("Indexing file %s" % infile)
			data = []

			with open(infile, 'r') as fp:
				for line in fp:
					tmp = json.loads(line)
					data.append(tmp)
					if len(data) % 1000 == 0:
						try:
							deque(helpers.parallel_bulk(es, data, thread_count=num_cpus, raise_on_exception=False), maxlen=0)
							data = []
						except ValueError as e:
							print("Failed indexing %s" % e)
							continue
			# leftover data
			try:
				deque(helpers.parallel_bulk(es, data, thread_count=num_cpus), maxlen=0)
			except:
				continue


		t2 = time.time()
		total1 = t2 - t1
		total2 = t2 - t0

		print("Finished creating ES index in %s seconds, total time %s seconds\n" % (total1, total2))

		#  make a gui config file
		print("Creating Web user interface, please wait ...")

		gui_mapping = make_gui_config(out_vcf_info, mapping_file, type_name, annot, case_control)


		make_gui(es, hostname, port, index_name, study, dataset_name, type_name, gui_mapping)

		print("*"*80+"\n")
		print("Successfully imported VCF file. You can now explore your data at %s:%s" % (hostname, webserver_port))

	# clean up
	if cleanup:
		for infile in output_files:
			print("Deleting %s..." % infile)
			os.remove(infile)
