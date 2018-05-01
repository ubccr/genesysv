#!/usr/bin/env python
from __future__ import print_function
import os
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
from collections import defaultdict
from collections import OrderedDict
import json
from collections import deque
#from utils import *
import elasticsearch
from collections import deque
from elasticsearch import helpers
#from es_celery.tasks import post_data, update_refresh_interval

parser = argparse.ArgumentParser(description='Parse vcf file(s) and create ElasticSearch mapping and index from the parsed data')
required = parser.add_argument_group('required named arguments')
required.add_argument("--vcf", help="Annovar or VEP annotated input vcf file. Must be compressed with bgzip and indexed with grabix", required=True)
required.add_argument("--tmp_dir", help="Temporory directory to store intermediate files", required=True)
required.add_argument("--annot", help="Type of variant consequence annotation. Valid values are 'annovar' or 'vep'", required=True)
required.add_argument("--hostname", help="ElasticSearch hostname", required=True)
required.add_argument("--port", help="ElasticSearch host port number", required=True)
required.add_argument("--index", help="ElasticSearch index name", required=True)
required.add_argument("--num_cores", help="Number of cpu cores to use. Default to the number of cpu cores of the system", required=False)
required.add_argument("--ped", help="Pedigree file in the format of '#Family Subject Father  Mother  Sex     Phenotype", required=False)
required.add_argument("--control_vcf", help="vcf file from control study. Must be compressed with bgzip and indexed with grabix", required=False)
required.add_argument("--interval_size", help="Genomic interval size (bp) for loading case/control vcf. Default is 5000000. Choose a smaller number if low in physical memory", required=False)
parser.add_argument("--debug", help="Run in single CPU mode for debugging purposes")
	
args = parser.parse_args()

# global variables
num_cpus = args.num_cores
if (num_cpus is None):
	num_cpus = multiprocessing.cpu_count()
else:
	num_cpus = int(args.num_cores)

index_name = args.index
type_name = 'gdwtest'

excluded_list = ['MQ0', 'DB', 'POSITIVE_TRAIN_SITE', 'NEGATIVE_TRAIN_SITE']

def check_commandline(args):
	# check if valid annotation type is specified
	annot = args.annot
	if annot == 'vep':
		annot_type = 'vep'
	elif annot == 'annovar':
		annot_type = 'annovar'
	else:
		print("Unsupported annotation type: %s" % annot)
		sys.exit(2)

	# check if valid vcf file specified
	vcf = os.path.abspath(args.vcf)
	out = check_output(["grabix", "check", vcf])

	if str(out.decode('ascii').strip()) != 'yes':
		print("Invalid vcf file. Please provide a bgzipped/grabix indexed vcf file.")
		sys.exit(2)
	
	if args.control_vcf:
		control_vcf = os.path.abspath(args.control_vcf)
		out = check_output(["grabix", "check", control_vcf])
	
		if str(out.decode('ascii').strip()) != 'yes':
			print("Invalid control_vcf file. Please provide a bgzipped/grabix indexed vcf file.")
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
	# declare some global variables

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
			if 'GQ_MEAN' in line:
				print
			if id_:
				if type_:
					if desc_:
						if line.startswith('##INFO'):
							info_dict[id_.group(1)] = {'type' : type_.group(1).lower(), 'Description' : desc_.group(1)}
						elif line.startswith('##FORMAT'):
							if id_.group(1) == 'PL': # make this as sting type
								format_dict[id_.group(1)] = {'type' : "string", 'Description' : desc_.group(1)}
							else:
								format_dict[id_.group(1)] = {'type' : type_.group(1).lower(), 'Description' : desc_.group(1)}
						else:
							pass
							#print("Should not reach here %s" % line)
					else:
						pass
						print("header1 %s", line)

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
	
	# get chromosome length
	valid_chrs = range(1, 23)
	valid_chrs = [str(item) for item in valid_chrs ]
	valid_chrs.append('X')
	valid_chrs.append('Y')
	valid_chrs.append('M')
	
	for key in contig_dict:
		if key in valid_chrs:
			chr2len[key] = int(contig_dict[key]['length'])

	return([num_header_lines, csq_fields, col_header, chr2len, info_dict, format_dict, contig_dict, csq_dict])

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
					output = output.decode('ascii')
				except ValueError:
					continue # need to check
				# remove the header lines from output
				lines = output.splitlines()
				variant_lines = lines[vcf_info['num_header_lines']:]
			
				process_line_data(variant_lines, log, f, vcf_info)
				
				num_variants_processed += end - start + 1
				
				# update start and end positions
				start += chunk_size
				
				print("Pid %s: processed %d variants" % (p.pid, num_variants_processed))
			
				if end >= interval[1]:
					break
	
def parse_info_fields(info_fields, result, log, vcf_info, group = ''):
	p = re.compile(r'^(.*?)\((.*)\)') # parsing SIFT and PolyPhen predition and score
	aac_dict = {}

	for info in info_fields:
		try:
			key, val = info.split('=')
		except ValueError:
			log.write("Single value: %s\n" % info)
			continue

		if key in excluded_list:
			continue

		elif key == 'CSQ':
			csq_list = []
			info_csq = val.split(',')

			for csq in info_csq:
				csq2	= csq.split('|')
				csq_dict2 = dict(zip(vcf_info['csq_fields'], csq2)) # map names to values for CSQ annotation sub-fields

				tmp_dict = {}
					
				for key2, val2 in csq_dict2.items():
					if '&' in val2:
						val2 = val2.split('&')[0] # to deal with situations like "AF, 0.1860&0.0423"
					if '&' in key2:
						key2 = key2.split('&')[0]

					if key2 in ['SIFT', 'PolyPhen']:
						m = p.match(val2)
						if m:
							tmp_dict[key2 + '_pred'] = m.group(1)
							tmp_dict[key2 + '_score'] = float(m.group(2))
						else: # only pred or score are included in vep annotation
							if isinstance(val2, float):
								tmp_dict[key] = float(val2)
							else:
								tmp_dict[key] = val2
					elif vcf_info['csq_dict'][key2]['type'] == 'integer':
						if val2 == '':
							csq_dict2[key2] = -999
						else:
							try:
									csq_dict2[key2] = int(csq_dict2[key2])
							except ValueError:
								log.write("Cating to int error: %s" % info)
					elif vcf_info['csq_dict'][key2]['type'] == 'float':
						if val2 == '':
							csq_dict2[key2] = -999.99
						else:
							try:
								csq_dict2[key2] = float(val2)
							except ValueError:
								log.write("casting to float error:  %s, %s\n" % (key2, val2))
				
				del csq_dict2['SIFT']
				del csq_dict2['PolyPhen']
				csq_dict2.update(tmp_dict)	
				csq_list.append(csq_dict2)

				# booleans
				if csq_dict2['Existing_variation'] is not None:
					if 'rs' in csq_dict2['Existing_variation']:
						result['in_dbsnp'] = True

					if 'COSM' in csq_dict2['Existing_variation']:
						result['in_cosmic'] = True

			result['CSQ_nested'] = (csq_list)
		else:
			if args.annot == 'annovar':
				aac_list = []

				if key == 'AAChange.refGene':
					if val == '.' or val == 'UNKNOWN':
						aac_dict['RefSeq'] = 'NA'
						aac_dict['exon_id'] = 'NA'
						aac_dict['cdna_change'] = 'NA'
						aac_dict['aa_change'] = 'NA'
					else:
						val_list = val.split(',')
						for subval in val_list:
							
							gene, refseq, exon, *cdna_aa = subval.split(':')
							aac_dict['RefSeq'] = refseq
							aac_dict['exon_id'] = exon
							if len(cdna_aa) == 2:
								aac_dict['cdna_change'] = cdna_aa[0]
								aac_dict['aa_change'] = cdna_aa[1]
							else:
								aac_dict['cdna_change'] = 'NA'
								aac_dict['aa_change'] = 'NA'
					aac_list.append(aac_dict)
					result[key] = aac_list

				elif key == 'AAChange.ensGene':		
					if val == '.' or val == 'UNKNOWN':
						aac_dict['EnsembleTranscriptID'] = 'NA'
						aac_dict['exon_id'] = 'NA'
						aac_dict['cdna_change'] = 'NA'
						aac_dict['aa_change'] = 'NA'
					else:
						val_list = val.split(',')
						for subval in val_list:
							gene, transcript, exon, *cdna_aa = subval.split(':')
							aac_dict['EnsembleTranscriptID'] = transcript
							aac_dict['exon_id'] = exon
							if len(cdna_aa) == 2:
								aac_dict['cdna_change'] = cdna_aa[0]
								aac_dict['aa_change'] = cdna_aa[1]
							else:
								aac_dict['cdna_change'] = 'NA'
								aac_dict['aa_change'] = 'NA'

					aac_list.append(aac_dict)
					result[key] = aac_list

			try:
				if vcf_info['info_dict'][key]['type'] == 'integer': 
					if val == '.':
						val = -999
					if key == 'CIPOS' or key == 'CIEND':
						result[key + group] = val # keep as is (i.e. string type)
					else:
						if isinstance(val, int):
							result[key + group] = int(val)
						else:
							result[key + group] = val
				elif vcf_info['info_dict'][key]['type'] == 'float': # key in name2float:
					if val == '.':
	 					val = -999.99
					else:
						if isinstance(val, float):
							result[key + group] = float(val)
						else:
							result[key + group] = val
				else: # string type
					if val =='.':
						val = 'NA'
					result[key + group] = val
			except KeyError:
				log.write("Key %s not found" % key)
				continue

			if key == 'ID' and val.startswith('rs'): 
				result['in_dbsnp'] = True
			if 'COSMIC_ID' in key and val !='.':
				result['in_cosmic'] = True

	return(result)

def parse_sample_info(result, format_fields, sample_info, log, vcf_info, group = ''):
	sample_data_array = []
	
	for sample_id, sample_data in sample_info.items():
		sample_data_dict = {}
		
		if sample_data.startswith('.') or sample_data.startswith('./.') or sample_data.startswith('0/0') or sample_data.startswith('0|0'):
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
				else:
					log.write("Unknown type: %s, %s\n" % (key, val))
					sample_data_dict[key] = val # this is the Phred scalled genotype likely hood, 10^(-log(n/10), no need to split them, just present as is
			elif key == 'DP':
				if val == '.':
					sample_data_dict[key] = -999
				else:
					sample_data_dict[key] = int(val)
			else:
				if val == '.':
					sample_data_dict[key] = 'NA'
				else:
					try:
						sample_data_dict[key] = int(val) # are they all the integer type?
					except ValueError:
						log.write("Unknown type: %s, %s\n" % (key, val))
						continue

		# add information from ped file
		if args.ped and sample_id in  vcf_info['ped_info']:
			sample_data_dict['Family_ID'] = vcf_info['ped_info'][sample_id]['family']
			sample_data_dict['Father_ID'] = vcf_info['ped_info'][sample_id]['father']
			sample_data_dict['Mother_ID'] = vcf_info['ped_info'][sample_id]['mother']
			sample_data_dict['Sex'] = vcf_info['ped_info'][sample_id]['sex']
			sample_data_dict['Phenotype'] = vcf_info['ped_info'][sample_id]['phenotype']
			
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
		result['in_cosmic'] = False
		result['in_dbsnp'] = False
		result = parse_info_fields(info_fields, result, log, vcf_info)
		
		# parse sample related data
		sample_info = dict(zip(vcf_info['col_header'][9:], col_data[9:]))

		result = parse_sample_info(result, format_fields, sample_info, log, vcf_info)

		json.dump({"_index" : index_name, "_type" : type_name, "_source" : result}, f, ensure_ascii=True)
		f.write("\n")


def process_single_cohort(vcf, vcf_info):

	# get the total number of variants in the input vcf
	out = check_output(["grabix", "size", vcf])
	total_lines = int(out.decode('ascii').strip())

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

	if args.debug:
		for intev in intervals: # debug
			output_file = 'tmp/output_' + str(intev) + '.json'
			parse_vcf(vcf, intev, output_file, vcf_info)	
			output_json.append(output_file)
	else:
 		# dispatch subtasks to each of the processes 
		for i in range(num_cpus):
			output_file = os.path.join(args.tmp_dir, os.path.basename(vcf) + '.chunk_' + str(i) + '.json')
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
	batch_size = 500000 # reduce this number if memory is an issue
	if args.interval_size:
		batch_size = args.interval_size

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
				if (end >= length):
					break

	# calculate number of batches each cpu need to process
	batches_per_cpu = math.ceil(len(batch_list)/num_cpus)
	batch_start = 0

	if args.debug:
		output_file = 'tmp/output_case_control_' + str(batch_list[0]) + '.json'
		parse_case_control(case_vcf, control_vcf, [batch_list[0]], output_file, vcf_info)
		output_json.append(output_file)
	else:
		for i in range(num_cpus):
			batch_end = batch_start + batches_per_cpu
			if batch_end > len(batch_list):
				batch_end = len(batch_list)

			output_file = os.path.join(args.tmp_dir, os.path.basename(control_vcf) + '.chunk_' + str(i) + '.json')
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
	data_dict = defaultdict()
	data_dict['_case'] = {}
	data_dict['_control'] = {}

	p = multiprocessing.current_process()

	logfile = re.sub('json', 'log', outfile)
	log = open(logfile, 'w')
	
	batch_count = 0
	total_batches = len(batch_sub_list)

	with open(outfile, 'w') as f:
		for batch in batch_sub_list:
			batch_count += 1

			print("Pid %s processing batch  %s, %d of %d"% (p.pid, batch, batch_count, total_batches))

	 		# get a chunck of line from each of the vcf files
			output_case = check_output(["tabix", case_vcf, batch])
			output_control = check_output(["tabix", control_vcf, batch])

			if len(output_case) + len(output_control) == 0:
				print("Empty batch %s" % batch)
				continue
			
			output_case = output_case.decode('ascii')
			output_control = output_control.decode('ascii')

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
					
					# make a short format of variant IDs, i.e. keep at most 9 bases for indels
					variant = '_'.join([data_fixed['CHROM'], data_fixed['POS'], data_fixed['REF'][:10], data_fixed['ALT'][:10]])
	
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
						result[v_id] = {}
						result[v_id]['Variant'] = variant
						result[v_id]['CHROM'] = data_fixed['CHROM']
						result[v_id]['POS'] = int(data_fixed['POS'])
						result[v_id]['ID'] = data_fixed['ID']
						result[v_id]['REF'] = data_fixed['REF']
						result[v_id]['ALT'] = data_fixed['ALT']
	
						# QUAL and FILTER field
						result[v_id]['QUAL' + group] = float(data_fixed['QUAL'])
						result[v_id]['FILTER' + group] = data_fixed['FILTER']
	
						if data_fixed['ID'].startswith('rs'): # should also check the INFO field
							result[v_id]['in_dbsnp'] = True
		
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

	for key in info_dict2:
		if 'Description' in info_dict2[key]:
			del info_dict2[key]['Description']
	for key in format_dict2:
		if 'Description' in format_dict2[key]:
			del format_dict2[key]['Description']

	keys = [key	for key in info_dict2]
	excluded_list.append('CSQ')
	keys = [x for x in keys if x not in excluded_list]

	if args.control_vcf:
		for key in keys:
			info_dict2[key + '_case'] = info_dict2[key]
			info_dict2[key + '_control'] = info_dict2[key]
			del info_dict2[key]
			format_dict2.update({'group' : {"type" : "keyword"}})

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
		elif info_dict2[key]['type'] == 'string':
			info_dict2[key]= {'type' : 'keyword', 'null_value' : 'NA'}
		elif info_dict2[key]['type'] == 'boolean':
			info_dict2[key]["null_value"] = 'false'
		else:
			info_dict2[key]["type"] = 'text'

	for key in format_dict2:
		if format_dict2[key]['type'] == 'string':
			format_dict2[key] = {'type' : 'keyword', "null_value" : 'NA'}
		elif format_dict2[key]['type'] == 'integer':
			format_dict2[key]["null_value"]  = -999
		elif format_dict2[key]['type'] == 'float':
			format_dict2[key]["null_value"]  = -999.99

	# update the field list to include the expanded fields
	if args.annot == 'vep':
		vcf_info['csq_dict'].update({'SIFT_pred' : {"type" : "keyword", "null_value" : 'NA'}})
		vcf_info['csq_dict'].update({'SIFT_score' : {"type" : "float", "null_value" : -999.99}})
		vcf_info['csq_dict'].update({'PolyPhen_pred' : {"type" : "keyword", "null_value" : 'NA'}})
		vcf_info['csq_dict'].update({'PolyPhen_score' : {"type" : "float", "null_value" : -999.99}})
		del vcf_info['csq_dict']['PolyPhen']
		del vcf_info['csq_dict']['SIFT']
	elif args.annot == 'annovar':
		info_dict2["annovar_nested"] = {"EnsembleTranscriptID" : {"type" : "keyword", "null_value" : 'NA'}}
		info_dict2["annovar_nested"].update({"exon_id" : {"type" : "keyword", "null_value" : 'NA'}})
		info_dict2["annovar_nested"].update({"cdna_change" : {"type" : "keyword", "null_value" : 'NA'}})
		info_dict2["annovar_nested"].update({"aa_change" : {"type" : "keyword", "null_value" : 'NA'}})
		
		if 'ANNOVAR_DATE' in info_dict2:
			del info_dict2['ANNOVAR_DATE']
	
	info_dict2.update({"in_dbsnp": {"type" : "boolean", 'null_value' : "false"}})
	info_dict2.update({"in_cosmic": {"type" : "boolean", 'null_value' : "false"}})

	format_dict2.update({'AD_ref' : {"type" : "integer", "null_value" : -999}})
	format_dict2.update({'AD_alt' : {"type" : "integer", "null_value" : -999}})
	if 'AD' in format_dict2:
		del format_dict2['AD']
	
	mapping = defaultdict()
	mapping[type_name] = {}
	mapping[type_name]["properties"] = {}
	mapping[type_name]["properties"].update(info_dict2)
	mapping[type_name]["properties"]["csq_annotation"] = {}
	csq_annot = {"type" : "nested", "properties" : vcf_info['csq_dict']}
	mapping[type_name]["properties"]["csq_annotation"].update(csq_annot)
	mapping[type_name]["properties"]["sample_info"] = {}
	sample_annot = {"type" : "nested", "properties" : format_dict2}
	mapping[type_name]["properties"]["sample_info"].update(sample_annot) 

	with open("my_mapping.json", 'w') as f:
		json.dump(mapping, f, sort_keys=True, indent=4, ensure_ascii=True)


if __name__ == '__main__':
	check_commandline(args)

	rv = process_vcf_header(args.vcf)
	vcf_info = dict(zip([ 'num_header_lines', 'csq_fields', 'col_header', 'chr2len', 'info_dict', 'format_dict', 'contig_dict', 'csq_dict'], rv))

	if args.control_vcf:
		rv2 = process_vcf_header(args.control_vcf)
		vcf_info2 = dict(zip([ 'num_header_lines', 'csq_fields', 'col_header', 'chr2len', 'info_dict', 'format_dict', 'contig_dict', 'csq_dict'], rv2)) 
		vcf_info['info_dict'] = {**vcf_info['info_dict'], **vcf_info2['info_dict']}

	es = elasticsearch.Elasticsearch( host=args.hostname, port=args.port, request_timeout=180)


	# prepare for elasticsearch 
	if es.indices.exists(index_name):
		print("deleting '%s' index..." % index_name)
		res = es.indices.delete(index = index_name)
		print("response: '%s'" % res)

	print("creating '%s' index..." % index_name)
	res = es.indices.create(index = index_name)
	print("response: '%s'" % res)

	if args.ped:
		ped_info = process_ped_file(args.ped)
		vcf_info['ped_info'] = ped_info

	# determine which work flow to choose, i.e. single cohort or case-control analysis
	if args.control_vcf:
		output_files = process_case_control(args.vcf, args.control_vcf, vcf_info)
	else:
		output_files = process_single_cohort(args.vcf, vcf_info)


	print("Finished parsing vcf file, now creating ElasticSearch index ...")

	make_es_mapping(vcf_info)
	mapping = json.load(open("my_mapping.json", 'r'))
	es.indices.put_mapping(index=index_name, doc_type=type_name, body=mapping)
	for infile in output_files:
		print("Indexing file %s" % infile)
		data = []

		with open(infile, 'r') as fp:
			for line in fp:
				tmp = json.loads(line)
				data.append(tmp)
				if len(data) % 5000 == 0:
					deque(helpers.parallel_bulk(es, data, thread_count=num_cpus), maxlen=0)
					data = []

		# leftover data
		deque(helpers.parallel_bulk(es, data, thread_count=num_cpus), maxlen=0)

	print("Finished creating ES index\n")	
		
