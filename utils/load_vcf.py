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
from collections import OrderedDict
from collections import defaultdict
import json
from utils import *
import elasticsearch
from elasticsearch import helpers
from es_celery.tasks import post_data, update_refresh_interval


parser = argparse.ArgumentParser()
required = parser.add_argument_group('required named arguments')
required.add_argument("--vcf", help="Annovar or VEP annotated input vcf file. Must be compressed with bgzip", required=True)
required.add_argument("--num_cores", help="Number of cpu cores to use. Default to the number of cpu cores of the system", required=True)
required.add_argument("--tmp_dir", help="Temporory directory to store intermediate files", required=True)
required.add_argument("--annot", help="Type of variant consequence annotation. Valid values are 'annovar' or 'vep'", required=True)
required.add_argument("--mapping", help="ElasticSearch mapping file", required=True)
required.add_argument("--hostname", help="ElasticSearch hostname", required=True)
required.add_argument("--port", help="ElasticSearch host port number", required=True)
required.add_argument("--index", help="ElasticSearch index name", required=True)

args = parser.parse_args()

	
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
	print("Invalid vcf file. Please provide a bgzipped vcf file.")
	sys.exit(2)

num_cpus = int(args.num_cores)
if (num_cpus == None):
	num_cpus = multiprocessing.cpu_count()

mapping_file = args.mapping

es = elasticsearch.Elasticsearch( host=args.hostname, port=args.port, request_timeout=180)
index_name = args.index
type_name = index_name # use the same name since only one type is allowed
	        


def get_mapping(mapping_file):
	global name2type

	name2type = {}

	vcf_mapping = json.load(open(mapping_file, 'r'))

	format_fields = vcf_mapping.get('FORMAT_FIELDS').get('nested_fields')
	fixed_fields = vcf_mapping.get('FIXED_FIELDS')
	info_fields = vcf_mapping.get('INFO_FIELDS')

	name2type['int'] = set([key for key in format_fields.keys() if format_fields[key].get('es_field_datatype') == 'integer'])
	name2type['int'].update(set([key for key in info_fields.keys() if info_fields[key].get('es_field_datatype') == 'integer']))
	name2type['int'].update(set([key for key in fixed_fields.keys() if fixed_fields[key].get('es_field_datatype') == 'integer']))
	
	name2type['float'] = set([key for key in format_fields.keys() if format_fields[key].get('es_field_datatype') == 'float'])
	name2type['float'].update(set([key for key in info_fields.keys() if info_fields[key].get('es_field_datatype') == 'float']))
	name2type['float'].update(set([key for key in fixed_fields.keys() if fixed_fields[key].get('es_field_datatype') == 'float']))


def process_vcf_header(vcf):
	# declare some global variables
	global num_header_lines
	global id2type
	global csq_fields
	global col_header
	global assembly

	num_header_lines = 0
	id2type = {}
	csq_fields = []
	col_header = []
	
	# compile some patterns
	p1 = re.compile('^##(.*?)=(.*)$')	
	p2 = re.compile(r'<(.*)\">')
	p3 = re.compile(r'^ID=(.*?),')
	p4 = re.compile(r'.*Number=(.*?),?.*$')
	p5 = re.compile(r'.*Type=(.*?),')
	p6 = re.compile(r'.*Description=\"(.*?)\",?.*')
	
	with gzip.open(vcf, 'rt') as fp:
		while True:
			line = fp.readline()
			if line.startswith('#CHROM'):
				col_header = line.strip().split("\t")
				col_header[0] = re.sub('#', '', col_header[0])
				num_header_lines += 1
				break

			num_header_lines += 1
			m1 = p1.match(line)
			attr, value = m1.group(1), m1.group(2)
			m2 = p2.match(value)
			
			if m2:
				value = m2.group(1) # nested key=value pairs, delimited by comma's
				if value.startswith('ID=CSQ'):
					csq_fields = value.split("|")
					csq_fields[0] = re.sub('ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: ', '', csq_fields[0])
					
					# prefix with "CSQ_" for each field name
					csq_fields = ['CSQ_' + field for field in csq_fields]
				else:	
					m3 = p3.match(value)
					m4 = p4.match(value)
					m5 = p5.match(value)
					m6 = p6.match(value)
			
					if m3 and m5:
						if m5.group(1) in ['Integer', 'String', 'Float']:
							id2type[m3.group(1)] = m5.group(1)
						else:
							continue
					else:
						continue
				 
			else:
				print(value)
				if 'assembly' in value:
					assembly = value.split('assembly=')[1]
					assembly = re.sub(r'\>', '', assembly)
				continue

def parse_vcf(vcf, interval):

	# declare a list to hold output json files
	global output_json
	output_json = []
	
	p = multiprocessing.current_process()

	# divide interval into smaller chunks to minimize memory footprint
	chunk_size = 1000
	start = interval[0]
	num_variants_processed = 0

	outfile = re.sub('.vcf.gz', '_chunk_' + str(p.pid) + '.json', vcf)
	logfile = re.sub('json', 'log', outfile)
	
	# open log file
	log = open(logfile, 'w')
	
	with open(outfile, 'w') as f:
		while True:	
			if start < interval[1]:
				end = start + chunk_size - 1
				if end >= interval[1]:
					end = interval[1]
				command = ["grabix", "grab", vcf, str(start), str(end)]
				output = check_output(command)
				output = output.decode('ascii')
	
				# remove the header lines from output
				lines = output.splitlines()
				variant_lines = lines[num_header_lines:]
			
				if annot == 'vep':
					result = process_line_data_vep(variant_lines, log)
				elif annot == 'annovr':
					result = process_line_data_annovar(variant_lines, log)
	
				json.dump(result, f, sort_keys=False, ensure_ascii=True)
				f.write('\n')
				
				num_variants_processed += end - start + 1
				
				# update start and end positions
				start += chunk_size
				
				print("Pid %s: processed %d variants" % (p.pid, num_variants_processed))
			
				if end >= interval[1]:
					break
	
		# save output file name and path
		output_json.append(outfile)
	
# TO DO: count number of parsed variants, skipped or failed
# write to log files

def process_line_data_vep(variant_lines, log):
	result = OrderedDict()
	
	for line in variant_lines:
		col_data = line.strip().split("\t")
		data_fixed = dict(zip(col_header[:6], col_data[:6]))					

		# in the first 8 field of vcf format, POS and QUAL are of non-string, so convert them to the right type
		data_fixed['POS'] = int(data_fixed['POS'])
		data_fixed['QUAL'] = float(data_fixed['QUAL'])

		result['Variant'] = '%s-%d-%s-%s' % (data_fixed['CHROM'], data_fixed['POS'], data_fixed['REF'][:10], data_fixed['ALT'][:10])
		result.update(data_fixed)

		# get variant type
		if data_fixed['REF'] in ['G','A','T','C'] and data_fixed['ALT'] in ['G','A','T','C']:
			result['VariantType'] = 'SNV'
		else:
			result['VariantType'] = 'INDEL'
			
		# parse INFO field
		if ';' in col_data[7]:
			info_fields = col_data[7].split(";") 
		else:
			log.write("Variant skipped: %s\n" % line)
			continue

		# parse FORMAT field
		if ':' in col_data[8]:
			format_fields = col_data[8].split(":")
		else:
			format_fields = 'GT' # 1000 Genomes Project used a single GT field
		
		# parse INFO field
		csq_annot = []
		
		for info in info_fields:
			if info.startswith('CSQ='):
				info_csq = info.split(',')
				for csq in info_csq:
					csq2	= csq.split('|')
					csq2[0] = re.sub('CSQ=', '', csq2[0]) # first csq item contains 'CSQ=', so remove it
					csq_dict = dict(zip(csq_fields, csq2)) # map names to values for CSQ annotation sub-fields

					# set missing values
					csq_dict = {key:None for (key, val) in csq_dict.items() if val == ''}
				
					# set missing MAF values to zero 
					for (key, val) in csq_dict.items():
						if key.endswith('_AF') and val is None:
							csq_dict[key] = 0.0
							
					# booleans
					if csq_dict['Existing_variation']:
						result['in_dbsnp'] == True
					else:
						result['in_dbsnp'] == False

					# TO DO: handle missing int values

					csq_annot.append(csq_dict)	
			else: # general annotation in INFO line, not specific to VEP
				if '=' in info:
					key, val = info.split('=')

					# type conversion
					if key in name2type['int']:
						result[key] = int(val)
					elif key in name2type['float']:
						result[key] = float(val)
				elif info in ['DB', 'POSITIVE_TRAIN_SITE', 'NEGATIVE_TRAIN_SITE']:
					continue
				else:
					log.write("INFO skipped: %s\n" % info)
					continue
		
		result['CSQ_nested'] = csq_annot
		
		# parse sample related data
		sample_info = dict(zip(col_header[9:], col_data[9:]))

		# parse sample data according to the FORMAT field
		sample_data_array = []
		
		for sample_id, sample_data in sample_info.items():
			sample_data_dict = {}
			
			if sample_data.startswith('.') or sample_data.startswith('./.') or sample_data.startswith('0/0') or sample_data.startswith('0|0'):
				continue
			if format_fields == 'GT':
				sample_data_dict['GT'] = sample_data # i.e. vcfs from 1000 Genomoes Project
			else:
				tmp = sample_data.split(':')
				sample_sub_info_dict = dict(zip(format_fields, tmp))
				
				# handle comma-delimited numeric values
				for (key, val) in sample_sub_info_dict.items():
					if key in ['GT', 'PGT', 'PID']:
						sample_data_dict[key] = val
					elif ',' in val:
						sub_items = val.split(',')
						if key == 'AD':
							sample_data_dict['AD_ref'], sample_data_dict['AD_alt'] = sub_items
							sample_data_dict['AD_ref'] = int(sample_data_dict['AD_ref'])
							sample_data_dict['AD_alt'] = int(sample_data_dict['AD_alt'])
						else:
							sample_data_dict[key] = val # unsplited values, should we care about it?
					else:
						if val == '.':
							sample_data_dict[key] = None
						else:
							try:
								sample_data_dict[key] = int(val) # are they all the integer type?
							except ValueError:
								log.write("Unknown type: %s\n" % val)
								continue

			sample_data_dict['Sample_ID'] = sample_id
			sample_data_array.append(sample_data_dict)

		result['sample'] = sample_data_array

		es_id = get_es_id(CHROM, POS, REF, ALT, ID, index_name, type_name)
		result['index'] = {"_id": es_id}

	return(result)

def populate_es_index(infile):
	post_data.delay(args.hostname, args.port, index_name, type_name, infile)
		
if __name__ == '__main__':
	get_mapping(mapping_file)

	process_vcf_header(vcf)
		
	# get the total number of variants in the input vcf
	out = check_output(["grabix", "size", vcf])
	total_lines = int(out.decode('ascii').strip())

	# calculate number of variants each cpu core need to process
	num_lines_per_proc = math.ceil(total_lines/num_cpus)
	
		
	# create a intervals list for distributing variants into each of the processes 
	intervals = []
	line_start = 1
	line_end = num_lines_per_proc + line_start

	while True:
		if (line_start < total_lines):
			line_end = line_start + num_lines_per_proc
			if line_end >= total_lines:
				line_end = total_lines
	
			interval = [line_start, line_end]
			intervals.append(interval)
			line_start = line_end + 1
			if (line_end >= total_lines):
				break
				
	# to be used to hold the process ids for the join() function
	processes = []

#parse_vcf(vcf, intervals[0])	
	
 	# dispatch subtasks to each of the processes. 
 	for i in range(num_cpus):
 		output_file = vcf + '.chunk_' + str(i)
 		proc = multiprocessing.Process(target=parse_vcf, args=[vcf, intervals[i],])
 		proc.start()
 		processes.append(proc)
 		
 	# wait for all the processes to finish
 	for proc in processes:
 		proc.join()
 		print("Process %s finished ..." % proc.pid)
		
	print("Finished parsing vcf file, now creating ElasticSearch index ...")

	for json in output_json:
#try:
		populate_es_index(json)	
#		except 
	print("Finished creating ES index\n")	
	print("All done!")
		
