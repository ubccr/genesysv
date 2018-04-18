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
from utils import *
#import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from es_celery.tasks import post_data, update_refresh_interval
from itertools import islice


parser = argparse.ArgumentParser()
required = parser.add_argument_group('required named arguments')
required.add_argument("--vcf", help="Annovar or VEP annotated input vcf file. Must be compressed with bgzip and indexed with grabix", required=True)
required.add_argument("--tmp_dir", help="Temporory directory to store intermediate files", required=True)
required.add_argument("--annot", help="Type of variant consequence annotation. Valid values are 'annovar' or 'vep'", required=True)
required.add_argument("--mapping", help="ElasticSearch mapping file", required=True)
required.add_argument("--hostname", help="ElasticSearch hostname", required=True)
required.add_argument("--port", help="ElasticSearch host port number", required=True)
required.add_argument("--index", help="ElasticSearch index name", required=True)
required.add_argument("--num_cores", help="Number of cpu cores to use. Default to the number of cpu cores of the system", required=False)
required.add_argument("--ped", help="Pedigree file", required=False)
required.add_argument("--case_vcf", help="vcf file from case study. Must be compressed with bgzip and indexed with grabix", required=False)
required.add_argument("--control_vcf", help="vcf file from control study. Must be compressed with bgzip and indexed with grabix", required=False)
	
args = parser.parse_args()

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
		print("Invalid vcf file. Please provide a bgzipped vcf file.")
		sys.exit(2)

	if args.vcf and args.case_vcf or args.control_vcf:
		print("If you are working on a single cohort vcf data, there is no need to provide case and control vcf files.")
	elif (args.case_vcf and not args.control_vcf) or (args.control_vcf and not args.case_vcf):
		print("You need to provide both the case and control vcf files.")
		sys.exit(2)

# global variables
num_cpus = args.num_cores
if (num_cpus is None):
	num_cpus = multiprocessing.cpu_count()
else:
	num_cpus = int(args.num_cores)

mapping_file = args.mapping

es = Elasticsearch( host=args.hostname, port=args.port, request_timeout=180)
index_name = args.index
type_name = index_name # use the same name since only one type is allowed
	        
result2 = OrderedDict()

# https://useast.ensembl.org/info/genome/variation/predicted_data.html
precedence_dict = {
	"transcript_ablation" : 1,
	"splice_acceptor_variant" : 2,
	"splice_donor_variant" : 3,
	"stop_gained" : 4,
	"frameshift_variant" : 5,
	"stop_lost" : 6,
	"start_lost" : 7,
	"transcript_amplification" : 8,
	"inframe_insertion" : 9,
	"inframe_deletion" : 10,
	"missense_variant" : 11,
	"protein_altering_variant" : 12,
	"splice_region_variant" : 13,
	"incomplete_terminal_codon_variant" : 14,
	"start_retained_variant" : 15,
	"stop_retained_variant" : 16,
	"synonymous_variant" : 17,
	"coding_sequence_variant" : 18,
	"mature_miRNA_variant" : 19,
	"5_prime_UTR_variant" : 20,
	"3_prime_UTR_variant" : 21,
	"non_coding_transcript_exon_variant" : 22,
	"intron_variant" : 23,
	"NMD_transcript_variant" : 24,
	"non_coding_transcript_variant" : 25,
	"upstream_gene_variant" : 26,
	"downstream_gene_variant" : 27,
	"TFBS_ablation" : 28,
	"TFBS_amplification" : 29,
	"TF_binding_site_variant" : 30,
	"regulatory_region_ablation" : 31,
	"regulatory_region_amplification" : 32,
	"feature_elongation" : 33,
	"regulatory_region_variant" : 34,
	"feature_truncation" : 35,
	"intergenic_variant" : 36
}

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

def process_ped_file(ped_file):
	global ped_info
	ped_info = {}

	with open(ped_file, 'r') as pd:
		for line in pd.readlines():
			if line.startswith('#'):
				continue

			family, subject, father, mother, sex, phenotype, *_ = line.split()
			ped_info[subject] = dict(zip(["family", "father", "mother", "sex"], [family, father, mother, sex]))

def process_vcf_header(vcf):
	# declare some global variables
	global num_header_lines
	global id2type
	global csq_fields
	global col_header
	global assembly
	global chr2len  # to hold a chr to its length lookup table

	num_header_lines = 0
	id2type = {}
	csq_fields = []
	col_header = []
	chr2len = {} 

	# compile some patterns
	p1 = re.compile('^##(.*?)=(.*)$')	
	p2 = re.compile(r'<(.*)\">')
	p3 = re.compile(r'^ID=(.*?),')
	p4 = re.compile(r'.*Number=(.*?),?.*$')
	p5 = re.compile(r'.*Type=(.*?),')
	p6 = re.compile(r'.*Description=\"(.*?)\",?.*')
	p7 = re.compile(r'^##contig=<ID=(.*?),.*?length=(\d+).*?>')
	
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

			m7 = p7.match(line)
			if m7:
				# only include regular chromosomes
				valid_chrs = range(1, 23)
				valid_chrs = [str(item) for item in valid_chrs ]
				valid_chrs.append('X')
				valid_chrs.append('Y')
				valid_chrs.append('M')

				if m7.group(1) in valid_chrs:
					chr2len[m7.group(1)] = int(m7.group(2))
				else:
					valid_chrs = ['chr' + chr for chr in valid_chrs]
					if m7.group(1) in valid_chrs:
						chr2len[m7.group(1)] = int(m7.group(2))


def parse_vcf(vcf, interval, outfile):

	# declare a list to hold output json files
	#global output_json
	#output_json = []
	
	p = multiprocessing.current_process()

	# divide interval into smaller chunks to minimize memory footprint
	chunk_size = 1000
	start = interval[0]
	num_variants_processed = 0

	#outfile = re.sub('.vcf.gz', '_chunk_' + str(p.pid) + '.json', vcf)
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
			
				process_line_data(variant_lines, log, f)
				
				num_variants_processed += end - start + 1
				
				# update start and end positions
				start += chunk_size
				
				print("Pid %s: processed %d variants" % (p.pid, num_variants_processed))
			
				if end >= interval[1]:
					break
	
def parse_info_fields(info_fields, result, group = None):
	in_dbsnp = 0 # for  boolean variables
	in_cosmic = 0

	for info in info_fields:
		if info.startswith('CSQ='):
			csq_list = []
			precedence_list = []
			info_csq = info.split(',')

			for csq in info_csq:
				csq2	= csq.split('|')
				csq2[0] = re.sub('CSQ=', '', csq2[0]) # first csq item contains 'CSQ=', so remove it
				csq_dict = OrderedDict(zip(csq_fields, csq2)) # map names to values for CSQ annotation sub-fields

				# set missing values
				for key, val in csq_dict.items():
					if val == '':
						if key.endswith('_AF'):
							csq_dict[key] = -9 # set missing MAF values to -9
						else:
							csq_dict[key] = None # set other string values to 'None'
					else:
						if key.endswith('_AF') or key.startswith('CADD'): # need to handel other float variables
							csq_dict[key] = float(csq_dict[key])

				csq_list.append(csq_dict)
				consequence_list = csq_dict['Consequence'].split('&')
				tmp = []

				for conseq in consequence_list:
					tmp.append(precedence_dict[conseq])
				
				precedence_list.append(min(tmp))
				
				# booleans
				if csq_dict['Existing_variation'] is not None:
					if 'rs' in csq_dict['Existing_variation']:
						in_dbsnp = 1

					if 'COSM' in csq_dict['Existing_variation']:
						in_cosmic = 1

			# reduce the CSQ annotation to keep the entry with the most severe damaging variant
			# get the index of the smallist value in the precedence_list
			pos = precedence_list.index(min(precedence_list))
			result.update(csq_list[pos])

		else: # general annotation in INFO line contains AC, AF, AN, ect. May also contains  Annovar annotation
			if '=' in info:
				key, val = info.split('=')

				# type conversion
				if key in name2type['int']:
					if val == '.':
						val = None
						log.write("INFO skipped: %s" % info)
						continue
					if group is not None:
						result[key +'_'+ group] = int(val)
					else:
						result[key] = int(val)
				elif key in name2type['float']:
					if val == '.':
						if 'ExAC_' in key or 'esp6500siv2_' in key or '1000g2015aug_' in key:
 							val = -9
						else:
							val = None
							log.write("INFO skipped: %s\n" % info)
							continue

					if group is not None:
						result[key +'_'+ group] = float(val)
					else:
						result[key] = float(val)
				else:
					if group is not None:
						result[key +'_'+ group] = val
					else:
						result[key] = val
			elif info in ['DB', 'POSITIVE_TRAIN_SITE', 'NEGATIVE_TRAIN_SITE']:
				continue
			else:
				log.write("INFO skipped: %s\n" % info)
				continue

	if in_dbsnp == 1:
		result['in_dbsnp'] = True
	if in_cosmic == 1:
		result['in_cosmic'] = True

	return(result)

def process_line_data(variant_lines, log, f):
	for line in variant_lines:
		result = OrderedDict()
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
		result['in_cosmic'] = False
		result['in_dbsnp'] = False
		result = parse_info_fields(info_fields, result)
		
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
							sample_data_dict['AD_ref'], sample_data_dict['AD_alt'], *_ = sub_items
							sample_data_dict['AD_ref'] = int(sample_data_dict['AD_ref'])
							sample_data_dict['AD_alt'] = int(sample_data_dict['AD_alt'])
						else:
							sample_data_dict[key] = val # this is the Phred scalled genotype likely hood, 10^(-log(n/10), no need to split them, just present as is
					else:
						if val == '.':
							sample_data_dict[key] = None
						else:
							try:
								sample_data_dict[key] = int(val) # are they all the integer type?
							except ValueError:
								log.write("Unknown type: %s\n" % val)
								continue

			# add information from ped file
			if args.ped and sample_id in  ped_info:
				sample_data_dict['Family_ID'] = ped_info[sample_id]['family']
				sample_data_dict['Father_ID'] = ped_info[sample_id]['father']
				sample_data_dict['Mother_ID'] = ped_info[sample_id]['mother']
				sample_data_dict['Sex'] = ped_info[sample_id]['sex']
				sample_data_dict['Phenotype'] = ped_info[sample_id]['phenotype']
				if ped_info[sample_id]['father'] == sample_id:
					sample_data_dict['is_father'] = True
				elif ped_info[sample_id]['mother'] == sample_id:
					sample_data_dict['is_mother'] = True
				elif ped_info[sample_id]['subject'] == sample_id:
					sample_data_dict['is_child'] = True
				elif ped_info[sample_id]['sex'] == 'M':
					sample_data_dict['is_male'] = True
				elif ped_info[sample_id]['sex'] == 'F':
					sample_data_dict['is_female'] = True

				
			sample_data_dict['Sample_ID'] = sample_id
			sample_data_array.append(sample_data_dict)

		result['sample'] = sample_data_array
		es_id = get_es_id(data_fixed['CHROM'], data_fixed['POS'], data_fixed['REF'], data_fixed['ALT'], data_fixed['ID'],  index_name, type_name)
		json.dump({"index": {"_index" : index_name, "_type" : "_doc", "_id": es_id}}, f, ensure_ascii=True)
		#result2.update({'_op_type': 'index', '_index': index_name, '_type': '_doc', '_id': es_id, '_source': result})
		f.write("\n")
		json.dump(result, f, sort_keys=False, ensure_ascii=True)
		f.write("\n")

def read_json_in_chunk(fp, n):
	while True:
		next_n_lines = list(islice(fp, n))

		if not next_n_lines:
			break
		else:
			yield next_n_lines


def process_single_cohort(vcf):
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

	parse_vcf(vcf, intervals[-1], 'tmp/output_annovar.json')	
	output_json = []

 	# dispatch subtasks to each of the processes. 
	for i in range(num_cpus):
		output_file = os.path.join(args.tmp_dir, os.path.basename(vcf) + '.chunk_' + str(i) + '.json')
		proc = multiprocessing.Process(target=parse_vcf, args=[vcf, intervals[i], output_file])
		proc.start()
		processes.append(proc)
		output_json.append(output_file)
 		
 	# wait for all the processes to finish
	for proc in processes:
		proc.join()
	print("Process %s finished ..." % proc.pid)
		
	return(output_json)

def process_case_control(case_vcf, control_vcf):
	process_vcf_header(case_vcf)
	start = 1
	batch_size = 1000000

	for chrom, length in chr2len.items():
		while True:
			if (start < length):
				
				batch = chrom + ':' + start + '-' + batch_size
				if (start + batch_size >= length):
					batch = chrom + ':' + start + '-' + length
 				# get a chunck of line from each of the vcf files
				lines_case = check_output("tabix", case_vcf, batch)
				lines_control = check_output("tabix", control_vcf, batch)

				parse_case_control(lines_case, lines_control)

				start = start + batch_size + 1
			if (start + batch_size >= length):
				break

def parse_case_control(lines_case, lines_control):
	data_dict = defaultdict()
	result = OrderedDict()

	for line in lines_case:
		col_data = line.strip().split("\t")
		v_id = '_'.join(col_data[0], col_data[1], col_data[3], col_data[4])
		data_dict['case'][v_id] = col_data

	for line in lines_control:
		col_data = line.strip().split("\t")
		v_id = '_'.join(col_data[0], col_data[1], col_data[3], col_data[4])
		data_dict['control'][v_id] = col_data

	for group in ['case', 'control']:
		for v_id in data_dict[group]:
			in_dbsnp = 0
			in_cosmic = 0

			# process shared variants
			data_fixed = dict(zip(col_header[:6], data_dict[group][v_id][:6]))

			result['Variant'] = '_'.join(data_fixed['CHROM'], data_fixed['POS'], data_fixed['REF'][:10], data_fixed['ALT'][:10])
			
			if data_fixed['ID'].startswith('rs'): # should also check the INFO field
				in_dbsnp = 1

			if data_fixed['REF'] in ['G','A','T','C'] and data_fixed['ALT'] in ['G','A','T','C']:
				result['VariantType'] = 'SNV'
			else:
				result['VariantType'] = 'INDEL'

			# QUAL and FILTER field
			result['QUAL_' + group] = float(data_fixed['QUAL'])
			result['FILTER_' + group] = data_fixed_case['FILTER']
			
			# parse INFO field
			if ';' in data_dict[group][key][7]:
				info_fields = data_dict[group][key][7].split(";")
				
				result = parse_info_fields(info_fields, result, group)

			
			# parse FORMAT field
			if ':' in data_dict[group][vid][8]:
				format_fields = data_dict[group][v_id][8].split(":")
			else:
				format_fields = 'GT' # 1000 Genomes Project used a single GT field


		
      #  if in_dbsnp == 1:
       #     result['in_dbsnp'] = True
        #if in_cosmic == 1:
         #   result['in_cosmic'] = True



if __name__ == '__main__':
	check_commandline(args)

	get_mapping(mapping_file)

#	mapping = json.load(open("", 'r'))
#	es.indices.put_mapping(index=index_name, doc_type=type_name, body=mapping)
	if args.ped:
		process_ped_file(args.ped)
	elif args.case_vcf and args.control_vcf:
		pass

	# determine which work flow to choose, i.e. single cohort or case-control analysis
	if args.vcf:
		output_files = process_single_cohort(args.vcf)
		print(output_files)
	elif args.case_vcf and args.control_vcf:
		output_files = process_case_control(args.case_vcf, args.control_vcf)



	print("Finished parsing vcf file, now creating ElasticSearch index ...")
	output_json = ["tmp/SIMCT20160930_variants_VEP.vcf.gz.chunk_0.json", "tmp/SIMCT20160930_variants_VEP.vcf.gz.chunk_1.json"]
	for infile in output_json:
		with open(infile, 'r') as fp:
			for chunk in read_json_in_chunk(fp, 1000):
				#deque(helpers.parallel_bulk(es, chunk, thread_count=4), maxlen=0)
				success, info = helpers.parallel_bulk(client=es, actions=chunk, thread_count=4)
				#res = helpers.bulk(es, chunk, chunk_size=5, request_timeout=200)
				#if not success:
				#	print('A document failed:', info)
			#pass
		print(infile)
#			post_data.delay(args.hostname, args.port, index_name, type_name, infile)

	print("Finished creating ES index\n")	
	print("All done!")
