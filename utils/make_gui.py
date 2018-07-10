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
import utils
import sqlite3
from collections import OrderedDict
import utils
import django
import pickle

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


FORM_TYPES = ("CharField", "ChoiceField", "MultipleChoiceField")

WIDGET_TYPES = ("TextInput", "Select", "SelectMultiple",
                "Textarea", "UploadField")

ES_FILTER_TYPES = ("filter_term",
                   "filter_terms",
                   "nested_filter_term",
                   "nested_filter_terms",
                   "filter_range_gte",
                   "filter_range_gt",
                   "filter_range_lte",
                   "filter_range_lt",
                   "nested_filter_range_gte",
                   "nested_filter_range_lte",
                   "filter_exists",
                   "must_not_exists",
                   "nested_filter_exists",
                   )

APP_NAMES = (
    "core",
    "complex",
    "mendelian",
    "microbiome",
)

ANALYSIS_TYPES = (
    ("base-search", "core"),
    ("complex","complex"),
    ("autosomal_dominant", "mendelian"),
    ("autosomal_recessive", "mendelian"),
    ("compound_heterozygous", "mendelian"),
    ("denovo", "mendelian"),
    ("x_linked_denovo", "mendelian"),
    ("x_linked_dominant", "mendelian"),
    ("x_linked_recessive", "mendelian"),
    ("microbiome","microbiome"),
)

def make_gui_config(vcf_info_file, mapping_file, type_name, annot, case_control):
	mapping = json.load(open(mapping_file, 'r'))
	mapping = mapping[type_name]['properties']
	vcf_info = json.load(open(vcf_info_file, 'r'))
	
	info_dict = vcf_info['info_dict']
	format_dict = vcf_info['format_dict']
	gui_mapping_var = OrderedDict()
	gui_mapping_qc = OrderedDict()
	gui_mapping_stat = OrderedDict()
	gui_mapping_gene = OrderedDict()
	gui_mapping_func = OrderedDict()
	gui_mapping_maf = OrderedDict()
	gui_mapping_conserv = OrderedDict()
	gui_mapping_patho_p = OrderedDict()
	gui_mapping_patho_s = OrderedDict()
	gui_mapping_disease = OrderedDict()
	gui_mapping_intvar = OrderedDict()
	gui_mapping_sample = OrderedDict()
	gui_mapping_others = OrderedDict()

	
	if case_control is True:
		VARIANT_RELATED_FIELDS = utils.VARIANT_RELATED_FIELDS + ['QUAL_case', 'QUAL_control', 'FILTER_case', 'FILTER_control']
		VARIANT_RELATED_FIELDS = [item for item in VARIANT_RELATED_FIELDS if item not in ['QUAL', 'FILTER']]

		VARIANT_QUALITY_RELATED_FIELDS_case = [item + '_case' for item in utils.VARIANT_QUALITY_RELATED_FIELDS]
		VARIANT_QUALITY_RELATED_FIELDS_control = [item + '_control' for item in utils.VARIANT_QUALITY_RELATED_FIELDS]
		VARIANT_QUALITY_RELATED_FIELDS = VARIANT_QUALITY_RELATED_FIELDS_case + VARIANT_QUALITY_RELATED_FIELDS_control

		SUMMARY_STATISTICS_FIELDS_case = [item + '_case' for item in utils.SUMMARY_STATISTICS_FIELDS]
		SUMMARY_STATISTICS_FIELDS_control = [item + '_control' for item in utils.SUMMARY_STATISTICS_FIELDS]
		SUMMARY_STATISTICS_FIELDS = SUMMARY_STATISTICS_FIELDS_case + SUMMARY_STATISTICS_FIELDS_control
	else:
		VARIANT_RELATED_FIELDS = utils.VARIANT_RELATED_FIELDS
		VARIANT_QUALITY_RELATED_FIELDS = utils.VARIANT_QUALITY_RELATED_FIELDS
		SUMMARY_STATISTICS_FIELDS = utils.SUMMARY_STATISTICS_FIELDS

	if annot == 'vep':
		minor_allele_freq_fields = [key for key in mapping if '_AF' in key]
	
		gene_related_fields = ["Gene", "SYMBOL", "BIOTYPE", "Feature", "Feature_type", "miRNA", "EXON", "INTRON", "CDS_position", "cDNA_position", "Protein_position", "Amino_acids", "DISTANCE", "HGNC_ID", "HGVSc", "HGVSp", "STRAND", "SWISSPROT", "Codons", "DOMAINS"]
		functional_consequence_fields = ['Consequence']
		pathogenicity_score_fields = ["CADD_RAW", "CADD_PHRED", "PolyPhen_score", "SIFT_score"]		
		pathogenicity_prediction_fields = ["IMPACT", "SIFT_pred", "PolyPhen_pred"]
		disease_association_fields = ['COSMIC_ID', 'PUBMED', 'PHENO', 'CLIN_SIG']
		conservation_fields = []
		intervar_fields = []
	elif annot == 'annovar':
		gene_related_fields =  ["Ensembl_Gene_ID", "Gene_ensGene", "EnsembleTranscriptID", "Ensembl_Protein_ID", "NCBI_Gene_ID", "Gene_symbol", "Gene_refGene",  "RefSeq", "NCBI_Protein_ID", "exon_id_rg", "cdna_change_rg", "aa_change_rg", "exon_id_eg", "cdna_change_eg", "aa_change_eg", "GeneDetail_ensGene", "GeneDetail_refGene", "Uniprot_ID", "Uniprot_Name"]
		functional_consequence_fields = ['Func_refGene', 'Func_ensGene', 'ExonicFunc_refGene', 'ExonicFunc_ensGene']
		pathogenicity_score_fields = { "Exome_region": ['PolyPhen2_score', 'SIFT_score', 'FatHmm_score', 'PROVEAN_score', 'MutAss_score', 
								'EFIN_Swiss_Prot_Score', 'EFIN_HumDiv_Score', 'Carol_score', 'VEST3_rankscore', 
								'Condel_score', 'COVEC_WMV', 'PolyPhen2_score_transf', 'SIFT_score_transf', 'Interpro_domain',
								'MutAss_score_transf', 'SIFT_converted_rankscore', 'Polyphen2_HDIV_score', 
								'Polyphen2_HDIV_rankscore', 'Polyphen2_HVAR_score', 'Polyphen2_HVAR_rankscore', 
								'LRT_score', 'LRT_converted_rankscore', 'MutationTaster_score', 
								'MutationTaster_converted_rankscore', 'MutationAssessor_score', 'REVEL', 'VEST3_score', 'VEST3_rankscore ',
								'MutationAssessor_score_rankscore', 'FATHMM_score', 'FATHMM_converted_rankscore', 
								'PROVEAN_score', 'PROVEAN_converted_rankscore', 'MetaSVM_score', 'MetaSVM_rankscore',
								'MetaLR_score', 'MetaLR_rankscore', 'M-CAP_score', 'M-CAP_rankscore', 'fathmm-MKL_coding_score',
								'fathmm-MKL_coding_rankscore'],
		
								"Whole_genome": ['CADD', 'CADD_raw', 'CADD_score','CADD_raw_rankscore', 'CADD_phred', 
								'CADD_PHRED', 'CADD_RAW', 'CADD13_RawScore', 'CADD_Phred_score', 'CADD_raw_pankscore',
								'CADD13_PHRED', 'DANN_score', 'DANN_rankscore',
								'Eigen', 'Eigen-raw', 'Eigen-PC-raw', 'GenoCanyon_score', 'GenoCanyon_score_rankscore',
								'GERPplusplus_RS', 'GERPplusplus_RS_rankscore', 'integrated_fitCons_score', 'integrated_fitCons_score_rankscore'],
								
								"Splice_junctions" : ['dbscSNV_ADA_SCORE', 'dbscSNV_RF_SCORE'] }
		pathogenicity_prediction_fields = ['PolyPhen2_prediction', 'SIFT_prediction', 'FatHmm_prediction', 'PROVEAN_prediction', 
			'MutAss_prediction', 'MutAss_pred_transf', 'EFIN_Swiss_Prot_Predictio', 'EFIN_HumDiv_Prediction', 'CADD_prediction', 
								'Carol_prediction', 'Condel_pred', 'COVEC_WMV_prediction', 'PolyPhen2_pred_transf', 
							 	'SIFT_pred_transf', 'utAss_pred_transf', 'SIFT_pred', 'Polyphen2_HDIV_pred', 'Polyphen2_HVAR_pred',
								'LRT_pred', 'MutationTaster_pred', 'MutationAssessor_pred', 'FATHMM_pred', 'PROVEAN_pred', 
								'MetaSVM_pred', 'MetaLR_pred', 'M-CAP_pred', 'fathmm-MKL_coding_pred', 'EFIN_Swiss_Prot_Prediction']
		minor_allele_freq_fields = [key for key in mapping if 'gnomAD_' in key or 'ExAC_' in key or '1000g2015aug_' in key or 'esp6500' in key or 'nci60' in key] 
		
		disease_association_fields = ['Associated_disease', 'COSMIC_Occurrence', 'ICGC_Id', 'ICGC_Occurrence',  'gwasCatalog', 'Tumor_site', 'CLINSIG', 'CLNDBN', 'CLNDSDBID', 'CLNDSDB', 'CLNACC', 'COSMIC_ID', 'snp138NonFlagged'] 

		conservation_fields = ['PhastCons_46V', 'PhyloP_100V', 'PhyloP_46V', 'PhastCons_100V', 'tfbsConsSites',
								'phyloP100way_vertebrate', 'phyloP100way_vertebrate_rankscore', 
								'phyloP20way_mammalian', 'phyloP20way_mammalian_rankscore', 
								'phastCons100way_vertebrate', 'phastCons100way_vertebrate_rankscore', 
								'phastCons20way_mammalian', 'phastCons20way_mammalian_rankscore', 
								'SiPhy_29way_logOdds', 'SiPhy_29way_logOdds_rankscore', 'wgRna', 'targetScanS']
		intervar_fields = ["BS1", "BS2", "BS3", "BS4", "BA1", "BP1", "BP2","BP3", "BP4", "BP5", "BP6", "BP7", "PVS1", "PS1", "PS2", "PS3", "PS4", "PM1", "PM2", "PM3", "PM4", "PM5", "PM6", "PP1", "PP2", "PP3", "PP4", "PP5"]

	sample_related_fields = ['Sample_ID', 'Phenotype', 'Sex', 'GT', 'PGT', 'PID', 'AD', 'AD_ref', 'AD_alt', 'DP', 'MIN_DP', 'GQ', 'PGQ', 'PL', 'SB', 'group', 'Family_ID', 'Mother_ID', 'Father_ID', 'Mother_Genotype', 'Father_Genotype', 'Mother_Phenotype', 'Father_Phenotype']	
	boolean_fields = ['dbSNP_ID', 'COSMIC_ID', 'snp138NonFlagged']
	
	to_exclude = ['sample', 'AAChange_refGene', 'AAChange_ensGene', 'CSQ_nested', 'Class_predicted', 'culprit','US', 'Presence_in_TD', 'Prob_N', 'Prob_P', 'Mutation_frequency', 'AA_pos', 'AA_sub', 'CCC', 'CCC_case', 'CCC_control', 'CSQ', 'END', 'END_case', 'END_control', 'DB', 'MQ0', 'ANNOVAR_DATE', 'NEGATIVE_TRAIN_SITE', 'POSITIVE_TRAIN_SITE', 'DS', 'DS_case', 'DS_control',  'ALLELE_END', 'NCC', 'NCC_case', 'NCC_control']

	# remove features that are converted to *_case and *_control
	if case_control is True:
		tmp = VARIANT_RELATED_FIELDS + VARIANT_QUALITY_RELATED_FIELDS + SUMMARY_STATISTICS_FIELDS
		features_to_remove = [item for item in tmp if item.endswith('_case')]
		features_to_remove = [re.sub('_case', '', item) for item in features_to_remove]
		to_exclude += features_to_remove

	keys_in_es_mapping = [key for key in mapping if key not in to_exclude]
	for key in mapping:
		if mapping[key]['type'] == 'nested':
			keys_in_es_mapping = keys_in_es_mapping + list(mapping[key]['properties'].keys())
			
	seen = {} # to store features in keys_in_es_mapping actually used
	
	default_gui_mapping = {"filters": 
								[
									{
									"display_text": "label",
									"es_filter_type": "filter_term", 
									"form_type": "CharField",
									"widget_type": "TextInput"
									}
								],
								"panel": "Other",
								"tab": "Basic"
							}

	# annotation independent fields
	for key in VARIANT_RELATED_FIELDS:
		if key in keys_in_es_mapping:
			gui_mapping_var[key] = copy.deepcopy(default_gui_mapping)
			gui_mapping_var[key]['panel'] = "Variant Related Information"
			gui_mapping_var[key]['filters'][0]["display_text"] = key

			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
			gui_mapping_var[key]['filters'][0]['tooltip'] = tooltip

			if key == 'CHROM':
				gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_var[key]['filters'][0]["values"] = "get_values_from_es()"
				gui_mapping_var[key]['filters'][0]["es_filter_type"] = "filter_terms"
			elif key == 'FILTER':
				gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_var[key]['filters'][0]["values"] = "get_values_from_es()"
			elif key in ['VariantType', 'SVTYPE']:
				gui_mapping_var[key]['filters'][0]["form_type"] = "ChoiceField"
				gui_mapping_var[key]['filters'][0]['widget_type'] = "Select"
				gui_mapping_var[key]['filters'][0]['values'] = "get_values_from_es()"
			elif key in ['POS', 'END', 'MLEN', 'MEND', 'MSTART', 'SVLEN', 'CIPOS', 'CIEND']:
				gui_mapping_var[key]['filters'].append(copy.deepcopy(gui_mapping_var[key]['filters'][0]))
				gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_var[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping_var[key]['filters'][1]['in_line_tooltip'] = "(<=)"
			elif key == 'Variant':
				gui_mapping_var[key]['filters'][0]["in_line_tooltip"] = "(e.g. 1_115252204_C_T)"
			elif key == 'QUAL':
				gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
			elif key == 'cytoBand':
				gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_var[key]['filters'][0]["values"] = "get_values_from_es()"
			elif key == 'ID':
				gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(from original VCF)"
			elif key in ['EX_TARGET', 'IMPRECISE', 'MULTI_ALLELIC']:	
				gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_var[key]['filters'][0]['form_type'] = "ChoiceField"
				gui_mapping_var[key]['filters'][0]["widget_type"] = "Select"
				gui_mapping_var[key]['filters'][0]['values'] = "get_values_from_es()"
			elif key in boolean_fields:
					gui_mapping_var[key]['filters'].append(copy.deepcopy(gui_mapping_var[key]['filters'][0]))
					gui_mapping_var[key]['filters'][0]['display_text'] = key
					gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(One ID per line)"
					gui_mapping_var[key]['filters'][0]['widget_type'] = "UploadField"
					gui_mapping_var[key]['filters'][0]['form_type'] = "CharField"
					gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_terms"
					gui_mapping_var[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
					gui_mapping_var[key]['filters'][1]['widget_type'] = "Select"
					gui_mapping_var[key]['filters'][1]['es_filter_type'] = "filter_exists"
					gui_mapping_var[key]['filters'][1]['form_type'] = "ChoiceField"
			seen[key] = ''
			
	for key in VARIANT_QUALITY_RELATED_FIELDS:
		if key in keys_in_es_mapping:
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
			gui_mapping_qc[key] = copy.deepcopy(default_gui_mapping)

			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				gui_mapping_qc[key]['filters'][0]['display_text'] = key
				gui_mapping_qc[key]['filters'].append(copy.deepcopy(gui_mapping_qc[key]['filters'][0]))
				gui_mapping_qc[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_qc[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_qc[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_qc[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_qc[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping_qc[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping_qc[key]['filters'][1]['in_line_tooltip'] = "(<=)"
			elif key.startswith('FILTER'):
				gui_mapping_qc[key]['filters'][0]['display_text'] = key
				gui_mapping_qc[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_qc[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_qc[key]['filters'][0]["values"] = "get_values_from_es()"

			gui_mapping_qc[key]['panel']  = 'Variant Quality Metrix'
			seen[key] = ''	
	for key in minor_allele_freq_fields:
		if key.endswith('_case' or key.endswith('_control')): # the '_case' and '_control' are added in the make_es_mapping funtion in load_vcf.pl
			continue

		if key in keys_in_es_mapping:
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
				
			gui_mapping_maf[key] = copy.deepcopy(default_gui_mapping)
			gui_mapping_maf[key]['filters'][0]['display_text'] = key
			if key == "MAX_AF_POPS":
				gui_mapping_maf[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_maf[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_maf[key]['filters'][0]["values"] = "get_values_from_es()"
			else:
				gui_mapping_maf[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_maf[key]['filters'][0]['es_filter_type'] = "filter_range_lt"
				gui_mapping_maf[key]['filters'][0]['in_line_tooltip'] = "(<)"
				gui_mapping_maf[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_maf[key]['filters'][0]['widget_type'] = "TextInput"
			
			if '1000g2015aug_' in key or key in ['MAX_AF', 'AA_AF', 'EA_AF', 'EUR_AF', 'AFR_AF', 'EAS_AF', 'ERR_AF', 'AMR_AF', 'SAS_AF']:
				gui_mapping_maf[key]['sub_panel'] = '1000 Genomes Project (Aug. 2015)'
			elif 'ExAC_' in key:
				gui_mapping_maf[key]['sub_panel'] = 'The Exome Aggregation Consortium (ExAC)'
			elif 'esp6500' in key:
				gui_mapping_maf[key]['sub_panel'] = 'Exome Sequencing Project'
			elif 'gnomAD' in key:
				gui_mapping_maf[key]['sub_panel'] = 'gnomAD Allele Frequency'
			elif key == 'nci60':
				gui_mapping_maf[key]['sub_panel'] = 'NCI60'

			gui_mapping_maf[key]['panel']  = 'Minor Allele Frequency (MAF)'
			seen[key] = ''
	for key in SUMMARY_STATISTICS_FIELDS:
		if key in keys_in_es_mapping:
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
				
			gui_mapping_stat[key] = copy.deepcopy(default_gui_mapping)
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				gui_mapping_stat[key]['filters'].append(copy.deepcopy(gui_mapping_stat[key]['filters'][0]))
				gui_mapping_stat[key]['filters'][0]['display_text'] = key
				gui_mapping_stat[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_stat[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_stat[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_stat[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_stat[key]['filters'][1]['display_text'] = key
				gui_mapping_stat[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping_stat[key]['filters'][1]['in_line_tooltip'] = "(<=)"
				gui_mapping_stat[key]['filters'][1]['form_type'] = "CharField"			
				gui_mapping_stat[key]['panel']  = 'Summary Statistics Information'
				seen[key] = ''
				
	sample_es_mapped_fields = [key for key in mapping['sample']['properties']]
	
	for key in sample_related_fields:
		if key in sample_es_mapped_fields:
			gui_mapping_sample[key] = copy.deepcopy(default_gui_mapping)
			gui_mapping_sample[key]['panel'] = 'Sample Related Information'
			gui_mapping_sample[key]['filters'][0]['display_text'] = key
			gui_mapping_sample[key]['filters'][0]['path'] = 'sample'
			gui_mapping_sample[key]['filters'][0]['es_filter_type'] = "nested_filter_term"
			
			
			if key == 'AD':
				del gui_mapping_sample[key]
			elif key in ['Sample_ID', 'GT', 'PGT', 'Family_ID', 'Father_ID', 'Mother_ID', 'Sex', 'Phenotype', 'Mother_Genotype', 'Father_Genotype', 'Mother_Phenotype', 'Father_Phenotype']:
				gui_mapping_sample[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"		
				gui_mapping_sample[key]['filters'][0]['values'] = "get_values_from_es()"
				gui_mapping_sample[key]['filters'][0]["in_line_tooltip"] = ""
				gui_mapping_sample[key]['filters'][0]['widget_type'] = "SelectMultiple"
				gui_mapping_sample[key]['filters'][0]['form_type'] = "MultipleChoiceField"
			elif key == 'group':
				gui_mapping_sample[key]['filters'][0]['widget_type'] = "Select"
				gui_mapping_sample[key]['filters'][0]['form_type'] = "ChoiceField"
				gui_mapping_sample[key]['filters'][0]['values'] = "get_values_from_es()"
			elif mapping['sample']['properties'][key]['type'] == 'integer' or mapping['sample']['properties'][key]['type'] == 'float':
				if key == 'PL': # keep it as string type
					gui_mapping_sample[key]['filters'][0]["tooltip"] = format_dict[key]['Description']
					
				else:
					gui_mapping_sample[key]['filters'][0]["es_filter_type"] = "nested_filter_range_gte"
					gui_mapping_sample[key]['filters'][0]["in_line_tooltip"] = "(>=)"
					if key.startswith('AD_'):
						gui_mapping_sample[key]['filters'][0]["tooltip"] = format_dict['AD']['Description']
					else:
						gui_mapping_sample[key]['filters'][0]["tooltip"] = format_dict[key]['Description']
			seen[key] = ''


	# Annotation specific fields
	if annot == 'vep':
		for key in functional_consequence_fields:
			if key in keys_in_es_mapping:
				gui_mapping_func[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_func[key]['filters'][0]['display_text'] = key
				gui_mapping_func[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"
				gui_mapping_func[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				gui_mapping_func[key]['filters'][0]["widget_type"] = "SelectMultiple" 
				gui_mapping_func[key]['filters'][0]['values'] = "get_values_from_es()"
				gui_mapping_func[key]['filters'][0]['path'] = 'CSQ_nested'
				gui_mapping_func[key]['panel']  = 'Functional Consequences'
				seen[key] = ''
		for key in gene_related_fields:
			if key in keys_in_es_mapping:
				gui_mapping_gene[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_gene[key]['filters'][0]['display_text'] = key
				if key in ['Gene', 'SYMBOL', 'Feature']:
					gui_mapping_gene[key]['filters'][0]['widget_type'] = "UploadField"
					gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] = "(One ID per line)"
					gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"	
					if key == 'Gene':
						gui_mapping_gene[key]['filters'][0]['display_text'] = 'Entrez Gene ID'
				elif key in ['Feature_type', 'BIOTYPE', 'DOMAINS']:
					gui_mapping_gene[key]['filters'][0]['values'] = "get_values_from_es()"
					gui_mapping_gene[key]['filters'][0]["form_type"] = "MultipleChoiceField"
					gui_mapping_gene[key]['filters'][0]["widget_type"] = "SelectMultiple"
				elif key == 'DISTANCE':
					gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_range_lte"
					gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] = "(<=)"
				elif key.endswith('_position'):
					gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] = "(=)"

				gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"
				gui_mapping_gene[key]['filters'][0]['path'] = 'CSQ_nested'
				gui_mapping_gene[key]['panel']  = 'Gene Related Information'
				seen[key] = ''
		for key in pathogenicity_prediction_fields:
			if key.endswith('_case') or key.endswith('_control'):
				continue
			if key in keys_in_es_mapping:
				gui_mapping_patho_p[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_patho_p[key]['filters'][0]['display_text'] = key
				gui_mapping_patho_p[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"
				gui_mapping_patho_p[key]['filters'][0]['form_type'] = "ChoiceField"
				gui_mapping_patho_p[key]['filters'][0]["widget_type"] = "Select"
				gui_mapping_patho_p[key]['filters'][0]['path'] = 'CSQ_nested'
				gui_mapping_patho_p[key]['filters'][0]['values'] = "get_values_from_es()"
				gui_mapping_patho_p[key]['panel']  = 'Pathogenicity Predictions'
				gui_mapping_patho_p[key]['sub_panel']  = 'Predictions'
				seen[key] = ''	
		for key in pathogenicity_score_fields:
			if key.endswith('_case') or key.endswith('_control'):
				continue
			if key in keys_in_es_mapping:
				gui_mapping_patho_s[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_patho_s[key]['filters'][0]['display_text'] = key
				gui_mapping_patho_s[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_patho_s[key]['filters'][0]['path'] = 'CSQ_nested'
				
				if key == 'SIFT_score':
					gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "nested_filter_range_lte"
					gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(<=)"
				else:
					gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "nested_filter_range_gte"
					gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(>=)"
					
				gui_mapping_patho_s[key]['panel']  = 'Pathogenicity Predictions'
				gui_mapping_patho_s[key]['sub_panel']  = 'Scores'
				seen[key] = ''
					
		for key in ['CADD_RAW', 'CADD_PHRED']:
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
					
				gui_mapping_patho_s[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_patho_s[key]['filters'][0]['display_text'] = key
				gui_mapping_patho_s[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_patho_s[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_patho_s[key]['filters'][0]['widget_type'] = "TextInput"
					
				gui_mapping_patho_s[key]['panel'] = 'Pathogenicity Predictions'
				gui_mapping_patho_s[key]['sub_panel'] = 'Scores'
				seen[key] = ''
		for key in disease_association_fields:
			if key in keys_in_es_mapping:
					gui_mapping_disease[key] = copy.deepcopy(default_gui_mapping)
					gui_mapping_disease[key]['panel'] = 'Disease Associations'
					if key in boolean_fields:
						gui_mapping_disease[key]['filters'].append(copy.deepcopy(gui_mapping_disease[key]['filters'][0]))
						gui_mapping_disease[key]['filters'][0]['display_text'] = key
						gui_mapping_disease[key]['filters'][0]['in_line_tooltip'] = "(One ID per line)"
						gui_mapping_disease[key]['filters'][0]['widget_type'] = "UploadField"
						gui_mapping_disease[key]['filters'][0]['form_type'] = "CharField"
						gui_mapping_disease[key]['filters'][0]['tooltip'] = tooltip
						gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
						gui_mapping_disease[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
						gui_mapping_disease[key]['filters'][1]['widget_type'] = "Select"
						gui_mapping_disease[key]['filters'][1]['es_filter_type'] = "filter_exists"
						gui_mapping_disease[key]['filters'][1]['form_type'] = "ChoiceField"
					else:
						gui_mapping_disease[key]['filters'][0]['display_text'] = key
						gui_mapping_disease[key]['filters'][0]['form_type'] = "MultipleChoiceField"
						gui_mapping_disease[key]['filters'][0]["widget_type"] = "SelectMultiple"
						gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
						gui_mapping_disease[key]['filters'][0]['values'] = "get_values_from_es()"
					seen[key] = ''
				
	elif annot == 'annovar':
		for key in functional_consequence_fields:
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
				gui_mapping_func[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_func[key]['filters'][0]['display_text'] = key
				gui_mapping_func[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_func[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				gui_mapping_func[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_func[key]['filters'][0]["widget_type"] = "SelectMultiple" 
				gui_mapping_func[key]['filters'][0]['values'] = "get_values_from_es()"
				gui_mapping_func[key]['panel']  = 'Functional Consequences'
				
				if 'ensGene' in key:
					gui_mapping_func[key]['sub_panel']  = 'Ensembl'
				elif 'refGene' in key or 'NCBI' in key:
					gui_mapping_func[key]['sub_panel']  = 'NCBI'
				else:
					gui_mapping_func[key]['sub_panel']  = 'Other'
				seen[key] = ''
		for key in gene_related_fields:
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
				gui_mapping_gene[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_gene[key]['panel'] = 'Gene Related Information'
				gui_mapping_gene[key]['filters'][0]['tooltip'] = tooltip
				if key in ["RefSeq", "exon_id_rg", "cdna_change_rg", "aa_change_rg", "EnsembleTranscriptID", "exon_id_eg", "cdna_change_eg", "aa_change_eg"]: # these are nested fields
					if key.startswith('exon_id'):
						gui_mapping_gene[key]['filters'][0]['display_text'] = 'Exon ID'
						gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] =  '(e.g. 2/25)'
						gui_mapping_gene[key]['filters'][0]['form_type'] = "CharField"
						gui_mapping_gene[key]['filters'][0]["widget_type"] = "TextInput"
						gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_term"
					elif key.startswith('cdna'):
						gui_mapping_gene[key]['filters'][0]['display_text'] = 'cDNA Change'
						gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] =  '(e.g. c.A727G)'
						gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_term"
						gui_mapping_gene[key]['filters'][0]['form_type'] = "CharField"
						gui_mapping_gene[key]['filters'][0]['widget_type'] = "TextInput"
					elif key.startswith('aa'):
						gui_mapping_gene[key]['filters'][0]['display_text'] = 'Amino Acid Change'
						gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] = '(e.g. p.T243A)'
						gui_mapping_gene[key]['filters'][0]['form_type'] = "CharField"
						gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_term"
						gui_mapping_gene[key]['filters'][0]['widget_type'] = "TextInput"
					elif key == 'RefSeq':
						gui_mapping_gene[key]['filters'][0]['display_text'] = "RefGene ID"
						gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] = "(e.g. NM_133378, one ID per line)"
						gui_mapping_gene[key]['filters'][0]['widget_type'] = "UploadField"
						gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"
					elif key == 'EnsembleTranscriptID':
						gui_mapping_gene[key]['filters'][0]['display_text'] = "Ensembl transcript ID"
						gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] = "(e.g. ENST00000460472)"
						gui_mapping_gene[key]['filters'][0]['widget_type'] = "UploadField"
						gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"
						
					if key in ["RefSeq", "exon_id_rg", "cdna_change_rg", "aa_change_rg"]:
						gui_mapping_gene[key]['filters'][0]['path'] = 'AAChange_refGene'
						gui_mapping_gene[key]['sub_panel'] = 'NCBI Gene'
					else:
						gui_mapping_gene[key]['filters'][0]['path'] = 'AAChange_ensGene'
						gui_mapping_gene[key]['sub_panel'] = 'Ensembl Gene'
				else: # non nested fields
					gui_mapping_gene[key]['filters'][0]['display_text'] = key
					if 'refGene' in key or key.startswith('NCBI'):
						if key == 'NCBI_Gene_ID':
							gui_mapping_gene[key]['filters'][0]['in_line_tooltip'] = '(One ID per line)'
							gui_mapping_gene[key]['filters'][0]['widget_type'] = "UploadField"
							gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "filter_terms"
							
						gui_mapping_gene[key]['sub_panel'] = 'NCBI Gene'
					elif 'ensGene' in key or key.startswith('Ensembl'):
						gui_mapping_gene[key]['sub_panel'] =  'Ensembl Gene'
					else:
						gui_mapping_gene[key]['sub_panel'] = 'Other'
						
					if key in ['Gene_refGene', 'Gene_ensGene', 'Ensembl_Gene_ID']:
						gui_mapping_gene[key]['filters'][0]['widget_type'] = "UploadField"
						gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "filter_terms"
						gui_mapping_gene[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				
				seen[key] = ''
		for  key in conservation_fields:
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
				gui_mapping_conserv[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_conserv[key]['filters'][0]["display_text"] = key
				gui_mapping_conserv[key]['panel'] = 'Conservation'
				gui_mapping_conserv[key]['filters'][0]['tooltip'] = tooltip
				
				if key == "tfbsConsSites":
					gui_mapping_conserv[key]['filters'][0]["display_text"] = key
					gui_mapping_conserv[key]['sub_panel'] = 'Sites'
				else:
					gui_mapping_conserv[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
					gui_mapping_conserv[key]['filters'][0]['in_line_tooltip'] = "(>=)"
					gui_mapping_conserv[key]['sub_panel'] = 'Scores'
				seen[key] = ''
		for key in sorted(pathogenicity_prediction_fields):
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
				gui_mapping_patho_p[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_patho_p[key]['filters'][0]["display_text"] = key
				gui_mapping_patho_p[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_patho_p[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_patho_p[key]['filters'][0]['values'] = "get_values_from_es()"
				gui_mapping_patho_p[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_patho_p[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				gui_mapping_patho_p[key]['panel'] = 'Pathogenicity Predictions'
				gui_mapping_patho_p[key]['sub_panel'] = 'Predictions'
				seen[key] = ''
		for region, features in pathogenicity_score_fields.items():
			for key in sorted(features):
				if key in keys_in_es_mapping:
					if key in info_dict and 'Description' in info_dict[key]:
						tooltip = info_dict[key]['Description']
					else:
						tooltip = ""
					gui_mapping_patho_s[key] = copy.deepcopy(default_gui_mapping)
					gui_mapping_patho_s[key]['filters'][0]['display_text'] = key
					if 'plusplus' in key:
						gui_mapping_patho_s[key]['filters'][0]['display_text'] = re.sub('plusplus', '++', key)
				
					gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
					gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(>=)"
					gui_mapping_patho_s[key]['filters'][0]['tooltip'] = tooltip
				
					if key.startswith('SIFT'): # lower score mean more detrimental
						gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "filter_range_lte"
						gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(<=)"
					
					gui_mapping_patho_s[key]['panel'] = 'Pathogenicity Scores'
					gui_mapping_patho_s[key]['sub_panel'] = region
					seen[key] = ''
		for key in disease_association_fields:
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
				gui_mapping_disease[key]= copy.deepcopy(default_gui_mapping)
				gui_mapping_disease[key]['filters'][0]['display_text'] = key
				gui_mapping_disease[key]['filters'][0]['tooltip'] = tooltip

				if key in ['CLNACC', 'ICGC_Id', 'ICGC_Occurrence', 'CLNDBN', 'CLNDSDBID', 'CLNDSDB' ]:
					gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_term"
				elif key in boolean_fields:
					gui_mapping_disease[key]['filters'].append(copy.deepcopy(gui_mapping_disease[key]['filters'][0]))
					gui_mapping_disease[key]['filters'][0]['display_text'] = key
					gui_mapping_disease[key]['filters'][0]['in_line_tooltip'] = "(One ID per line)"
					gui_mapping_disease[key]['filters'][0]['widget_type'] = "UploadField"
					gui_mapping_disease[key]['filters'][0]['form_type'] = "CharField"
					gui_mapping_disease[key]['filters'][0]['tooltip'] = tooltip
					gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
					gui_mapping_disease[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
					gui_mapping_disease[key]['filters'][1]['widget_type'] = "Select"
					gui_mapping_disease[key]['filters'][1]['es_filter_type'] = "filter_exists"
					gui_mapping_disease[key]['filters'][1]['form_type'] = "ChoiceField"
				else:
					gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
					gui_mapping_disease[key]['filters'][0]['form_type'] = "ChoiceField"
					gui_mapping_disease[key]['filters'][0]["widget_type"] = "Select"
					gui_mapping_disease[key]['filters'][0]['values'] = "get_values_from_es()"
					
				gui_mapping_disease[key]['panel'] = 'Disease Associations'
				seen[key] = ''
		for key in intervar_fields:
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
				gui_mapping_intvar[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_intvar[key]['filters'][0]['values'] = "get_values_from_es()"
				gui_mapping_intvar[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_intvar[key]['panel'] = 'ACMG/AMP InterVar Criteria'
				seen[key] = ''

	# get features not defined in this GUI config
	all_es_mappings = keys_in_es_mapping + sample_es_mapped_fields
	features_unmapped = [key for key in all_es_mappings if key not in seen]

	for key in features_unmapped:
		print("Unmapped GUI %s" % key)
		if key in info_dict and 'Description' in info_dict[key]:
			tooltip = info_dict[key]['Description']
		else:
			tooltip = ""
		gui_mapping_others[key] = copy.deepcopy(default_gui_mapping)
		gui_mapping_others[key]['filters'][0]['display_text'] = key
		gui_mapping_others[key]['filters'][0]['tooltip'] = tooltip
		
	result = OrderedDict()
	
	for dict_ in [gui_mapping_var, gui_mapping_stat, gui_mapping_qc, gui_mapping_gene, gui_mapping_func, gui_mapping_maf, gui_mapping_conserv, gui_mapping_patho_p, gui_mapping_patho_s, gui_mapping_intvar, gui_mapping_disease, gui_mapping_sample, gui_mapping_others]:
		result.update(dict_)

	with open("test_gui.json", 'w') as fp:
		json.encoder.c_make_encoder = None	
		json.dump(result, fp, sort_keys=True, indent=2, ensure_ascii=False)
	return(result)

def add_required_data_to_db():
    """Setup required models"""
    for name in FORM_TYPES:
        FormType.objects.get_or_create(name=name)

    for name in WIDGET_TYPES:
        WidgetType.objects.get_or_create(name=name)

    for name in ES_FILTER_TYPES:
        ESFilterType.objects.get_or_create(name=name)

    for name in APP_NAMES:
        AppName.objects.get_or_create(name=name)

    for analysis_type, app_name in ANALYSIS_TYPES:
        AnalysisType.objects.get_or_create(name=analysis_type, app_name=AppName.objects.get(name=app_name))

def get_order_of_import(ele, vcf_gui_mapping_order):
    if ele in vcf_gui_mapping_order:
        return vcf_gui_mapping_order.index(ele)
    else:
        return len(vcf_gui_mapping_order)+1

def make_gui(es, hostname, port, index_name, study, dataset, type_name, vcf_gui_mapping):

        add_required_data_to_db()

        mapping = elasticsearch.client.IndicesClient.get_mapping(
            es, index=index_name, doc_type=type_name)
        # mapping =
        # mapping[options.get('study]')['mappings'][options.get('dataset]['properties']
        mapping = mapping[index_name]['mappings'][type_name]['properties']

        nested_fields = []
        for var_name, var_info in mapping.items():
            if var_info.get('type') == 'nested':
                nested_fields.append(var_name)

        popped_nested_fields = {}
        for ele in nested_fields:
            popped_nested_fields[ele] = mapping.pop(ele)

        for key, value in popped_nested_fields.items():
            for inner_key, inner_value in value['properties'].items():
                mapping[inner_key] = inner_value

        print("*" * 80 + "\n")
        print('Study Name: %s' % (study))
        print('Dataset Name: %s' % (dataset))
        print('Dataset ES Index Name: %s' % (index_name))
        print('Dataset ES Type Name: %s' % (type_name))
        print('Dataset ES Host: %s' % (hostname))
        print('Dataset ES Port: %s' % (port))


        study_obj, created = Study.objects.get_or_create(
            name=study, description=study)

        dataset_obj, created = Dataset.objects.get_or_create(study=study_obj,
                                                             name=dataset,
                                                             description=dataset,
                                                             es_index_name=index_name,
                                                             es_type_name=type_name,
                                                             es_host=hostname,
                                                             es_port=port,
                                                             is_public=True)

        a = AnalysisType.objects.filter(name__in=['complex', 'autosomal_dominant', 'autosomal_recessive', 'compound_heterozygous', 'denovo', 'x_linked_denovo', 'x_linked_dominant', 'x_linked_recessive'])
        dataset_obj.analysis_type.add(*a)

        SearchOptions.objects.get_or_create(dataset=dataset_obj)

        import_order = sorted(
            list(mapping), key=lambda ele: get_order_of_import(ele,
                                                               list(vcf_gui_mapping)))

        idx = 1
        warning_and_skipped_msgs = []
        # for var_name, var_info in mapping.items():
        for var_name in import_order:
            var_info = mapping.get(var_name)
            if not vcf_gui_mapping.get(var_name):
                warning_and_skipped_msgs.append(
                    '*' * 20 + 'WARNING: No GUI mapping defined for %s' % (var_name))
                continue

            gui_info = vcf_gui_mapping.get(var_name)

            tab_name = gui_info.get('tab').strip()
            panel_name = gui_info.get('panel').strip()
            sub_panel_name = gui_info.get('sub_panel', '').strip()
            filters = gui_info.get('filters')

            for filter_field in filters:

                field_display_text = filter_field.get('display_text').strip()
                field_tooltip = filter_field.get('tooltip', '').strip()
                field_in_line_tooltip = filter_field.get(
                    'in_line_tooltip', '').strip()
                field_form_type = filter_field.get('form_type').strip()
                field_widget_type = filter_field.get('widget_type').strip()
                field_es_name = var_name.strip()
                field_es_filter_type = filter_field.get(
                    'es_filter_type').strip()
                field_es_data_type = var_info.get('type').strip()
                field_es_text_analyzer = var_info.get('analyzer', '').strip()
                field_path = filter_field.get('path', '').strip()
                field_values = filter_field.get('values')

                print("\n%s --- Filter Field" % (idx))
                print("Filter Tab Name: %s" % (tab_name))
                print("Filter Panel Name: %s" % (panel_name))
                print("Filter Subpanel Name: %s" % (sub_panel_name))
                print("Filter Display Name: %s" % (field_display_text))
                print("Filter in Line Tooltip: %s" % (field_in_line_tooltip))
                print("Filter Tooltip: %s" % (field_tooltip))
                print("Filter Form Type: %s" % (field_form_type))
                print("Filter Widget Type: %s" % (field_widget_type))
                print("Filter ES Name: %s" % (field_es_name))
                print("Filter Path: %s" % (field_path))
                print("Filter ES Filter Type: %s" % (field_es_filter_type))
                print("Filter ES Data Type: %s" % (field_es_data_type))
                print("Filter ES Text Analyzer: %s" % (field_es_text_analyzer))
                print("Filter Values: %s" % (field_values))

                match_status = False
                if isinstance(field_values, str):
                    match = re.search(r'python_eval(.+)', field_values)
                    if field_values == 'get_values_from_es()':
                        field_values = get_values_from_es(index_name,
                                                   type_name,
                                                   hostname,
                                                   port,
                                                   field_es_name,
                                                   field_path)
                        pprint(field_values)
                        if not field_values:
                            warning_and_skipped_msgs.append(
                                '*' * 20 + 'Skipped creating GUI for %s because no values found' % (var_name))
                            continue

                    elif match:
                        match_status = True
                        tmp_str = match.groups()[0]
                        try:
                            field_values = eval(tmp_str)
                        except NameError as e:
                            print('Failed to evaluate %s' % (tmp_str))
                            raise(e)

                form_type_obj = FormType.objects.get(name=field_form_type)
                widget_type_obj = WidgetType.objects.get(
                    name=field_widget_type)
                es_filter_type_obj = ESFilterType.objects.get(
                    name=field_es_filter_type)

                filter_tab_obj, _ = FilterTab.objects.get_or_create(
                    dataset=dataset_obj, name=tab_name)
                filter_panel_obj, _ = FilterPanel.objects.get_or_create(
                    name=panel_name, dataset=dataset_obj)

                if not filter_tab_obj.filter_panels.filter(id=filter_panel_obj.id):
                    filter_tab_obj.filter_panels.add(filter_panel_obj)

                filter_field_obj, created = FilterField.objects.get_or_create(dataset=dataset_obj,
                                                                              display_text=field_display_text,
                                                                              in_line_tooltip=field_in_line_tooltip,
                                                                              tooltip=field_tooltip,
                                                                              form_type=form_type_obj,
                                                                              widget_type=widget_type_obj,
                                                                              es_name=field_es_name,
                                                                              path=field_path,
                                                                              es_data_type=field_es_data_type,
                                                                              es_text_analyzer=field_es_text_analyzer,
                                                                              es_filter_type=es_filter_type_obj,
                                                                              place_in_panel=filter_panel_obj.name)

                if field_values:
                    for choice in field_values:
                        FilterFieldChoice.objects.get_or_create(
                            filter_field=filter_field_obj, value=choice)

                attribute_tab_obj, _ = AttributeTab.objects.get_or_create(
                    dataset=dataset_obj, name=tab_name)
                attribute_panel_obj, _ = AttributePanel.objects.get_or_create(
                    name=panel_name, dataset=dataset_obj)

                if not attribute_tab_obj.attribute_panels.filter(id=attribute_panel_obj.id):
                    attribute_tab_obj.attribute_panels.add(attribute_panel_obj)

                try:
                    attribute_field_obj, _ = AttributeField.objects.get_or_create(dataset=dataset_obj,
                                                                                  display_text=field_display_text.replace(
                                                                                      'Limit Variants to', ''),
                                                                                  es_name=field_es_name,
                                                                                  path=field_path,
                                                                                  place_in_panel=attribute_panel_obj.name)
                except:
                    pass

                if sub_panel_name:
                    filter_sub_panel_obj, _ = FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                                                                   name=sub_panel_name, dataset=dataset_obj)

                    attribute_sub_panel_obj, _ = AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                                                                         name=sub_panel_name, dataset=dataset_obj)

                    if not filter_sub_panel_obj.filter_fields.filter(id=filter_field_obj.id):
                        filter_field_obj.place_in_panel = filter_sub_panel_obj.name
                        filter_field_obj.save()
                        filter_sub_panel_obj.filter_fields.add(
                            filter_field_obj)

                    if not attribute_sub_panel_obj.attribute_fields.filter(id=attribute_field_obj.id):
                        attribute_field_obj.place_in_panel = attribute_sub_panel_obj.name
                        attribute_field_obj.save()
                        attribute_sub_panel_obj.attribute_fields.add(
                            attribute_field_obj)

                else:

                    if not filter_panel_obj.filter_fields.filter(id=filter_field_obj.id):
                        filter_panel_obj.filter_fields.add(filter_field_obj)

                    if not attribute_panel_obj.attribute_fields.filter(id=attribute_field_obj.id):
                        attribute_panel_obj.attribute_fields.add(
                            attribute_field_obj)
                idx += 1

        for msg in warning_and_skipped_msgs:
            print(msg)



if __name__ == '__main__':
	hostname= 'localhost'
	port = 9200

	vcf_info_file = 'config/G1K_ALL_phase3_vep_vcf_info.json' #AshkenazimTrio_VEP_vcf_info.json' #G1K_ALL_phase3_vep_vcf_info.json'
	mapping_file = 'utils/scripts/g1k_phase3_all_vep_hg19_mapping.json' #AshkenazimTrio_vep_mapping.json' #g1k_phase3_all_vep_hg19_mapping.json'
	index_name = os.path.basename(mapping_file) #NB: only takes all lower case letters
	index_name = re.sub('_mapping.json', '', index_name)
	type_name = index_name + '_' 
	annot = 'vep' #annovar'
	case_control = False #True

	gui_mapping = make_gui_config(vcf_info_file, mapping_file, type_name, annot, case_control)


	es = elasticsearch.Elasticsearch( host=hostname, port=port, request_timeout=180, max_retries=10, timeout=400, read_timeout=400)
	# make sure the destination dataset not exists
	conn = sqlite3.connect('db.sqlite3')
	c = conn.cursor()
	
	study = 'DEMO'
	dataset_name = index_name
	webserver_port = 8000

	query = "DELETE FROM core_dataset WHERE name = '" + dataset_name + "'"
	try:
		c.execute(query)
	except Exception as e:
		print("Sqlite error: %s" % e)

	conn.commit()
	conn.close()
	
	make_gui(es, hostname, port, index_name, study, dataset_name, type_name, gui_mapping)
	
	print("*"*80+"\n")	
	print("Successfully imported VCF file. You can now explore your data at %s:%s" % (hostname, webserver_port))
	
