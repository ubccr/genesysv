import json
import os
import copy
from collections import OrderedDict


def make_gui(vcf_info_file, mapping_file, type_name, annot):
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

	# annotation independent fields
	variant_related_fields = ['Variant', 'CHROM', 'POS', 'REF', 'ALT', 'QUAL', 'FILTER', 'VariantType']

	variant_quality_related_fields = ['SB', 'OND', 'ExcessHet', 'RAW_MQ', 'InbreedingCoeff', 'MQRankSum', 'MQ0', 'BaseQRankSum', 'HWP', 'FS', 'FS','ClippingRankSum', 'MQ', 'QD', 'ReadPosRankSum', 'HaplotypeScore', 'VQSLOD', 'SOR']
	
	summary_statistics_fields = sorted(['AC', 'AF', 'AN', 'MLEAC', 'MLEAF', 'DP', 'FS', 'GQ_MEAN', 'GQ_STDDEV' ])
	summary_statistics_fields_case =  [item + '_case' for item in summary_statistics_fields]
	summary_statistics_fields_control = [item + '_control' for item in summary_statistics_fields]
	summary_statistics_fields = summary_statistics_fields + summary_statistics_fields_case
	summary_statistics_fields = summary_statistics_fields + summary_statistics_fields_control
	
	if annot == 'vep':
		minor_allele_freq_fields = [key for key in mapping if '_AF' in key]
	
		gene_related_fields = ["Gene", "SYMBOL", "BIOTYPE", "Feature", "Feature_type", "miRNA", "EXON", "INTRON", "CDS_position", "cDNA_position", "Protein_position", "Amino_acids", "DISTANCE", "HGNC_ID", "HGVSc", "HGVSp", "STRAND", "SWISSPROT", "Codons", "DOMAINS"]
		functional_consequence_fields = ['Consequence']
		pathogenicity_score_fields = ["CADD_RAW", "CADD_PHRED", "PolyPhen_score", "SIFT_score"]		
		pathogenicity_prediction_fields = ["IMPACT", "SIFT_pred", "PolyPhen_pred"]
		disease_association_fields = ['COSMIC_ID', 'CLIN_SIG']
		conservation_fields = []
		intervar_fields = []
	elif annot == 'annovar':
		gene_related_fields =  [key for key in mapping if 'Gene' in key or 'Uniprot' in key]
		functional_consequence_fields = ['Func_refGene', 'Func_ensGene', 'ExonicFunc_refGene', 'ExonicFunc_ensGene']
		pathogenicity_score_fields = sorted(['PolyPhen2_score', 'SIFT_score', 'FatHmm_score', 'PROVEAN_score', 'MutAss_score', 
								'EFIN_Swiss_Prot_Score', 'EFIN_HumDiv_Score', 'CADD_Phred_score', 'Carol_score', 
								'Condel_score', 'COVEC_WMV', 'PolyPhen2_score_transf', 'SIFT_score_transf', 
								'MutAss_score_transf', 'SIFT_converted_rankscore', 'Polyphen2_HDIV_score', 
								'Polyphen2_HDIV_rankscore', 'Polyphen2_HVAR_score', 'Polyphen2_HVAR_rankscore', 
								'LRT_score', 'LRT_converted_rankscore', 'MutationTaster_score', 
								'MutationTaster_converted_rankscore', 'MutationAssessor_score', 'REVEL', 'VEST3_score', 'VEST3_rankscore ',
								'MutationAssessor_score_rankscore', 'FATHMM_score', 'FATHMM_converted_rankscore', 
								'PROVEAN_score', 'PROVEAN_converted_rankscore', 'MetaSVM_score', 'MetaSVM_rankscore', 
								'MetaLR_score', 'MetaLR_rankscore', 'M-CAP_score', 'M-CAP_rankscore', 'CADD', 'CADD_raw', 
								'CADD_raw_rankscore', 'CADD_phred', 'CADD_PHRED', 'CADD_RAW', 'CADD13_RawScore',
								'CADD13_PHRED', 'DANN_score', 'DANN_rankscore', 'fathmm-MKL_coding_score', 
								'fathmm-MKL_coding_rankscore', 'Eigen', 'Eigen-raw', 'Eigen-PC-raw', 'GenoCanyon_score', 
								'GenoCanyon_score_rankscore', 'integrated_fitCons_score', 'integrated_fitCons_score_rankscore', 
								'integrated_confidence_value', 'dbscSNV_ADA_SCORE', 'dbscSNV_RF_SCORE']) 
		pathogenicity_prediction_fields =  sorted(['PolyPhen2_prediction', 'SIFT_prediction', 'FatHmm_prediction', 'PROVEAN_prediction', 
			'MutAss_prediction', 'MutAss_pred_transf', 'EFIN_Swiss_Prot_Predictio', 'EFIN_HumDiv_Prediction', 'CADD_prediction', 
								'Carol_prediction', 'Condel_pred', 'COVEC_WMV_prediction', 'PolyPhen2_pred_transf', 
							 	'SIFT_pred_transf', 'utAss_pred_transf', 'SIFT_pred', 'Polyphen2_HDIV_pred', 'Polyphen2_HVAR_pred',
								'LRT_pred', 'MutationTaster_pred', 'MutationAssessor_pred', 'FATHMM_pred', 'PROVEAN_pred', 
								'MetaSVM_pred', 'MetaLR_pred', 'M-CAP_pred', 'fathmm-MKL_coding_pred', 'EFIN_Swiss_Prot_Prediction'])
		minor_allele_freq_fields = [key for key in mapping if 'ExAC_' in key or '1000g2015aug_' in key or 'esp6500' in key] 
		
		disease_association_fields = sorted(['Associated_disease', 'gwasCatalog', 'COSMIC_ID', 'Tumor_site', 'CLIN_SIG', 'clinvar_CLNDBN', 'clinvar_CLINSIG']), 

		conservation_fields = sorted(['PhastCons_46V', 'PhyloP_100V', 'PhyloP_46V', 'PhastCons_100V', 'tfbsConsSites',
								'phyloP100way_vertebrate', 'phyloP100way_vertebrate_rankscore', 
								'phyloP20way_mammalian', 'phyloP20way_mammalian_rankscore', 
								'phastCons100way_vertebrate', 'phastCons100way_vertebrate_rankscore', 
								'phastCons20way_mammalian', 'phastCons20way_mammalian_rankscore', 
								'SiPhy_29way_logOdds', 'SiPhy_29way_logOdds_rankscore'])
		intervar_fields = sorted(["BS1", "BS2", "BS3", "BS4", "BA1", "BP1", "BP2","BP3", "BP4", "BP5", "BP6", "BP7", "PVS1", "PS1", "PS2", "PS3", "PS4", "PM1", "PM2", "PM3", "PM4", "PM5", "PM6", "PP1", "PP2", "PP3", "PP4", "PP5"])

	
		
	select_multiple_list = pathogenicity_prediction_fields
	
	# annotation independent fields
	# make the varaint related fields stand alone to keep the variable ordering
	for key in variant_related_fields:
		gui_mapping_var[key] = {"filters": [{"display_text": key, 
										"es_filter_type": "filter_term", 
										"form_type": "CharField",
										"widget_type": "TextInput"}
										],
							"panel": "Variant Related Information",
							"tab": "Basic"
							}
		if key == 'CHROM':
			gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
			gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
			gui_mapping_var[key]['filters'][0]["values"] = "get_from_es()"
			gui_mapping_var[key]['filters'][0]["es_filter_type"] = "filter_terms"
		elif key == 'FILTER':
			gui_mapping_var[key] = copy.deepcopy(gui_mapping_var[key])
			gui_mapping_var[key]['filters'][0]["display_text"] = 'FILTER'
			gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
			gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
			gui_mapping_var[key]['filters'][0]["values"] = "get_from_es()"
		elif key == 'VariantType':
			gui_mapping_var[key]['filters'][0]["form_type"] = "ChoiceField"
			gui_mapping_var[key]['filters'][0]['widget_type'] = "Select"
			gui_mapping_var[key]['filters'][0]['values'] = "get_from_es()"
		elif key == 'POS':
			gui_mapping_var[key]['filters'].append(copy.deepcopy(gui_mapping_var[key]['filters'][0]))
			gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
			gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(>=)"
			gui_mapping_var[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
			gui_mapping_var[key]['filters'][1]['in_line_tooltip'] = "(<=)"
		elif key == 'Variant':
			gui_mapping_var[key]['filters'][0]["in_line_tooltip"] = "(e.g. 1_115252204_C_T)"
		elif key == 'QUAL':
			gui_mapping_var[key]['filters'][0]["display_text"] = "GATK VQSR Score"
			gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(>=)"
			gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_range_gte"

	to_exclude = ['ID', 'Ensembl_Gene_ID', 'culprit','US', 'Presence_in_TD', 'Prob_N', 'Prob_P', 'Mutation_frequency', 'AA_pos', 'AA_sub', 'CCC', 'CSQ', 'END', 'DB', 'MQ0', 'ANNOVAR_DATE', 'NEGATIVE_TRAIN_SITE', 'POSITIVE_TRAIN_SITE', 'DS', 'ALLELE_END']
	keys = sorted([key for key in mapping if key not in gui_mapping_var and key not in to_exclude])
	for key in keys:
		if key in info_dict and 'Description' in info_dict[key]:
			tooltip = info_dict[key]['Description']
		else:
			tooltip = ""
			
		default_gui_mapping = {"filters": 
								[
									{
									"display_text": key,
									"es_filter_type": "filter_term", 
									"form_type": "CharField",
									"tooltip": tooltip,
									"widget_type": "TextInput"
									}
								],
								"panel": "Other",
								"tab": "Basic"
								}

		if key in variant_quality_related_fields or 'Allele Balance' in tooltip or 'Fraction of Reads' in tooltip or 'Homopolymer Run' in tooltip:
			gui_mapping_qc[key] = copy.deepcopy(default_gui_mapping)	
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				gui_mapping_qc[key]['filters'].append(copy.deepcopy(gui_mapping_qc[key]['filters'][0]))
				gui_mapping_qc[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_qc[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_qc[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_qc[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping_qc[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping_qc[key]['filters'][1]['in_line_tooltip'] = "(<=)"

			gui_mapping_qc[key]['panel']  = 'Variant Quality Metrix'
		elif key in minor_allele_freq_fields:
			gui_mapping_maf[key] = copy.deepcopy(default_gui_mapping)
			if mapping[key]['type'] == 'float':
				gui_mapping_maf[key]['filters'].append(copy.deepcopy(gui_mapping_maf[key]['filters'][0]))
				gui_mapping_maf[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_maf[key]['filters'][0]['es_filter_type'] = "filter_range_lt"
				gui_mapping_maf[key]['filters'][0]['in_line_tooltip'] = "(<)"
				gui_mapping_maf[key]['filters'][0]['widget_type'] = "TextInput"
				del gui_mapping_maf[key]['filters'][1]	
			else:
				gui_mapping_maf[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_maf[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_maf[key]['filters'][0]["values"] = "get_from_es()"
				
			if '1000g2015aug_' in key or key in ['MAX_AF', 'AA_AF', 'EA_AF', 'EUR_AF', 'AFR_AF', 'EAS_AF', 'ERR_AF', 'AMR_AF', 'SAS_AF']:
				gui_mapping_maf[key]['sub_panel'] = '1000 Genomes Project (Aug. 2015)'
			elif 'ExAC_' in key:
				gui_mapping_maf[key]['sub_panel'] = 'The Exome Aggregation Consortium (ExAC)'
			elif 'esp6500' in key:
				gui_mapping_maf[key]['sub_panel'] = 'Exome Sequencing Project'
			elif 'gnomAD' in key:
				gui_mapping_maf[key]['sub_panel'] = 'gnomAD Allele Frequency'

			gui_mapping_maf[key]['panel']  = 'Minor Allele Frequency (MAF)'
					
		elif key in summary_statistics_fields:
			gui_mapping_stat[key] = copy.deepcopy(default_gui_mapping)
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				gui_mapping_stat[key]['filters'].append(copy.deepcopy(gui_mapping_stat[key]['filters'][0]))
				gui_mapping_stat[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_stat[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_stat[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_stat[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping_stat[key]['filters'][1]['in_line_tooltip'] = "(<=)"
				gui_mapping_stat[key]['filters'][1]['form_type'] = "CharField"			
			gui_mapping_stat[key]['panel']  = 'Summary Statistics Information'
		elif key == 'sample': # annotation independent
			keys2 = [key for key in mapping['sample']['properties']]
			for key2 in keys2:
				gui_mapping_sample[key2] = copy.deepcopy(default_gui_mapping)
				gui_mapping_sample[key2]['panel'] = 'Sample Related Information'
				gui_mapping_sample[key2]['filters'][0]['display_text'] = key2
				gui_mapping_sample[key2]['filters'][0]['path'] = key
				
				if key2 == 'AD':
		
					del gui_mapping_sample[key2]
				elif key2 in ['Sample_ID', 'GT', 'PGT', 'Family_ID', 'Father_ID', 'Mother_ID', 'Sex', 'Phenotype', 'Mother_Genotype', 'Father_Genotype', 'Mother_Phenotype', 'Father_Genotype']:
					gui_mapping_sample[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"		
					gui_mapping_sample[key2]['filters'][0]['values'] = "get_from_es()"
					gui_mapping_sample[key2]['filters'][0]["in_line_tooltip"] = ""
					gui_mapping_sample[key2]['filters'][0]['widget_type'] = "SelectMultiple"
					gui_mapping_sample[key2]['filters'][0]['form_type'] = "MultipleChoiceField"
				elif key2 == 'group':
					gui_mapping_sample[key2]['filters'][0]['widget_type'] = "Select"
					gui_mapping_sample[key2]['filters'][0]['form_type'] = "ChoiceField"
				elif mapping['sample']['properties'][key2]['type'] == 'integer' or mapping['sample']['properties'][key2]['type'] == 'float':
					if key2 == 'PL': # keep it as string type
						gui_mapping_sample[key2]['filters'][0]["tooltip"] = format_dict[key2]['Description']
						
					else:
						gui_mapping_sample[key2]['filters'][0]["es_filter_type"] = "nested_filter_range_gte"
						gui_mapping_sample[key2]['filters'][0]["in_line_tooltip"] = "(>=)"
						if key2.startswith('AD_'):
							gui_mapping_sample[key2]['filters'][0]["tooltip"] = format_dict['AD']['Description']
						else:
							gui_mapping_sample[key2]['filters'][0]["tooltip"] = format_dict[key2]['Description']
						 
		elif key.startswith("dbSNP"):
			gui_mapping_var['dbSNP_ID'] = copy.deepcopy(default_gui_mapping)
			gui_mapping_var['dbSNP_ID']['filters'].append(copy.deepcopy(gui_mapping_var['dbSNP_ID']['filters'][0]))
			gui_mapping_var['dbSNP_ID']['filters'][0]['display_text'] = 'dbSNP ID'
			gui_mapping_var['dbSNP_ID']['filters'][0]['widget_type'] = "UploadField"
			gui_mapping_var['dbSNP_ID']['filters'][0]['form_type'] = "CharField"
			gui_mapping_var['dbSNP_ID']['filters'][0]['es_filter_type'] = "filter_terms"
			gui_mapping_var['dbSNP_ID']['filters'][1]['display_text'] = 'Limit Variants to dbSNP'
			gui_mapping_var['dbSNP_ID']['filters'][1]['widget_type'] = "Select"
			gui_mapping_var['dbSNP_ID']['filters'][1]['es_filter_type'] = "filter_exists"
			gui_mapping_var['dbSNP_ID']['filters'][1]['form_type'] = "ChoiceField"
			gui_mapping_var['dbSNP_ID']['panel'] = "Variant Related Information"
#	else:
#			if key not in pathogenicity_score_fields and key not in disease_association_fields:
#				gui_mapping_others[key] = copy.deepcopy(default_gui_mapping)

	to_exclude.append('sample')
	
	keys = sorted([key for key in mapping if key not in summary_statistics_fields and key not in variant_quality_related_fields and key not in minor_allele_freq_fields and key not in variant_related_fields and key not in to_exclude])
	if annot == 'vep':
		for key in keys:
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
			
			default_gui_mapping = {"filters": 
								[
									{
									"display_text": key,
									"es_filter_type": "filter_term", 
									"form_type": "CharField",
									"tooltip": tooltip,
									"widget_type": "TextInput"
									}
								],
								"panel": "Other",
								"tab": "Basic"
								}
			if key == 'CSQ_nested':
				seen = {}
				keys = [key for key in mapping['CSQ_nested']['properties']]
				for key2 in functional_consequence_fields:
					if key2 in keys:
						gui_mapping_func[key2] = copy.deepcopy(default_gui_mapping)
						gui_mapping_func[key2]['filters'][0]['display_text'] = key2
						gui_mapping_func[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
						gui_mapping_func[key2]['filters'][0]['form_type'] = "MultipleChoiceField"
						gui_mapping_func[key2]['filters'][0]["widget_type"] = "SelectMultiple" 
						gui_mapping_func[key2]['filters'][0]['values'] = "get_from_es()"
						gui_mapping_func[key2]['filters'][0]['path'] = key
						gui_mapping_func[key2]['panel']  = 'Functional Consequences'
						seen[key2] = {}
				for key2 in gene_related_fields:
					if key2 in keys:
						gui_mapping_gene[key2] = copy.deepcopy(default_gui_mapping)
						gui_mapping_gene[key2]['filters'][0]['display_text'] = key2
						if key2 in ['Gene', 'SYMBOL']:
							gui_mapping_gene[key2]['filters'][0]['widget_type'] = "UploadField"
							gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"	
							if key2 == 'Gene':
								gui_mapping_gene[key2]['filters'][0]['display_text'] = 'Entrez Gene ID'
						elif key2 in ['Feature_type', 'BIOTYPE', 'DOMAINS']:
							gui_mapping_gene[key2]['filters'][0]['values'] = "get_from_es()"
							gui_mapping_gene[key2]['filters'][0]["form_type"] = "MultipleChoiceField"
							gui_mapping_gene[key2]['filters'][0]["widget_type"] = "SelectMultiple"
						elif key2 == 'DISTANCE':
							gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_range_lte"
							gui_mapping_gene[key2]['filters'][0]['in_line_tooltip'] = "(<=)"
						elif key2.endswith('_position'):
							gui_mapping_gene[key2]['filters'][0]['in_line_tooltip'] = "(=)"

						gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
						gui_mapping_gene[key2]['filters'][0]['path'] = key
						gui_mapping_gene[key2]['panel']  = 'Gene Related Information'
						seen[key2] = {}
				for key2 in pathogenicity_prediction_fields:
					if key2 in keys:
						gui_mapping_patho_p[key2] = copy.deepcopy(default_gui_mapping)
						gui_mapping_patho_p[key2]['filters'][0]['display_text'] = key2
						gui_mapping_patho_p[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
						gui_mapping_patho_p[key2]['filters'][0]['form_type'] = "MultipleChoiceField"
						gui_mapping_patho_p[key2]['filters'][0]["widget_type"] = "SelectMultiple"
						gui_mapping_patho_p[key2]['filters'][0]['path'] = key
						gui_mapping_patho_p[key2]['filters'][0]['values'] = "get_from_es()"
						gui_mapping_patho_p[key2]['panel']  = 'Pathogenicity Predictions'
						gui_mapping_patho_p[key2]['sub_panel']  = 'Predictions'
						seen[key2] = {}	
				for key2 in pathogenicity_score_fields:
					if key2 in keys: 
						gui_mapping_patho_s[key2] = copy.deepcopy(default_gui_mapping)
						gui_mapping_patho_s[key2]['filters'][0]['display_text'] = key2
						gui_mapping_patho_s[key2]['filters'][0]['form_type'] = "CharField"
						gui_mapping_patho_s[key2]['filters'][0]['path'] = key
						
						if key2 == 'SIFT_score':
							gui_mapping_patho_s[key2]['filters'][0]['es_filter_type'] = "nested_filter_range_lte"
							gui_mapping_patho_s[key2]['filters'][0]['in_line_tooltip'] = "(<=)"
						else:
							gui_mapping_patho_s[key2]['filters'][0]['es_filter_type'] = "nested_filter_range_gte"
							gui_mapping_patho_s[key2]['filters'][0]['in_line_tooltip'] = "(>=)"
							
						gui_mapping_patho_s[key2]['panel']  = 'Pathogenicity Predictions'
						gui_mapping_patho_s[key2]['sub_panel']  = 'Scores'
						seen[key2] = {}
						
				unmapped = [key for key in keys if key not in seen]
				for key2 in unmapped:
					gui_mapping_others[key2] = copy.deepcopy(default_gui_mapping)
			elif key == 'COSMIC_ID':
				gui_mapping_disease[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_disease[key]['filters'].append(copy.deepcopy(gui_mapping_disease[key]['filters'][0]))
				gui_mapping_disease[key]['filters'][0]['display_text'] = 'COSMIC ID'
				gui_mapping_disease[key]['filters'][0]['widget_type'] = "UploadField"
				gui_mapping_disease[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_disease[key]['filters'][1]['display_text'] = 'Limit Variants to COSMIC'
				gui_mapping_disease[key]['filters'][1]['widget_type'] = "Select"
				gui_mapping_disease[key]['filters'][1]['es_filter_type'] = "filter_exists"
				gui_mapping_disease[key]['filters'][1]['form_type'] = "ChoiceField"
				gui_mapping_disease[key]['panel'] = 'Disease Associations'
			elif key == 'CLIN_SIG':
				gui_mapping_disease[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_disease[key]['filters'][0]['display_text'] = 'Clinvar Significance'
				gui_mapping_disease[key]['filters'][0]['widget_type'] = "SelectMultiple"
				gui_mapping_disease[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_disease[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_disease[key]['panel'] = 'Disease Associations'
				
			else:
				if key in pathogenicity_score_fields or key.startswith('CADD') or key.startswith('GERP') or key.startswith('DANN'):
					gui_mapping_patho_s[key] = copy.deepcopy(default_gui_mapping)
					if '++' in key:
						oldkey = key
						key = key.replace('++', 'plusplus')
						gui_mapping_patho_s[key] = copy.deepcopy(gui_mapping_patho_s[oldkey])
						del gui_mapping_patho_s[oldkey]
					
					gui_mapping_patho_s[key]['filters'][0]['form_type'] = "CharField"
					gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
					gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(>=)"
					gui_mapping_patho_s[key]['filters'][0]['widget_type'] = "TextInput"
						
					gui_mapping_patho_s[key]['panel'] = 'Pathogenicity Predictions'
					gui_mapping_patho_s[key]['sub_panel'] = 'Scores'
					
				elif key in disease_association_fields:
					gui_mapping_disease[key] = copy.deepcopy(default_gui_mapping)
					gui_mapping_disease[key]['filters'][0]['display_text'] = key
					gui_mapping_disease[key]['filters'][0]['form_type'] = "MultipleChoiceField"
					gui_mapping_disease[key]['filters'][0]["widget_type"] = "SelectMultiple"
					gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
					gui_mapping_disease[key]['filters'][0]['values'] = "get_from_es()"
					gui_mapping_disease[key]['panel'] = 'Disease Associations'
			
	elif annot == 'annovar':
		for key in sorted([key for key in mapping ]):
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
			
			default_gui_mapping = {"filters": 
								[
									{
									"display_text": key,
									"es_filter_type": "filter_term", 
									"form_type": "CharField",
									"tooltip": tooltip,
									"widget_type": "TextInput"
									}
								],
								"panel": "Other",
								"tab": "Basic"
								}
			if key in functional_consequence_fields:
				gui_mapping_func[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_func[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_func[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				gui_mapping_func[key]['filters'][0]["widget_type"] = "SelectMultiple" 
				gui_mapping_func[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_func[key]['panel']  = 'Functional Consequences'
				
				if 'ensGene' in key:
					gui_mapping_func[key]['sub_panel']  = 'Ensembl'
				elif 'refGene' in key:
					gui_mapping_func[key]['sub_panel']  = 'RefSeq'
			elif key in gene_related_fields or key.startswith('Gene'):
				gui_mapping_gene[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_gene[key]['panel'] = 'Gene Related Information'
				
				if key in ['AAChange_ensGene', 'AAChange_refGene']:
					for key2 in mapping[key]['properties']:
						gui_mapping_gene[key2] = copy.deepcopy(gui_mapping_gene[key])
						gui_mapping_gene[key2]['filters'][0]['tooltip'] = ''
						if key2.startswith('exon_id'):
							gui_mapping_gene[key2]['filters'][0]['display_text'] = 'Exon ID'
							gui_mapping_gene[key2]['filters'][0]['form_type'] = "MultipleChoiceField"
							gui_mapping_gene[key2]['filters'][0]["widget_type"] = "SelectMultiple"
							gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
						elif key2.startswith('cdna'):
							gui_mapping_gene[key2]['filters'][0]['display_text'] = 'cDNA Change'
							gui_mapping_gene[key2]['filters'][0]['in_line_tooltip'] =  'c.A727G'
							gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_term"
							gui_mapping_gene[key2]['filters'][0]['form_type'] = "CharField"
							gui_mapping_gene[key2]['filters'][0]['widget_type'] = "TextInput"
						elif key2.startswith('aa'):
							gui_mapping_gene[key2]['filters'][0]['display_text'] = 'Amino Acid Change'
							gui_mapping_gene[key2]['filters'][0]['in_line_tooltip'] = 'p.T243A'
							gui_mapping_gene[key2]['filters'][0]['form_type'] = "CharField"
							gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_term"
							gui_mapping_gene[key2]['filters'][0]['widget_type'] = "TextInput"
						elif key2 == 'RefSeq':
							gui_mapping_gene[key2]['filters'][0]['display_text'] = "RefGene ID"
							gui_mapping_gene[key2]['filters'][0]['in_line_tooltip'] = "(e.g. NM_133378)"
							gui_mapping_gene[key2]['filters'][0]['widget_type'] = "UploadField"
							gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
						elif key2 == 'EnsembleTranscriptID':
							gui_mapping_gene[key2]['filters'][0]['display_text'] = "Ensembl transcript ID"
							gui_mapping_gene[key2]['filters'][0]['in_line_tooltip'] = "(e.g. ENST00000460472)"
							gui_mapping_gene[key2]['filters'][0]['widget_type'] = "UploadField"
							gui_mapping_gene[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
						gui_mapping_gene[key2]['filters'][0]['path'] = key
	
						if key == 'AAChange_ensGene':
							gui_mapping_gene[key2]['sub_panel'] = 'Ensembl Gene'
						else: 
							gui_mapping_gene[key2]['sub_panel'] = 'NCBI Gene'
	
					del gui_mapping_gene[key]
				else:
					if 'refGene' in key:
						gui_mapping_gene[key]['sub_panel'] = 'NCBI Gene'
					elif 'ensGene' in key or key == 'Ensembl_Gene_ID':
						gui_mapping_gene[key]['sub_panel'] =  'Ensembl Gene'
					else:
						gui_mapping_gene[key]['sub_panel'] = ''
						
					if key in ['Gene_refGene', 'Gene_ensGene', 'Ensembl_Gene_ID']:
						gui_mapping_gene[key]['filters'][0]['widget_type'] = "UploadField"
						gui_mapping_gene[key]['filters'][0]['es_filter_type'] = "filter_terms"
						gui_mapping_gene[key]['filters'][0]['form_type'] = "MultipleChoiceField"

			elif key in conservation_fields:
				gui_mapping_conserv[key] = copy.deepcopy(default_gui_mapping)
				if key == "tfbsConsSites":
					gui_mapping_conserv[key]['filters'][0]["values"] = "get_from_es()"
				else:
					gui_mapping_conserv[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
					gui_mapping_conserv[key]['filters'][0]['in_line_tooltip'] = "(>=)"
	
				gui_mapping_conserv[key]['panel'] = 'Conservation Scores'
			elif key in pathogenicity_prediction_fields or '_prediction' in key or '_pred' in key:
				gui_mapping_patho_p[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_patho_p[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_patho_p[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_patho_p[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_patho_p[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				gui_mapping_patho_p[key]['panel'] = 'Pathogenicity Predictions'
				gui_mapping_patho_p[key]['sub_panel'] = 'Predictions'
			elif key in pathogenicity_score_fields or key.startswith('GERP') or key.startswith('DANN'):
				gui_mapping_patho_s[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_patho_s[key]['filters'][0]['display_text'] = key
#	if '++' in key:
#					oldkey = key
#					key = key.replace('++', 'plusplus')
#					gui_mapping_patho_s[key] = gui_mapping_patho_s[oldkey]
#	del gui_mapping_patho_s[oldkey]
					
				gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				if key.startswith('SIFT'):
					gui_mapping_patho_s[key]['filters'][0]['es_filter_type'] = "filter_range_lte"
					gui_mapping_patho_s[key]['filters'][0]['in_line_tooltip'] = "(<=)"
					
				gui_mapping_patho_s[key]['panel'] = 'Pathogenicity Predictions'
				gui_mapping_patho_s[key]['sub_panel'] = 'Scores'
				
			elif key in disease_association_fields:
				gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
				if key != 'CLNACC':
					gui_mapping_disease[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_disease[key]['panel'] = 'Disease Associations'
	
			elif key in intervar_fields:
				gui_mapping_intvar[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_intvar[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_intvar[key]['panel'] = 'ACMG/AMP InterVar Criteria'
			elif key == 'cytoBand':
				gui_mapping_var[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_var[key]['filters'][0]["values"] = "get_from_es()"
				gui_mapping_var[key]['panel'] = "Variant Related Information"	
			elif key.startswith('snp138NonFlagged'):
				gui_mapping_disease[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_disease[key]['filters'].append(copy.deepcopy(gui_mapping_disease[key]['filters'][0]))
				gui_mapping_disease[key]['filters'][0]['display_text'] = key
				gui_mapping_disease[key]['filters'][0]['widget_type'] = "UploadField"
				gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
	
				gui_mapping_disease[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
				gui_mapping_disease[key]['filters'][1]['widget_type'] = "Select"
				gui_mapping_disease[key]['filters'][1]['es_filter_type'] = "filter_exists"
				gui_mapping_disease[key]['filters'][1]['form_type'] = "ChoiceField"
				gui_mapping_disease[key]['panel'] = 'Disease Associations'
				
			elif key.startswith('avsnp'):
				gui_mapping_var[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_var[key]['filters'].append(copy.deepcopy(gui_mapping_var[key]['filters'][0]))
				gui_mapping_var[key]['filters'][0]['display_text'] = key
				gui_mapping_var[key]['filters'][0]['widget_type'] = "UploadField"
				gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_terms"
	
				gui_mapping_var[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
				gui_mapping_var[key]['filters'][1]['widget_type'] = "Select"
				gui_mapping_var[key]['filters'][1]['es_filter_type'] = "filter_exists"
				gui_mapping_var[key]['filters'][1]['form_type'] = "ChoiceField"
				gui_mapping_var[key]['panel'] = "Variant Related Information"

	result = OrderedDict()
	
	for dict_ in [gui_mapping_var, gui_mapping_stat, gui_mapping_qc, gui_mapping_gene, gui_mapping_func, gui_mapping_maf, gui_mapping_conserv, gui_mapping_patho_p, gui_mapping_patho_s, gui_mapping_intvar, gui_mapping_disease, gui_mapping_sample, gui_mapping_others]:
		result.update(dict_)
	
	outputfile = os.path.join("../config", type_name + '_gui_config.json')
	with open(outputfile, 'w') as f:
		json.dump(result, f, sort_keys=False, indent=4, ensure_ascii=True)


if __name__ == '__main__':
	vcf_info_file = 'trio.trim.hg19_multianno_vcf_info.json'
	mapping_file = 'scripts/trio_mapping.json'
	type_name = 'trio_'
	annot = 'annovar'
	make_gui(vcf_info_file, mapping_file, type_name, annot)
#	make_gui(vcf_info_file, mapping_file, type_name, annot)
