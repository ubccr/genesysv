import json
import copy

mapping = json.load(open("scripts/simwescase_annovar_mapping.json", 'r'))
mapping = mapping['simwescase_annovar_']['properties']
vcf_info = json.load(open("20170419_SIM_WES_CASE.hg19_multianno_vcf_info.json", 'r'))

annot = 'vep'

def make_gui(vcf_info, mapping):
	info_dict = vcf_info['info_dict']
	format_dict = vcf_info['format_dict']
	gui_mapping = {}
	with open("info_dict.json", 'w') as f:
		json.dump(info_dict, f, sort_keys=True, indent=4, ensure_ascii=True)

	exist_list = ['']

	# section categories
	variant_related_fields = set(['CHROM', 'POS', 'REF', 'ALT', 'QUAL', 'FILTER', 'VariantID', 'dbSNP_ID', 'VariantType', 'cytoBand', 'MAX_AF_POPS'])

	variant_quality_related_fields = set(['SB', 'OND', 'InbreedingCoeff', 'MQRankSum', 'MQ0', 'BaseQRankSum', 'HWP', 'FS', 'FS','ClippingRankSum', 'MQ', 'QD', 'ReadPosRankSum', 'HaplotypeScore', 'VQSLOD', 'SOR'])
	
	functional_consequence_fields = set(['Func_refGene', 'Func_ensGene', 'ExonicFunc_refGene', 'ExonicFunc_ensGene'])
	minor_allele_freq_fields = set([key for key in mapping if '_AF' in key])
	if 'CSQ_nested' in mapping:
		minor_allele_freq_fields = minor_allele_freq_fields | set([key for key in mapping['CSQ_nested']['properties'] if '_AF' in key or 'gnomAD_' in key])
	
		gene_related_fields = set([key for key in mapping['CSQ_nested']['properties']]) - minor_allele_freq_fields
		
	if 'AAChange_refGene' in mapping or 'AAChange_ensGene' in mapping:
		gene_related_fields = set({key for key in mapping['AAChange_refGene']}) | set({key for key in mapping['AAChange_ensGene']})
		
	gene_related_fields = gene_related_fields | set(['wgRna', 'targetScan', 'Gene_symbol'])
	gene_related_fields = gene_related_fields | set({key for key in mapping if 'Gene' in key or 'Uniprot' in key})
	conservation_fields = set(['PhastCons_46V', 'PhyloP_100V', 'PhastCons_100V', 'tfbsConsSites',
								'phyloP100way_vertebrate', 'phyloP100way_vertebrate_rankscore', 
								'phyloP20way_mammalian', 'phyloP20way_mammalian_rankscore', 
								'phastCons100way_vertebrate', 'phastCons100way_vertebrate_rankscore', 
								'phastCons20way_mammalian', 'phastCons20way_mammalian_rankscore', 
								'SiPhy_29way_logOdds', 'SiPhy_29way_logOdds_rankscore'])
	pathogenicity_score_fields = set(['PolyPhen2_score', 'SIFT_score', 'FatHmm_score', 'PROVEAN_score', 'MutAss_score', 
								'EFIN_Swiss_Prot_Score', 'EFIN_HumDiv_Score', 'CADD_Phred_score', 'Carol_score', 
								'Condel_score', 'COVEC_WMV', 'PolyPhen2_score_transf', 'SIFT_score_transf', 
								'MutAss_score_transf', 'SIFT_converted_rankscore', 'Polyphen2_HDIV_score', 
								'Polyphen2_HDIV_rankscore', 'Polyphen2_HVAR_score', 'Polyphen2_HVAR_rankscore', 
								'LRT_score', 'LRT_converted_rankscore', 'MutationTaster_score', 
								'MutationTaster_converted_rankscore', 'MutationAssessor_score', 
								'MutationAssessor_score_rankscore', 'FATHMM_score', 'FATHMM_converted_rankscore', 
								'PROVEAN_score', 'PROVEAN_converted_rankscore', 'MetaSVM_score', 'MetaSVM_rankscore', 
								'MetaLR_score', 'MetaLR_rankscore', 'M-CAP_score', 'M-CAP_rankscore', 'CADD', 'CADD_raw', 
								'CADD_raw_rankscore', 'CADD_phred', 'CADD_PHRED', 'CADD_RAW', 'CADD13_RawScore',
								'CADD13_PHRED', 'DANN_score', 'DANN_rankscore', 'fathmm-MKL_coding_score', 
								'fathmm-MKL_coding_rankscore Eigen-raw', 'Eigen-PC-raw', 'GenoCanyon_score', 
								'GenoCanyon_score_rankscore integrated_fitCons_score', 'integrated_fitCons_score_rankscore', 
								'integrated_confidence_value', 'dbscSNV_ADA_SCORE', 'dbscSNV_RF_SCORE']) 
	pathogenicity_prediction_fields =  set(['PolyPhen2_prediction', 'SIFT_prediction', 'FatHmm_prediction', 'PROVEAN_prediction', 
								'MutAss_prediction', 'EFIN_Swiss_Prot_Predictio', 'EFIN_HumDiv_Prediction', 'CADD_prediction', 
								'Carol_prediction', 'Condel_pred', 'COVEC_WMV_prediction', 'PolyPhen2_pred_transf', 
							 	'SIFT_pred_transf', 'utAss_pred_transf', 'SIFT_pred', 'Polyphen2_HDIV_pred', 'Polyphen2_HVAR_pred',
								'LRT_pred', 'MutationTaster_pred', 'MutationAssessor_pred', 'FATHMM_pred', 'PROVEAN_pred', 
								'MetaSVM_pred', 'MetaLR_pred', 'M-CAP_pred', 'fathmm-MKL_coding_pred'])
	minor_allele_freq_fields = set({key for key in mapping if 'ExAC_' in key or '1000g2015aug_' in key or 'esp6500' in key}) 

	summary_statistics_fields = set(['AC', 'AF', 'AN', 'MLEAC', 'MLEAF', 'DP', 'FS', 'GQ_MEAN', 'GQ_STDDEV' ])
	summary_statistics_fields_case =  {item + '_case' for item in summary_statistics_fields}
	summary_statistics_fields_control = {item + '_control' for item in summary_statistics_fields}
	summary_statistics_fields = summary_statistics_fields | summary_statistics_fields_case
	summary_statistics_fields = summary_statistics_fields | summary_statistics_fields_control
		
	disease_association_fields = set(['Associated_disease', 'gwasCatalog', 'clinvar_CLNDBN', 'clinvar_CLINSIG', 
								'CLINSIG', 'CLNACC', 'CLNDBN', 'CLNDSDB', 'CLNDSDBID', 'COSMIC_ID'])

	select_multiple_list = pathogenicity_prediction_fields

	for key in variant_related_fields:
		gui_mapping[key] = {"filters": [{"display_text": key, 
										"es_filter_type": "filter_term", 
										"form_type": "CharField",
										"in_line_tooltip": "(e.g. 1-115252204-C-T)",
										"values": "get_from_es()",
										"widget_type": "TextInput"}
										],
							"panel": "Variant Related Information",
							"tab": "Basic"
							}
		if key in ['CHROM', 'FILTER', 'cytoBand', 'MAX_AF_POPS']:
			gui_mapping[key]['filters'][0]["widget_type"] = "SelectMultiple"
			gui_mapping[key]['filters'][0]["form_type"] = "MultipleChoiceField"
		elif key == 'VariantType':
			gui_mapping[key]['filters'][0]["form_type"] = "ChoiceField"
			gui_mapping[key]['filters'][0]['widget_type'] = "Select"
		elif key == 'POS':
			del gui_mapping[key]['filters'][0]["values"]
			gui_mapping[key]['filters'].append(copy.deepcopy(gui_mapping[key]['filters'][0]))
			gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
			gui_mapping[key]['filters'][0]['in_line_tooltip'] = "(>=)"
			gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
			gui_mapping[key]['filters'][1]['in_line_tooltip'] = "(<=)"
		

	for key in info_dict:
		if key in ['CSQ', 'END', 'DB', 'MQ0', 'ANNOVAR_DATE', 'NEGATIVE_TRAIN_SITE', 'POSITIVE_TRAIN_SITE', 'DS', 'ALLELE_END']:
			continue

		gui_mapping[key] = {"filters": [{"display_text": key, 
										"es_filter_type": "es_filter_type", 
										"form_type": "form_type",
										"tooltip": info_dict[key]['Description'],
										"in_line_tooltip": "in_line_tooltip",
										"values": "get_from_es()",
										"widget_type": "TextInput"}
										],
							"panel": "Other",
							"tab": "Basic"
							}
		if key in variant_related_fields:
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				del gui_mapping[key]['filters'][0]["values"]
				gui_mapping[key]['filters'].append(copy.deepcopy(gui_mapping[key]['filters'][0]))
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping[key]['filters'][1]['in_line_tooltip'] = "(<=)"
				gui_mapping[key]['panel'] = 'Variant Related Information'
				
			elif mapping[key]['type'] == 'keyword':
				if key in select_multiple_list:
					gui_mapping[key]['filters'][0]["widget_type"] = "SelectMultiple"
					gui_mapping[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				elif 'avsnp' in key or 'dbSNP_ID' in key:
					gui_mapping[key]['filters'].append(gui_mapping[key]['filters'][0])
					gui_mapping[key]['filters'][0]['display_text'] = key + ' ID'
					gui_mapping[key]['filters'][0]['widget_type'] = "UploadField"
					gui_mapping[key]['filters'][0]['form_type'] = "CharField"
					gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_terms"

					gui_mapping[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
					gui_mapping[key]['filters'][1]['widget_type'] = "Select"
					gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_exists"
					gui_mapping[key]['filters'][1]['form_type'] = "ChoiceField"
				
		elif key in variant_quality_related_fields or 'Allele Balance' in info_dict[key]['Description'] or 'Fraction of Reads' in info_dict[key]['Description'] or 'Homopolymer Run' in info_dict[key]['Description']:
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				del gui_mapping[key]['filters'][0]["values"]
				gui_mapping[key]['filters'].append(copy.deepcopy(gui_mapping[key]['filters'][0]))
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping[key]['filters'][1]['in_line_tooltip'] = "(<=)"

			gui_mapping[key]['panel']  = 'Variant Quality Metrix'
		elif key in minor_allele_freq_fields:
			if mapping[key]['type'] == 'float':
				del gui_mapping[key]['filters'][0]["values"]
				gui_mapping[key]['filters'].append(copy.deepcopy(gui_mapping[key]['filters'][0]))
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_range_lt"
				gui_mapping[key]['filters'][0]['in_line_tooltip'] = "(<)"
				gui_mapping[key]['filters'][0]['widget_type'] = "TextInput"
				del gui_mapping[key]['filters'][1]	
			gui_mapping[key]['panel']  = 'Minor Allele Frequency (MAF)'
				
			if '1000g2015aug_' in key or 'AFR_AF' in key or 'EAS_AF' in key or 'ERR_AF' in key or 'AMR_AF' in key or 'SAS_AF':
				gui_mapping[key]['sub_panel'] = '1000 Genomes Project (Aug. 2015)'
			elif 'ExAC_' in key:
				gui_mapping[key]['sub_panel'] = 'The Exome Aggregation Consortium (ExAC)'
			elif 'esp6500' in key:
				gui_mapping[key]['sub_panel'] = 'Exome Sequencing Project'
			elif 'gnomAD' in kye:
				gui_mapping[key]['sub_panel'] = 'gnomAD Allele Frequency'
					
		elif key in summary_statistics_fields:
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				del gui_mapping[key]['filters'][0]["values"]
				gui_mapping[key]['filters'].append(copy.deepcopy(gui_mapping[key]['filters'][0]))
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping[key]['filters'][1]['in_line_tooltip'] = "(<=)"
				gui_mapping[key]['filters'][1]['form_type'] = "CharField"			
			gui_mapping[key]['panel']  = 'Summary Statistics Information'
		elif key in gene_related_fields or 'Gene' in key:
			if key in ['AAChange_ensGene', 'AAChange_refGene']:
				for key2 in mapping[key]['properties']:
					gui_mapping[key2] = gui_mapping[key]
					if key2.startswith('exon_id'):
						gui_mapping[key2]['filters'][0]['display_text'] = 'Exon ID'
						gui_mapping[key2]['filters'][0]['form_type'] = "MultipleChoiceField"
						gui_mapping[key2]['filters'][0]["widget_type"] = "SelectMultiple"
						gui_mapping[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
					elif key2.startswith('cdna'):
						gui_mapping[key2]['filters'][0]['display_text'] = 'cDNA Change'
						gui_mapping[key2]['filters'][0]['in_line_tooltip'] =  'c.A727G'
						gui_mapping[key2]['filters'][0]['es_filter_type'] = "nested_filter_term"
						gui_mapping[key2]['filters'][0]['form_type'] = "CharField"
						del gui_mapping[key2]['filters'][0]['values']
					elif key2.startswith('aa'):
						gui_mapping[key2]['filters'][0]['display_text'] = 'Amino Acid Change'
						gui_mapping[key2]['filters'][0]['in_line_tooltip'] = 'p.T243A'
						gui_mapping[key2]['filters'][0]['form_type'] = "CharField"
#						del gui_mapping[key2]['filters'][0]['values']
					elif key2 == 'RefSeq':
						gui_mapping[key2]['filters'][0]['display_text'] = "RefGene ID"
						gui_mapping[key2]['filters'][0]['in_line_tooltip'] = "(e.g. NM_133378)"
						gui_mapping[key2]['filters'][0]['widget_type'] = "UploadField"
						gui_mapping[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
#del gui_mapping[key2]['filters'][0]['values']
					elif key2 == 'EnsembleTranscriptID':
						gui_mapping[key2]['filters'][0]['display_text'] = "Ensembl transcript ID"
						gui_mapping[key2]['filters'][0]['in_line_tooltip'] = "(e.g. ENST00000460472)"
						gui_mapping[key2]['filters'][0]['widget_type'] = "UploadField"
						gui_mapping[key2]['filters'][0]['es_filter_type'] = "nested_filter_terms"
#del gui_mapping[key2]['filters'][0]['values']
					gui_mapping[key2]['filters'][0]['path'] = key
					 
					gui_mapping[key2]['panel'] = 'Gene Related Information'

					if key == 'AAChange_ensGene':
						gui_mapping[key2]['sub_panel'] = 'Ensembl Gene'
					elif key == 'AAChange_refGene':
						gui_mapping[key2]['sub_panel'] = 'refSeq Gene'

				del gui_mapping[key]
			elif key in ['Func_ensGene', 'Func_refGene']:
				gui_mapping[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping[key]['filters'][0]['form_type'] = "MultipleChoiceField"
			else:
				pass
		elif key in conservation_fields:
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				del gui_mapping[key]['filters'][0]["values"]
				gui_mapping[key]['filters'].append(copy.deepcopy(gui_mapping[key]['filters'][0]))
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping[key]['filters'][1]['in_line_tooltip'] = "(<=)"

			gui_mapping[key]['panel'] = 'Conservation Scores'
		elif key in pathogenicity_prediction_fields:
			gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_terms"
			del gui_mapping[key]['filters'][0]['in_line_tooltip']

			if key in select_multiple_list:
				gui_mapping[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping[key]['filters'][0]['form_type'] = "MultipleChoiceField"
			gui_mapping[key]['panel'] = 'Pathognicity Predictions'
			gui_mapping[key]['sub_panel'] = 'Pathognicity Predictions'
		elif key in pathogenicity_score_fields or key.startswith('CADD') or key.startswith('GERP') or key.startswith('DANN'):
			if '++' in key:
				oldkey = key
				key = key.replace('++', 'plusplus')
				gui_mapping[key] = gui_mapping[oldkey]
				del gui_mapping[oldkey]
				del gui_mapping[key]['filters'][0]['values']
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				gui_mapping[key]['filters'].append(copy.deepcopy(gui_mapping[key]['filters'][0]))
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping[key]['filters'][1]['in_line_tooltip'] = "(<=)"
				
			gui_mapping[key]['panel'] = 'Pathognicity Predictions'
			gui_mapping[key]['sub_panel'] = 'Pathognicity Prediction Scores'
			
		elif key in disease_association_fields:
			if 'COSMIC' in key:
				gui_mapping[key]['filters'].append(gui_mapping[key]['filters'][0])
				gui_mapping[key]['filters'][0]['display_text'] = key + ' ID'
				gui_mapping[key]['filters'][0]['widget_type'] = "UploadField"
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_terms"

				gui_mapping[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
				gui_mapping[key]['filters'][1]['widget_type'] = "Select"
				gui_mapping[key]['filters'][1]['es_filter_type'] = "filter_exists"
				gui_mapping[key]['filters'][1]['form_type'] = "ChoiceField"
			else:
				gui_mapping[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping[key]['filters'][0]['es_filter_type'] = "filter_terms"
			del gui_mapping[key]['filters'][0]['in_line_tooltip']		
			gui_mapping[key]['panel'] = 'Disease Associations'

					
	# sample related fields
	gui_mapping2 = {}
	for key in format_dict:
		
		gui_mapping2[key] = {"filters": [{"display_text": key, 
										"es_filter_type": "nested_filter_range_gte", 
										"form_type": "CharField",
										"tooltip": format_dict[key]['Description'],
										"in_line_tooltip": "(>=)",
										"values": "get_from_es()",
										"path": "sample",
										"widget_type": "TextInput"}
										],
							"panel": "Sample Related Information",
							"tab": "Basic"
							}
		if format_dict[key]['type'] == 'integer':
			del gui_mapping2[key]['filters'][0]['values']
			if key == 'PL':
				gui_mapping2[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"		
				gui_mapping2[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping2[key]['filters'][0]['form_type'] = "CharField"
				del gui_mapping2[key]['filters'][0]['in_line_tooltip']
			if key == 'AD':
				gui_mapping2[key + '_ref'] = gui_mapping2[key]
				gui_mapping2[key + '_ref']['filters'][0]['display_text'] = 'AD_ref'
				
				gui_mapping2[key + '_alt'] = gui_mapping2[key]
				gui_mapping2[key + '_alt']['filters'][0]['display_text'] = 'AD_alt'

				del gui_mapping2[key]
		elif format_dict[key]['type'] == 'keyword':
			gui_mapping2[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"		
			del gui_mapping2[key]['filters'][0]['in_line_tooltip']
			
			if key == 'group':
				gui_mapping2[key]['filters'][0]['widget_type'] = "Select"
				gui_mapping2[key]['filters'][0]['form_type'] = "ChoiceField"
			else:
				gui_mapping2[key]['filters'][0]['widget_type'] = "SelectMultiple"
				gui_mapping2[key]['filters'][0]['form_type'] = "MultipleChoiceField"

				
			

	outputfile = 'default_gui_config.json'
	result = {**gui_mapping, **gui_mapping2}
	with open(outputfile, 'w') as f:
		json.dump(result, f, sort_keys=True, indent=4, ensure_ascii=True)

if __name__ == '__main__':
	make_gui(vcf_info, mapping)
