import json
import os
import re
import copy
from collections import OrderedDict


def make_gui_config(vcf_info_file, mapping_file, type_name, annot):
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
	variant_related_fields = ['Variant', 'CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'VariantType', 'cytoBand']

	variant_quality_related_fields = ['OND', 'HRun', 'ABHom', 'ABHet', 'ExcessHet', 'RAW_MQ', 'InbreedingCoeff', 'MQRankSum', 'MQ0', 'BaseQRankSum', 'HWP', 'FS', 'FS','ClippingRankSum', 'MQ', 'QD', 'ReadPosRankSum', 'HaplotypeScore', 'VQSLOD', 'SOR']
	
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
		disease_association_fields = ['PUBMED', 'PHENO', 'CLIN_SIG']
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
		
		disease_association_fields = ['Associated_disease', 'COSMIC_Occurrence', 'ICGC_Id', 'ICGC_Occurrence',  'gwasCatalog', 'Tumor_site', 'CLINSIG', 'CLNDBN', 'CLNDSDBID', 'CLNDSDB', 'CLNACC'] 

		conservation_fields = ['PhastCons_46V', 'PhyloP_100V', 'PhyloP_46V', 'PhastCons_100V', 'tfbsConsSites',
								'phyloP100way_vertebrate', 'phyloP100way_vertebrate_rankscore', 
								'phyloP20way_mammalian', 'phyloP20way_mammalian_rankscore', 
								'phastCons100way_vertebrate', 'phastCons100way_vertebrate_rankscore', 
								'phastCons20way_mammalian', 'phastCons20way_mammalian_rankscore', 
								'SiPhy_29way_logOdds', 'SiPhy_29way_logOdds_rankscore', 'wgRna', 'targetScanS']
		intervar_fields = ["BS1", "BS2", "BS3", "BS4", "BA1", "BP1", "BP2","BP3", "BP4", "BP5", "BP6", "BP7", "PVS1", "PS1", "PS2", "PS3", "PS4", "PM1", "PM2", "PM3", "PM4", "PM5", "PM6", "PP1", "PP2", "PP3", "PP4", "PP5"]

	sample_related_fields = ['Sample_ID', 'Phenotype', 'Sex', 'GT', 'PGT', 'PID', 'AD', 'AD_ref', 'AD_alt', 'MIN_DP', 'GQ', 'PGQ', 'PL', 'SB', 'Family_ID', 'Mother_ID', 'Father_ID', 'Mother_Genotype', 'Father_Genotype', 'Mother_Phenotype', 'Father_Phenotype']	
	boolean_fields = ['dbSNP_ID', 'COSMIC_ID', 'snp138NonFlagged']
	
	to_exclude = ['ID', 'sample', 'AAChange_refGene', 'AAChange_ensGene', 'CSQ_nested', 'Class_predicted', 'culprit','US', 'Presence_in_TD', 'Prob_N', 'Prob_P', 'Mutation_frequency', 'AA_pos', 'AA_sub', 'CCC', 'CSQ', 'END', 'DB', 'MQ0', 'ANNOVAR_DATE', 'NEGATIVE_TRAIN_SITE', 'POSITIVE_TRAIN_SITE', 'DS', 'ALLELE_END']
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
	for key in variant_related_fields:
		if key in keys_in_es_mapping:
			gui_mapping_var[key] = copy.deepcopy(default_gui_mapping)
			gui_mapping_var[key]['panel'] = "Variant Related Information"
			gui_mapping_var[key]['filters'][0]["display_text"] = key

			if key == 'CHROM':
				gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_var[key]['filters'][0]["values"] = "get_from_es()"
				gui_mapping_var[key]['filters'][0]["es_filter_type"] = "filter_terms"
			elif key == 'FILTER':
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
				gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
			elif key == 'cytoBand':
				gui_mapping_var[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_var[key]['filters'][0]["form_type"] = "MultipleChoiceField"
				gui_mapping_var[key]['filters'][0]["values"] = "get_from_es()"
			elif key == 'ID':
				gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(from original VCF)"
			seen[key] = ''
			
	for key in variant_quality_related_fields:
		if key in keys_in_es_mapping:
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
			if mapping[key]['type'] == 'integer' or mapping[key]['type'] == 'float':
				gui_mapping_qc[key] = copy.deepcopy(default_gui_mapping)	
				gui_mapping_qc[key]['filters'][0]['display_text'] = key
				gui_mapping_qc[key]['filters'].append(copy.deepcopy(gui_mapping_qc[key]['filters'][0]))
				gui_mapping_qc[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_qc[key]['filters'][0]['es_filter_type'] = "filter_range_gte"
				gui_mapping_qc[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_qc[key]['filters'][0]['in_line_tooltip'] = "(>=)"
				gui_mapping_qc[key]['filters'][0]['widget_type'] = "TextInput"
				gui_mapping_qc[key]['filters'][1]['es_filter_type'] = "filter_range_lte"
				gui_mapping_qc[key]['filters'][1]['in_line_tooltip'] = "(<=)"

				gui_mapping_qc[key]['panel']  = 'Variant Quality Metrix'
				seen[key] = ''	
	for key in minor_allele_freq_fields:
		if key in keys_in_es_mapping:
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
				
			gui_mapping_maf[key] = copy.deepcopy(default_gui_mapping)
			gui_mapping_maf[key]['filters'][0]['display_text'] = key
			if mapping[key]['type'] == 'float':
				gui_mapping_maf[key]['filters'][0]['form_type'] = "CharField"
				gui_mapping_maf[key]['filters'][0]['es_filter_type'] = "filter_range_lt"
				gui_mapping_maf[key]['filters'][0]['in_line_tooltip'] = "(<)"
				gui_mapping_maf[key]['filters'][0]['tooltip'] = tooltip
				gui_mapping_maf[key]['filters'][0]['widget_type'] = "TextInput"
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
			elif key == 'nci60':
				gui_mapping_maf[key]['sub_panel'] = 'NCI60'

			gui_mapping_maf[key]['panel']  = 'Minor Allele Frequency (MAF)'
			seen[key] = ''
	for key in summary_statistics_fields:
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
			
			if key == 'AD':
				del gui_mapping_sample[key]
			elif key in ['Sample_ID', 'GT', 'PGT', 'Family_ID', 'Father_ID', 'Mother_ID', 'Sex', 'Phenotype', 'Mother_Genotype', 'Father_Genotype', 'Mother_Phenotype', 'Father_Phenotype']:
				gui_mapping_sample[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"		
				gui_mapping_sample[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_sample[key]['filters'][0]["in_line_tooltip"] = ""
				gui_mapping_sample[key]['filters'][0]['widget_type'] = "SelectMultiple"
				gui_mapping_sample[key]['filters'][0]['form_type'] = "MultipleChoiceField"
			elif key == 'group':
				gui_mapping_sample[key]['filters'][0]['widget_type'] = "Select"
				gui_mapping_sample[key]['filters'][0]['form_type'] = "ChoiceField"
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

	for key in boolean_fields:
		if key in keys_in_es_mapping:
			if key in info_dict and 'Description' in info_dict[key]:
				tooltip = info_dict[key]['Description']
			else:
				tooltip = ""
			gui_mapping_var[key] = copy.deepcopy(default_gui_mapping)
			gui_mapping_var[key]['filters'].append(copy.deepcopy(gui_mapping_var[key]['filters'][0]))
			gui_mapping_var[key]['filters'][0]['display_text'] = key
			gui_mapping_var[key]['filters'][0]['in_line_tooltip'] = "(One ID per line)"
			gui_mapping_var[key]['filters'][0]['widget_type'] = "UploadField"
			gui_mapping_var[key]['filters'][0]['form_type'] = "CharField"
			gui_mapping_var[key]['filters'][0]['tooltip'] = tooltip
			gui_mapping_var[key]['filters'][0]['es_filter_type'] = "filter_terms"
			gui_mapping_var[key]['filters'][1]['display_text'] = 'Limit Variants to ' + key
			gui_mapping_var[key]['filters'][1]['widget_type'] = "Select"
			gui_mapping_var[key]['filters'][1]['es_filter_type'] = "filter_exists"
			gui_mapping_var[key]['filters'][1]['form_type'] = "ChoiceField"
			
			if key.startswith("dbSNP"):
				gui_mapping_var[key]['panel'] = "Variant Related Information"
			elif key.startswith("COSMIC") or key in ["snp138NonFlagged", "CLINSIG", "CLIN_SIG"]:
				gui_mapping_var[key]['panel'] = "Disease Associations"
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
				gui_mapping_func[key]['filters'][0]['values'] = "get_from_es()"
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
					gui_mapping_gene[key]['filters'][0]['values'] = "get_from_es()"
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
			if key in keys_in_es_mapping:
				gui_mapping_patho_p[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_patho_p[key]['filters'][0]['display_text'] = key
				gui_mapping_patho_p[key]['filters'][0]['es_filter_type'] = "nested_filter_terms"
				gui_mapping_patho_p[key]['filters'][0]['form_type'] = "ChoiceField"
				gui_mapping_patho_p[key]['filters'][0]["widget_type"] = "Select"
				gui_mapping_patho_p[key]['filters'][0]['path'] = 'CSQ_nested'
				gui_mapping_patho_p[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_patho_p[key]['panel']  = 'Pathogenicity Predictions'
				gui_mapping_patho_p[key]['sub_panel']  = 'Predictions'
				seen[key] = ''	
		for key in pathogenicity_score_fields:
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
				gui_mapping_disease[key]['filters'][0]['display_text'] = key
				gui_mapping_disease[key]['filters'][0]['form_type'] = "MultipleChoiceField"
				gui_mapping_disease[key]['filters'][0]["widget_type"] = "SelectMultiple"
				gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
				gui_mapping_disease[key]['filters'][0]['values'] = "get_from_es()"
				gui_mapping_disease[key]['panel'] = 'Disease Associations'
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
				gui_mapping_func[key]['filters'][0]['values'] = "get_from_es()"
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
				gui_mapping_patho_p[key]['filters'][0]['values'] = "get_from_es()"
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
				else:
					gui_mapping_disease[key]['filters'][0]['es_filter_type'] = "filter_terms"
					gui_mapping_disease[key]['filters'][0]['form_type'] = "ChoiceField"
					gui_mapping_disease[key]['filters'][0]["widget_type"] = "Select"
					gui_mapping_disease[key]['filters'][0]['values'] = "get_from_es()"
					
				gui_mapping_disease[key]['panel'] = 'Disease Associations'
				seen[key] = ''
		for key in intervar_fields:
			if key in keys_in_es_mapping:
				if key in info_dict and 'Description' in info_dict[key]:
					tooltip = info_dict[key]['Description']
				else:
					tooltip = ""
				gui_mapping_intvar[key] = copy.deepcopy(default_gui_mapping)
				gui_mapping_intvar[key]['filters'][0]['values'] = "get_from_es()"
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
	
	outputfile = os.path.join("config", type_name + '_gui_config.json')
	with open(outputfile, 'w') as f:
		json.dump(result, f, sort_keys=False, indent=4, ensure_ascii=True)

	return(outputfile)



if __name__ == '__main__':
	vcf_info_file = 'mendelian_test_six_families_vcf_info.json' #sim_control_three_samples_random_100000_vcf_info.json' #sim_control.hg19_multianno_vcf_info.json'
	mapping_file = 'scripts/mendelian_six_mapping.json' #sim_control_3s_mapping.json'
	type_name = 'mendelian_six_' #sim_control_3s_' #sim_control_3s_'
	annot = 'vep' #annovar'
	make_gui(vcf_info_file, mapping_file, type_name, annot)
