curl -XPUT 'localhost:9200/simwescase_annovar?pretty' -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index.merge.policy.floor_segment": "100mb",
    "index.merge.policy.max_merge_at_once": 7,
    "index.merge.policy.max_merged_segment": "10gb",
    "index.merge.policy.segments_per_tier": 25,
    "index.merge.scheduler.max_merge_count": 7,
    "index.merge.scheduler.max_thread_count": 7,
    "number_of_replicas": 1,
    "number_of_shards": 7,
    "refresh_interval": "1s"
  }
}'
curl -XPUT 'localhost:9200/simwescase_annovar/_mapping/simwescase_annovar_?pretty' -H 'Content-Type: application/json' -d'
{
  "simwescase_annovar_": {
    "properties": {
      "1000g2015aug_afr": {
        "null_value": -999.99,
        "type": "float"
      },
      "1000g2015aug_all": {
        "null_value": -999.99,
        "type": "float"
      },
      "1000g2015aug_amr": {
        "null_value": -999.99,
        "type": "float"
      },
      "1000g2015aug_eur": {
        "null_value": -999.99,
        "type": "float"
      },
      "1000g2015aug_sas": {
        "null_value": -999.99,
        "type": "float"
      },
      "AAChange_ensGene": {
        "properties": {
          "EnsembleTranscriptID": {
            "null_value": "NA",
            "type": "keyword"
          },
          "aa_change_eg": {
            "null_value": "NA",
            "type": "keyword"
          },
          "cdna_change_eg": {
            "null_value": "NA",
            "type": "keyword"
          },
          "exon_id_eg": {
            "null_value": "NA",
            "type": "keyword"
          }
        },
        "type": "nested"
      },
      "AAChange_refGene": {
        "properties": {
          "RefSeq": {
            "null_value": "NA",
            "type": "keyword"
          },
          "aa_change_rg": {
            "null_value": "NA",
            "type": "keyword"
          },
          "cdna_change_rg": {
            "null_value": "NA",
            "type": "keyword"
          },
          "exon_id_rg": {
            "null_value": "NA",
            "type": "keyword"
          }
        },
        "type": "nested"
      },
      "AA_pos": {
        "null_value": "NA",
        "type": "keyword"
      },
      "AA_sub": {
        "null_value": "NA",
        "type": "keyword"
      },
      "AC": {
        "null_value": -999,
        "type": "integer"
      },
      "AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "ALLELE_END": {
        "type": "text"
      },
      "ALT": {
        "type": "keyword"
      },
      "AN": {
        "null_value": -999,
        "type": "integer"
      },
      "Associated_disease": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BA1": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BP1": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BP2": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BP3": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BP4": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BP5": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BP6": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BP7": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BS1": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BS2": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BS3": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BS4": {
        "null_value": "NA",
        "type": "keyword"
      },
      "BaseQRankSum": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD13_PHRED": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD13_RawScore": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_Phred": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_Phred_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_phred": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_prediction": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_raw": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_raw_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "CCC": {
        "null_value": -999,
        "type": "integer"
      },
      "CHROM": {
        "type": "keyword"
      },
      "CLINSIG": {
        "null_value": "NA",
        "type": "keyword"
      },
      "CLNACC": {
        "null_value": "NA",
        "type": "keyword"
      },
      "CLNDBN": {
        "null_value": "NA",
        "type": "keyword"
      },
      "CLNDSDB": {
        "null_value": "NA",
        "type": "keyword"
      },
      "CLNDSDBID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "COSMIC_ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "COVEC_WMV": {
        "null_value": "NA",
        "type": "keyword"
      },
      "COVEC_WMV_prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Carol_prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Carol_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "Class_predicted": {
        "null_value": "NA",
        "type": "keyword"
      },
      "ClippingRankSum": {
        "null_value": -999.99,
        "type": "float"
      },
      "Codon_sub": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Condel_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Condel_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "DANN_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "DANN_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "DP": {
        "null_value": -999,
        "type": "integer"
      },
      "DS": {
        "type": "text"
      },
      "EFIN_HumDiv_Prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "EFIN_HumDiv_Score": {
        "null_value": "NA",
        "type": "keyword"
      },
      "EFIN_Swiss_Prot_Prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "EFIN_Swiss_Prot_Score": {
        "null_value": "NA",
        "type": "keyword"
      },
      "END": {
        "null_value": -999,
        "type": "integer"
      },
      "Eigen": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Eigen-PC-raw": {
        "null_value": -999.99,
        "type": "float"
      },
      "Eigen-raw": {
        "null_value": -999.99,
        "type": "float"
      },
      "Eigen_coding_or_noncoding": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Ensembl_Gene_ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Ensembl_Protein_ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "ExAC_AFR": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_ALL": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_AMR": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_EAS": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_FIN": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_NFE": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_OTH": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_SAS": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_AFR": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_ALL": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_AMR": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_EAS": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_FIN": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_NFE": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_OTH": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nonpsych_SAS": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_AFR": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_ALL": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_AMR": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_EAS": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_FIN": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_NFE": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_OTH": {
        "null_value": -999.99,
        "type": "float"
      },
      "ExAC_nontcga_SAS": {
        "null_value": -999.99,
        "type": "float"
      },
      "Examined_samples": {
        "null_value": "NA",
        "type": "keyword"
      },
      "ExonicFunc_ensGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "ExonicFunc_refGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "FATHMM_converted_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "FATHMM_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "FATHMM_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "FILTER": {
        "type": "keyword"
      },
      "FS": {
        "null_value": -999.99,
        "type": "float"
      },
      "FatHmm_prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "FatHmm_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "Func_ensGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Func_refGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "GERPplusplus_RS": {
        "null_value": -999.99,
        "type": "float"
      },
      "GERPplusplus_RS_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "GQ_MEAN": {
        "null_value": -999.99,
        "type": "float"
      },
      "GQ_STDDEV": {
        "null_value": -999.99,
        "type": "float"
      },
      "GTEx_V6_gene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "GTEx_V6_tissue": {
        "null_value": "NA",
        "type": "keyword"
      },
      "GeneDetail_ensGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "GeneDetail_refGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Gene_ensGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Gene_pos": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Gene_refGene": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Gene_symbol": {
        "null_value": "NA",
        "type": "keyword"
      },
      "GenoCanyon_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "GenoCanyon_score_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "HWP": {
        "null_value": -999.99,
        "type": "float"
      },
      "HaplotypeScore": {
        "null_value": -999.99,
        "type": "float"
      },
      "ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "InbreedingCoeff": {
        "null_value": -999.99,
        "type": "float"
      },
      "InterVar(automated)": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Interpro_domain": {
        "null_value": "NA",
        "type": "keyword"
      },
      "LRT_converted_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "LRT_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "LRT_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "M-CAP_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "M-CAP_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "M-CAP_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "MCAP": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MLEAC": {
        "null_value": -999,
        "type": "integer"
      },
      "MLEAF": {
        "null_value": -999.99,
        "type": "float"
      },
      "MQ": {
        "null_value": -999.99,
        "type": "float"
      },
      "MQRankSum": {
        "null_value": -999.99,
        "type": "float"
      },
      "Mean_MI_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "MetaLR_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MetaLR_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "MetaLR_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "MetaSVM_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MetaSVM_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "MetaSVM_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "MutAss_pred_transf": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MutAss_prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MutAss_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "MutAss_score_transf": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MutationAssessor_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MutationAssessor_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "MutationAssessor_score_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "MutationTaster_converted_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "MutationTaster_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "MutationTaster_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "Mutation_frequency": {
        "null_value": -999.99,
        "type": "float"
      },
      "NCBI_Gene_ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "NCBI_Protein_ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "NCC": {
        "null_value": -999,
        "type": "integer"
      },
      "OXPHOS_Complex": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PM1": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PM2": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PM3": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PM4": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PM5": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PM6": {
        "null_value": "NA",
        "type": "keyword"
      },
      "POS": {
        "type": "integer"
      },
      "PP1": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PP2": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PP3": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PP4": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PP5": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PROVEAN_converted_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "PROVEAN_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PROVEAN_prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PROVEAN_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "PS1": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PS2": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PS3": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PS4": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PVS1": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Perc_coevo_Sites": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PhastCons_100V": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PhastCons_46V": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PhyloP_100V": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PhyloP_46V": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PolyPhen2_pred_transf": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PolyPhen2_prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "PolyPhen2_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "PolyPhen2_score_transf": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Polyphen2_HDIV_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Polyphen2_HDIV_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "Polyphen2_HDIV_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "Polyphen2_HVAR_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Polyphen2_HVAR_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "Polyphen2_HVAR_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "Presence_in_TD": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Prob_N": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Prob_P": {
        "null_value": "NA",
        "type": "keyword"
      },
      "QD": {
        "null_value": -999.99,
        "type": "float"
      },
      "QUAL": {
        "type": "float"
      },
      "REF": {
        "type": "keyword"
      },
      "REVEL": {
        "null_value": "NA",
        "type": "keyword"
      },
      "ReadPosRankSum": {
        "null_value": -999.99,
        "type": "float"
      },
      "SIFT_converted_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "SIFT_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "SIFT_pred_transf": {
        "null_value": "NA",
        "type": "keyword"
      },
      "SIFT_prediction": {
        "null_value": "NA",
        "type": "keyword"
      },
      "SIFT_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "SIFT_score_transf": {
        "null_value": "NA",
        "type": "keyword"
      },
      "SOR": {
        "null_value": -999.99,
        "type": "float"
      },
      "SiPhy_29way_logOdds": {
        "null_value": "NA",
        "type": "keyword"
      },
      "SiPhy_29way_logOdds_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "SiteVar": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Status": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Tumor_site": {
        "null_value": "NA",
        "type": "keyword"
      },
      "US": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Uniprot_ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "Uniprot_Name": {
        "null_value": "NA",
        "type": "keyword"
      },
      "VEST3_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "VEST3_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "VQSLOD": {
        "null_value": -999.99,
        "type": "float"
      },
      "Variant": {
        "type": "keyword"
      },
      "VariantType": {
        "type": "keyword"
      },
      "avsnp147": {
        "type": "keyword"
      },
      "culprit": {
        "null_value": "NA",
        "type": "keyword"
      },
      "cytoBand": {
        "null_value": "NA",
        "type": "keyword"
      },
      "dbSNP_ID": {
        "null_value": "NA",
        "type": "keyword"
      },
      "dbscSNV_ADA_SCORE": {
        "null_value": -999.99,
        "type": "float"
      },
      "dbscSNV_RF_SCORE": {
        "null_value": -999.99,
        "type": "float"
      },
      "esp6500siv2_aa": {
        "null_value": -999.99,
        "type": "float"
      },
      "esp6500siv2_all": {
        "null_value": -999.99,
        "type": "float"
      },
      "esp6500siv2_ea": {
        "null_value": -999.99,
        "type": "float"
      },
      "fathmm-MKL_coding_pred": {
        "null_value": "NA",
        "type": "keyword"
      },
      "fathmm-MKL_coding_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "fathmm-MKL_coding_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "gwasCatalog": {
        "null_value": "NA",
        "type": "keyword"
      },
      "integrated_confidence_value": {
        "null_value": "NA",
        "type": "keyword"
      },
      "integrated_fitCons_score": {
        "null_value": -999.99,
        "type": "float"
      },
      "integrated_fitCons_score_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "phastCons100way_vertebrate": {
        "null_value": "NA",
        "type": "keyword"
      },
      "phastCons100way_vertebrate_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "phastCons20way_mammalian": {
        "null_value": "NA",
        "type": "keyword"
      },
      "phastCons20way_mammalian_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "phyloP100way_vertebrate": {
        "null_value": "NA",
        "type": "keyword"
      },
      "phyloP100way_vertebrate_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "phyloP20way_mammalian": {
        "null_value": "NA",
        "type": "keyword"
      },
      "phyloP20way_mammalian_rankscore": {
        "null_value": -999.99,
        "type": "float"
      },
      "sample": {
        "properties": {
          "AD_alt": {
            "null_value": -999,
            "type": "integer"
          },
          "AD_ref": {
            "null_value": -999,
            "type": "integer"
          },
          "DP": {
            "null_value": -999,
            "type": "integer"
          },
          "GQ": {
            "null_value": -999,
            "type": "integer"
          },
          "GT": {
            "null_value": "NA",
            "type": "keyword"
          },
          "MIN_DP": {
            "null_value": -999,
            "type": "integer"
          },
          "PGT": {
            "null_value": "NA",
            "type": "keyword"
          },
          "PID": {
            "null_value": "NA",
            "type": "keyword"
          },
          "PL": {
            "null_value": "NA",
            "type": "keyword"
          },
          "SB": {
            "null_value": -999,
            "type": "integer"
          },
          "Sample_ID": {
            "type": "keyword"
          }
        },
        "type": "nested"
      },
      "snp138NonFlagged": {
        "type": "keyword"
      },
      "targetScanS": {
        "null_value": "NA",
        "type": "keyword"
      },
      "tfbsConsSites": {
        "null_value": "NA",
        "type": "keyword"
      },
      "wgRna": {
        "null_value": "NA",
        "type": "keyword"
      }
    }
  }
}'