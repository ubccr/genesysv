curl -XPUT '199.109.192.80:9200/sim_wes?pretty' -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_replicas": 0,
    "number_of_shards": 5,
    "refresh_interval": "-1"
  }
}'
curl -XPUT '199.109.192.80:9200/sim_wes/_mapping/sim_wes?pretty' -H 'Content-Type: application/json' -d'
{
  "properties": {
    "1000g2015aug_afr": {
      "type": "float"
    },
    "1000g2015aug_all": {
      "type": "float"
    },
    "1000g2015aug_amr": {
      "type": "float"
    },
    "1000g2015aug_eur": {
      "type": "float"
    },
    "1000g2015aug_sas": {
      "type": "float"
    },
    "AC": {
      "type": "integer"
    },
    "AF": {
      "type": "float"
    },
    "ALT": {
      "type": "keyword"
    },
    "AN": {
      "type": "integer"
    },
    "ANNOVAR_DATE": {
      "type": "keyword"
    },
    "BA1": {
      "type": "integer"
    },
    "BP1": {
      "type": "integer"
    },
    "BP2": {
      "type": "integer"
    },
    "BP3": {
      "type": "integer"
    },
    "BP4": {
      "type": "integer"
    },
    "BP5": {
      "type": "integer"
    },
    "BP6": {
      "type": "integer"
    },
    "BP7": {
      "type": "integer"
    },
    "BS1": {
      "type": "integer"
    },
    "BS2": {
      "type": "integer"
    },
    "BS3": {
      "type": "integer"
    },
    "BS4": {
      "type": "integer"
    },
    "BaseQRankSum": {
      "type": "float"
    },
    "CADD": {
      "type": "keyword"
    },
    "CADD13_PHRED": {
      "type": "keyword"
    },
    "CADD13_RawScore": {
      "type": "float"
    },
    "CADD_Phred": {
      "type": "float"
    },
    "CADD_phred": {
      "type": "float"
    },
    "CADD_raw": {
      "type": "float"
    },
    "CADD_raw_rankscore": {
      "type": "float"
    },
    "CHROM": {
      "type": "keyword"
    },
    "CLINSIG": {
      "type": "keyword"
    },
    "CLNACC": {
      "type": "keyword"
    },
    "CLNDBN": {
      "analyzer": "simple",
      "type": "text"
    },
    "CLNDSDB": {
      "analyzer": "simple",
      "type": "text"
    },
    "CLNDSDBID": {
      "analyzer": "simple",
      "type": "text"
    },
    "ClippingRankSum": {
      "type": "float"
    },
    "DANN_rankscore": {
      "type": "float"
    },
    "DANN_score": {
      "type": "float"
    },
    "DB": {
      "type": "boolean"
    },
    "DP": {
      "type": "integer"
    },
    "Eigen": {
      "type": "float"
    },
    "Eigen-PC-raw": {
      "type": "float"
    },
    "Eigen-raw": {
      "type": "float"
    },
    "Eigen_coding_or_noncoding": {
      "type": "keyword"
    },
    "ExAC_AFR": {
      "type": "float"
    },
    "ExAC_ALL": {
      "type": "float"
    },
    "ExAC_AMR": {
      "type": "float"
    },
    "ExAC_EAS": {
      "type": "float"
    },
    "ExAC_FIN": {
      "type": "float"
    },
    "ExAC_NFE": {
      "type": "float"
    },
    "ExAC_OTH": {
      "type": "float"
    },
    "ExAC_SAS": {
      "type": "float"
    },
    "ExAC_nonpsych_AFR": {
      "type": "float"
    },
    "ExAC_nonpsych_ALL": {
      "type": "float"
    },
    "ExAC_nonpsych_AMR": {
      "type": "float"
    },
    "ExAC_nonpsych_EAS": {
      "type": "float"
    },
    "ExAC_nonpsych_FIN": {
      "type": "float"
    },
    "ExAC_nonpsych_NFE": {
      "type": "float"
    },
    "ExAC_nonpsych_OTH": {
      "type": "float"
    },
    "ExAC_nonpsych_SAS": {
      "type": "float"
    },
    "ExAC_nontcga_AFR": {
      "type": "float"
    },
    "ExAC_nontcga_ALL": {
      "type": "float"
    },
    "ExAC_nontcga_AMR": {
      "type": "float"
    },
    "ExAC_nontcga_EAS": {
      "type": "float"
    },
    "ExAC_nontcga_FIN": {
      "type": "float"
    },
    "ExAC_nontcga_NFE": {
      "type": "float"
    },
    "ExAC_nontcga_OTH": {
      "type": "float"
    },
    "ExAC_nontcga_SAS": {
      "type": "float"
    },
    "ExonicFunc_ensGene": {
      "type": "keyword"
    },
    "ExonicFunc_refGene": {
      "type": "keyword"
    },
    "FATHMM_converted_rankscore": {
      "type": "float"
    },
    "FATHMM_pred": {
      "type": "keyword"
    },
    "FATHMM_score": {
      "type": "float"
    },
    "FILTER": {
      "properties": {
        "FILTER_cohort": {
          "type": "keyword"
        },
        "FILTER_status": {
          "type": "keyword"
        }
      },
      "type": "nested"
    },
    "FS": {
      "type": "float"
    },
    "Func_ensGene": {
      "type": "keyword"
    },
    "Func_refGene": {
      "type": "keyword"
    },
    "GERPplusplus_RS": {
      "type": "float"
    },
    "GERPplusplus_RS_rankscore": {
      "type": "float"
    },
    "GQ_MEAN": {
      "type": "float"
    },
    "GQ_STDDEV": {
      "type": "float"
    },
    "GTEx_V6_gene": {
      "analyzer": "whitespace",
      "type": "text"
    },
    "GTEx_V6_tissue": {
      "type": "text"
    },
    "GenoCanyon_score": {
      "type": "float"
    },
    "GenoCanyon_score_rankscore": {
      "type": "float"
    },
    "ID": {
      "type": "keyword"
    },
    "InbreedingCoeff": {
      "type": "float"
    },
    "InterVar(automated)": {
      "type": "keyword"
    },
    "Interpro_domain": {
      "analyzer": "simple",
      "type": "text"
    },
    "LRT_converted_rankscore": {
      "type": "float"
    },
    "LRT_pred": {
      "type": "keyword"
    },
    "LRT_score": {
      "type": "float"
    },
    "M-CAP_pred": {
      "type": "keyword"
    },
    "M-CAP_rankscore": {
      "type": "float"
    },
    "M-CAP_score": {
      "type": "float"
    },
    "MCAP": {
      "type": "float"
    },
    "MLEAC": {
      "type": "integer"
    },
    "MLEAF": {
      "type": "float"
    },
    "MQ": {
      "type": "float"
    },
    "MQ0": {
      "type": "float"
    },
    "MQRankSum": {
      "type": "float"
    },
    "MetaLR_pred": {
      "type": "keyword"
    },
    "MetaLR_rankscore": {
      "type": "float"
    },
    "MetaLR_score": {
      "type": "float"
    },
    "MetaSVM_pred": {
      "type": "keyword"
    },
    "MetaSVM_rankscore": {
      "type": "float"
    },
    "MetaSVM_score": {
      "type": "float"
    },
    "MutationAssessor_pred": {
      "type": "keyword"
    },
    "MutationAssessor_score": {
      "type": "float"
    },
    "MutationAssessor_score_rankscore": {
      "type": "float"
    },
    "MutationTaster_converted_rankscore": {
      "type": "float"
    },
    "MutationTaster_pred": {
      "type": "keyword"
    },
    "MutationTaster_score": {
      "type": "float"
    },
    "NCC": {
      "type": "integer"
    },
    "NEGATIVE_TRAIN_SITE": {
      "type": "boolean"
    },
    "PM1": {
      "type": "integer"
    },
    "PM2": {
      "type": "integer"
    },
    "PM3": {
      "type": "integer"
    },
    "PM4": {
      "type": "integer"
    },
    "PM5": {
      "type": "integer"
    },
    "PM6": {
      "type": "integer"
    },
    "POS": {
      "type": "integer"
    },
    "POSITIVE_TRAIN_SITE": {
      "type": "boolean"
    },
    "PP1": {
      "type": "integer"
    },
    "PP2": {
      "type": "integer"
    },
    "PP3": {
      "type": "integer"
    },
    "PP4": {
      "type": "integer"
    },
    "PP5": {
      "type": "integer"
    },
    "PROVEAN_converted_rankscore": {
      "type": "float"
    },
    "PROVEAN_pred": {
      "type": "keyword"
    },
    "PROVEAN_score": {
      "type": "float"
    },
    "PS1": {
      "type": "integer"
    },
    "PS2": {
      "type": "integer"
    },
    "PS3": {
      "type": "integer"
    },
    "PS4": {
      "type": "integer"
    },
    "PVS1": {
      "type": "integer"
    },
    "Polyphen2_HDIV_pred": {
      "type": "keyword"
    },
    "Polyphen2_HDIV_rankscore": {
      "type": "float"
    },
    "Polyphen2_HDIV_score": {
      "type": "float"
    },
    "Polyphen2_HVAR_pred": {
      "type": "keyword"
    },
    "Polyphen2_HVAR_rankscore": {
      "type": "float"
    },
    "Polyphen2_HVAR_score": {
      "type": "float"
    },
    "QD": {
      "type": "float"
    },
    "QUAL": {
      "properties": {
        "QUAL_cohort": {
          "type": "keyword"
        },
        "QUAL_score": {
          "type": "float"
        }
      },
      "type": "nested"
    },
    "REF": {
      "type": "keyword"
    },
    "REVEL": {
      "type": "float"
    },
    "ReadPosRankSum": {
      "type": "float"
    },
    "SIFT_converted_rankscore": {
      "type": "float"
    },
    "SIFT_pred": {
      "type": "keyword"
    },
    "SIFT_score": {
      "type": "float"
    },
    "SOR": {
      "type": "float"
    },
    "SiPhy_29way_logOdds": {
      "type": "float"
    },
    "SiPhy_29way_logOdds_rankscore": {
      "type": "float"
    },
    "VEST3_rankscore": {
      "type": "float"
    },
    "VEST3_score": {
      "type": "float"
    },
    "VQSLOD": {
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
      "type": "keyword"
    },
    "cytoBand": {
      "type": "keyword"
    },
    "dbscSNV_ADA_SCORE": {
      "type": "keyword"
    },
    "dbscSNV_RF_SCORE": {
      "type": "keyword"
    },
    "ensGene": {
      "properties": {
        "ensGene_aa_change": {
          "type": "keyword"
        },
        "ensGene_cDNA_change": {
          "type": "keyword"
        },
        "ensGene_distance_to_gene": {
          "type": "keyword"
        },
        "ensGene_gene_id": {
          "analyzer": "whitespace",
          "type": "text"
        },
        "ensGene_location": {
          "type": "keyword"
        },
        "ensGene_transcript_id": {
          "type": "keyword"
        }
      },
      "type": "nested"
    },
    "esp6500siv2_aa": {
      "type": "float"
    },
    "esp6500siv2_all": {
      "type": "float"
    },
    "esp6500siv2_ea": {
      "type": "float"
    },
    "fathmm-MKL_coding_pred": {
      "type": "keyword"
    },
    "fathmm-MKL_coding_rankscore": {
      "type": "float"
    },
    "fathmm-MKL_coding_score": {
      "type": "float"
    },
    "gwasCatalog": {
      "analyzer": "simple",
      "type": "text"
    },
    "integrated_confidence_value": {
      "type": "integer"
    },
    "integrated_fitCons_score": {
      "type": "float"
    },
    "integrated_fitCons_score_rankscore": {
      "type": "float"
    },
    "phastCons100way_vertebrate": {
      "type": "float"
    },
    "phastCons100way_vertebrate_rankscore": {
      "type": "float"
    },
    "phastCons20way_mammalian": {
      "type": "float"
    },
    "phastCons20way_mammalian_rankscore": {
      "type": "float"
    },
    "phyloP100way_vertebrate": {
      "type": "float"
    },
    "phyloP100way_vertebrate_rankscore": {
      "type": "float"
    },
    "phyloP20way_mammalian": {
      "type": "float"
    },
    "phyloP20way_mammalian_rankscore": {
      "type": "float"
    },
    "refGene": {
      "properties": {
        "refGene_aa_change": {
          "type": "keyword"
        },
        "refGene_cDNA_change": {
          "type": "keyword"
        },
        "refGene_distance_to_gene": {
          "type": "keyword"
        },
        "refGene_location": {
          "type": "keyword"
        },
        "refGene_refgene_id": {
          "type": "keyword"
        },
        "refGene_symbol": {
          "analyzer": "whitespace",
          "type": "text"
        }
      },
      "type": "nested"
    },
    "sample": {
      "properties": {
        "sample_AD": {
          "type": "integer"
        },
        "sample_DP": {
          "type": "integer"
        },
        "sample_GQ": {
          "type": "integer"
        },
        "sample_GT": {
          "type": "keyword"
        },
        "sample_ID": {
          "type": "keyword"
        },
        "sample_MIN_DP": {
          "type": "integer"
        },
        "sample_PGT": {
          "type": "keyword"
        },
        "sample_PID": {
          "type": "keyword"
        },
        "sample_PL": {
          "type": "integer"
        },
        "sample_SB": {
          "type": "integer"
        }
      },
      "type": "nested"
    },
    "snp138NonFlagged": {
      "type": "keyword"
    },
    "targetScanS": {
      "type": "keyword"
    },
    "tfbsConsSites": {
      "type": "keyword"
    },
    "wgRna": {
      "type": "keyword"
    }
  }
}'