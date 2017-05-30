curl -XPUT '199.109.192.65:9200/empire?pretty' -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_replicas": 0,
    "number_of_shards": 5,
    "refresh_interval": "-1"
  }
}'
curl -XPUT '199.109.192.65:9200/empire/_mapping/empire?pretty' -H 'Content-Type: application/json' -d'
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
    "AF": {
      "type": "float"
    },
    "ALT": {
      "type": "keyword"
    },
    "ANNOVAR_DATE": {
      "type": "keyword"
    },
    "AO": {
      "type": "integer"
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
      "type": "text"
    },
    "CLNACC": {
      "type": "keyword"
    },
    "CLNDBN": {
      "type": "text"
    },
    "CLNDSDB": {
      "type": "text"
    },
    "CLNDSDBID": {
      "type": "text"
    },
    "DANN_rankscore": {
      "type": "float"
    },
    "DANN_score": {
      "type": "float"
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
    "FAO": {
      "type": "integer"
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
    "FDP": {
      "type": "integer"
    },
    "FILTER_status": {
      "type": "keyword"
    },
    "FR": {
      "type": "keyword"
    },
    "FRO": {
      "type": "integer"
    },
    "FSAF": {
      "type": "integer"
    },
    "FSAR": {
      "type": "integer"
    },
    "FSRF": {
      "type": "integer"
    },
    "FSRR": {
      "type": "integer"
    },
    "FWDB": {
      "type": "float"
    },
    "FXX": {
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
    "GenoCanyon_score": {
      "type": "float"
    },
    "GenoCanyon_score_rankscore": {
      "type": "float"
    },
    "HRUN": {
      "type": "integer"
    },
    "ID": {
      "type": "keyword"
    },
    "InterVar(automated)": {
      "type": "keyword"
    },
    "Interpro_domain": {
      "analyzer": "simple",
      "type": "text"
    },
    "LEN": {
      "type": "integer"
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
    "MLLD": {
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
    "OALT": {
      "type": "keyword"
    },
    "OID": {
      "type": "keyword"
    },
    "OMAPALT": {
      "type": "keyword"
    },
    "OPOS": {
      "type": "keyword"
    },
    "OREF": {
      "type": "keyword"
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
    "QUAL_score": {
      "type": "float"
    },
    "RBI": {
      "type": "float"
    },
    "REF": {
      "type": "keyword"
    },
    "REFB": {
      "type": "float"
    },
    "REVB": {
      "type": "float"
    },
    "REVEL": {
      "type": "float"
    },
    "RO": {
      "type": "integer"
    },
    "SAF": {
      "type": "integer"
    },
    "SAR": {
      "type": "integer"
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
    "SRF": {
      "type": "integer"
    },
    "SRR": {
      "type": "integer"
    },
    "SSEN": {
      "type": "float"
    },
    "SSEP": {
      "type": "float"
    },
    "SSSB": {
      "type": "float"
    },
    "STB": {
      "type": "float"
    },
    "STBP": {
      "type": "float"
    },
    "SiPhy_29way_logOdds": {
      "type": "float"
    },
    "SiPhy_29way_logOdds_rankscore": {
      "type": "float"
    },
    "TYPE": {
      "type": "keyword"
    },
    "VARB": {
      "type": "float"
    },
    "VEST3_rankscore": {
      "type": "float"
    },
    "VEST3_score": {
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
    "cosmic70": {
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
        "sample_AF": {
          "type": "float"
        },
        "sample_AO": {
          "type": "integer"
        },
        "sample_DP": {
          "type": "integer"
        },
        "sample_FAO": {
          "type": "integer"
        },
        "sample_FDP": {
          "type": "integer"
        },
        "sample_FRO": {
          "type": "integer"
        },
        "sample_FSAF": {
          "type": "integer"
        },
        "sample_FSAR": {
          "type": "integer"
        },
        "sample_FSRF": {
          "type": "integer"
        },
        "sample_FSRR": {
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
        "sample_RO": {
          "type": "integer"
        },
        "sample_SAF": {
          "type": "integer"
        },
        "sample_SAR": {
          "type": "integer"
        },
        "sample_SRF": {
          "type": "integer"
        },
        "sample_SRR": {
          "type": "integer"
        }
      },
      "type": "nested"
    },
    "snp138NonFlagged": {
      "type": "keyword"
    },
    "tfbsConsSites": {
      "type": "keyword"
    }
  }
}'