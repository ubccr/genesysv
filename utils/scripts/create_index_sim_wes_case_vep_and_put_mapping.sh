curl -XPUT 'localhost:9200/sim_wes_case_vep?pretty' -H 'Content-Type: application/json' -d'
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
curl -XPUT 'localhost:9200/sim_wes_case_vep/_mapping/sim_wes_case_vep_?pretty' -H 'Content-Type: application/json' -d'
{
  "sim_wes_case_vep_": {
    "properties": {
      "AA_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "AC": {
        "null_value": -999,
        "type": "integer"
      },
      "AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "AFR_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "ALT": {
        "type": "keyword"
      },
      "AMR_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "AN": {
        "null_value": -999,
        "type": "integer"
      },
      "BaseQRankSum": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_PHRED": {
        "null_value": -999.99,
        "type": "float"
      },
      "CADD_RAW": {
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
      "CLIN_SIG": {
        "null_value": "NA",
        "type": "keyword"
      },
      "COSMIC_ID": {
        "type": "keyword"
      },
      "CSQ_nested": {
        "properties": {
          "Amino_acids": {
            "null_value": "NA",
            "type": "keyword"
          },
          "BIOTYPE": {
            "null_value": "NA",
            "type": "keyword"
          },
          "CDS_position": {
            "null_value": "NA",
            "type": "keyword"
          },
          "Codons": {
            "null_value": "NA",
            "type": "keyword"
          },
          "Consequence": {
            "null_value": "NA",
            "type": "keyword"
          },
          "DISTANCE": {
            "null_value": -999,
            "type": "integer"
          },
          "DOMAINS": {
            "null_value": "NA",
            "type": "keyword"
          },
          "EXON": {
            "null_value": "NA",
            "type": "keyword"
          },
          "Feature": {
            "null_value": "NA",
            "type": "keyword"
          },
          "Feature_type": {
            "null_value": "NA",
            "type": "keyword"
          },
          "Gene": {
            "null_value": "NA",
            "type": "keyword"
          },
          "HGNC_ID": {
            "null_value": "NA",
            "type": "keyword"
          },
          "HGVSc": {
            "null_value": "NA",
            "type": "keyword"
          },
          "HGVSp": {
            "null_value": "NA",
            "type": "keyword"
          },
          "IMPACT": {
            "null_value": "NA",
            "type": "keyword"
          },
          "INTRON": {
            "null_value": "NA",
            "type": "keyword"
          },
          "PolyPhen_pred": {
            "null_value": "NA",
            "type": "keyword"
          },
          "PolyPhen_score": {
            "null_value": -999.99,
            "type": "float"
          },
          "Protein_position": {
            "null_value": "NA",
            "type": "keyword"
          },
          "SIFT_pred": {
            "null_value": "NA",
            "type": "keyword"
          },
          "SIFT_score": {
            "null_value": -999.99,
            "type": "float"
          },
          "STRAND": {
            "null_value": "NA",
            "type": "keyword"
          },
          "SWISSPROT": {
            "null_value": "NA",
            "type": "keyword"
          },
          "SYMBOL": {
            "null_value": "NA",
            "type": "keyword"
          },
          "cDNA_position": {
            "null_value": "NA",
            "type": "keyword"
          },
          "miRNA": {
            "null_value": "NA",
            "type": "keyword"
          }
        },
        "type": "nested"
      },
      "ClippingRankSum": {
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
      "EAS_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "EA_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "END": {
        "null_value": -999,
        "type": "integer"
      },
      "EUR_AF": {
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
      "GQ_MEAN": {
        "null_value": -999.99,
        "type": "float"
      },
      "GQ_STDDEV": {
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
      "MAX_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "MAX_AF_POPS": {
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
      "NCC": {
        "null_value": -999,
        "type": "integer"
      },
      "POS": {
        "type": "integer"
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
      "ReadPosRankSum": {
        "null_value": -999.99,
        "type": "float"
      },
      "SAS_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "SOR": {
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
      "culprit": {
        "null_value": "NA",
        "type": "keyword"
      },
      "dbSNP_ID": {
        "type": "keyword"
      },
      "gnomAD_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_AFR_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_AMR_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_ASJ_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_EAS_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_FIN_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_NFE_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_OTH_AF": {
        "null_value": -999.99,
        "type": "float"
      },
      "gnomAD_SAS_AF": {
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
      }
    }
  }
}'