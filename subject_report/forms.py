from django import forms
from django.contrib.auth.models import User
from search.models import Study, Dataset, FilterFieldChoice
from django.db.models import Q
from crispy_forms.helper import FormHelper
from pprint import pprint
from collections import defaultdict
from urllib.parse import parse_qs
import elasticsearch
import json
from common.utils import filter_array_dicts, must_not_array_dicts
from search.models import SampleReadDepth


result_summary_template = """Whole [----] sequencing of this individual's genome was performed and resulted
    in %s/%s/%s percent coverage at 15/20/40(X).
    A total of %s variants were identified when compared to a reference genome.
    These variant data were analyzed to identify previously reported variants of potential clinical
    relevance associated with indicated for testing: %s.
"""


result_summary_template = " ".join((ele.strip() for ele in result_summary_template.split('\n')))

methodology_default_value = """[Blood,Saliva,?] sample from
    this individual was used to prepare a library for sequencing.
    Whole [---] sequencing was performed using Illumina HiSeq 2500
    sequencer at University at Buffalo's Genomics and Bioinformatics
    Core (check for proper name). Raw sequences were aligned with BWA MEM (v0.7.12)
    and variants were called using a GATK (v3.5) variant calling pipeline (v3.5.2).
    Variants were annotated using annovar (March 2016 release). Clinical relevance
    were based on ClinVar and GWASCatalog annotations.

"""
methodology_default_value = " ".join((ele.strip() for ele in methodology_default_value.split('\n')))


acceptable_clin_vars = ['probable-pathogenic', 'pathogenic', 'drug-response', 'histocompatibility']

def get_relevant_clinvar(subject, database_name, indication_for_testing):
    dataset_obj = Dataset.objects.get(name=database_name)

    es = elasticsearch.Elasticsearch(host=dataset_obj.es_host)

    es_query_string_template = """
    {
    "query": {
        "bool": {
            "minimum_should_match": 1,
            "must": [
                    {"term": {"CLNDBN": "%s"}},
                    {"terms": {"CLINSIG": ["probable-pathogenic", "pathogenic", "drug-response", "histocompatibility"]},
                    {"nested": {
                        "path": "sample",
                        "query": {
                            "bool": {
                                "filter": [{"terms": {"sample.sample_ID": ["%s"]}}]
                                }
                            }
                        }
                    }
                ],
            "should" : [
                {"term" : {"SIFT_pred": "Deleterious"}},
                {"term" : {"LRT_pred": "Deleterious"}},
                {"terms" : {"Polyphen2_HDIV_pred": ["possibly_damaging","probably_damaging"]}},
                {"terms" : {"Polyphen2_HVAR_pred": ["possibly_damaging","probably_damaging"]}}
            ]
        }
        },
        "_source": ["Variant", "Chr", "Start", "Ref", "Alt", "refGene", "CLINSIG", "CLNDBN", "sample", "ExAC_ALL", "1000g2015aug_all", "SIFT_pred", "LRT_pred", "Polyphen2_HDIV_pred", "Polyphen2_HVAR_pred"],
        "size": 1000
    }
    """

    es_query_string = es_query_string_template %(indication_for_testing, subject)
    # print(es_query_string)
    body = json.loads(es_query_string)
    response = es.search(index=dataset_obj.es_index_name, doc_type=dataset_obj.es_type_name, body=body)

    total = response['hits']['total']
    tmp_results = response['hits']['hits']
    results = []
    for ele in tmp_results:
        tmp_source = ele['_source']
        tmp_source['es_id'] = ele['_id']
        tmp_source['clinvar_20150629'] = filter_array_dicts(tmp_source['clinvar_20150629'],
                                                     'clinvar_20150629_CLINSIG',
                                                     ['probable-pathogenic', 'pathogenic', 'drug-response', 'histocompatibility'],
                                                     'equal')
        tmp_source['clinvar_20150629'] = filter_array_dicts(tmp_source['clinvar_20150629'],
                                                     'clinvar_20150629_CLNDBN',
                                                     [indication_for_testing,],
                                                     'default')
        tmp_source['sample'] = filter_array_dicts(tmp_source['sample'],
                                                     'sample_ID',
                                                     [subject,],
                                                     'equal')[0]
        if 'ExAC_ALL' in tmp_source:
            tmp_source['AF'] = tmp_source['ExAC_ALL']
        elif '1000g2015aug_all' in tmp_source:
            tmp_source['AF'] = tmp_source['1000g2015aug_all']
        else:
            tmp_source['AF'] = 'N/A'
        if tmp_source['clinvar_20150629']:
            results.append(tmp_source)


    # print(results)
    return results if results else []

def get_not_relevant_clinvar(subject, database_name, indication_for_testing):
    dataset_obj = Dataset.objects.get(name=database_name)

    es = elasticsearch.Elasticsearch(host=dataset_obj.es_host)

    es_query_string_template = """
    {
        "query": {
            "bool": {
                "minimum_should_match": 1,
                "filter": [
                        {"nested": {
                            "path": "clinvar_20150629",
                            "query": {
                                "bool": {
                                    "must_not": [{"term": {"clinvar_20150629.clinvar_20150629_CLNDBN": "%s"}}]
                                    }
                                }
                            }
                        },
                        {"nested": {
                            "path": "clinvar_20150629",
                            "query": {
                                "bool": {
                                    "filter": [{"terms": {"clinvar_20150629.clinvar_20150629_CLINSIG": ["probable-pathogenic", "pathogenic", "drug-response", "histocompatibility"]}}]
                                    }
                                }
                            }
                        },
                        {"nested": {
                            "path": "sample",
                            "query": {
                                "bool": {
                                    "filter": [{"terms": {"sample.sample_ID": ["%s"]}}]
                                    }
                                }
                            }
                        }
                    ],
                "should" : [
                    {"term" : {"SIFT_pred": "Deleterious"}},
                    {"term" : {"LRT_pred": "Deleterious"}},
                    {"terms" : {"Polyphen2_HDIV_pred": ["possibly_damaging","probably_damaging"]}},
                    {"terms" : {"Polyphen2_HVAR_pred": ["possibly_damaging","probably_damaging"]}}
                ]
                }
            },
        "_source": ["Variant", "Chr", "Start", "Ref", "Alt", "refGene", "clinvar_20150629", "sample", "ExAC_ALL", "1000g2015aug_all", "SIFT_pred", "LRT_pred", "Polyphen2_HDIV_pred", "Polyphen2_HVAR_pred"],
        "size": 25
    }

    """
    es_query_string = es_query_string_template %(indication_for_testing, subject)
    # print(es_query_string)
    body = json.loads(es_query_string)
    response = es.search(index=dataset_obj.es_index_name, doc_type=dataset_obj.es_type_name, body=body)

    total = response['hits']['total']
    tmp_results = response['hits']['hits']
    results = []
    for ele in tmp_results:
        tmp_source = ele['_source']
        tmp_source['es_id'] = ele['_id']
        tmp_source['clinvar_20150629'] = filter_array_dicts(tmp_source['clinvar_20150629'],
                                                     'clinvar_20150629_CLINSIG',
                                                     ['probable-pathogenic', 'pathogenic', 'drug-response', 'histocompatibility'],
                                                     'equal')
        tmp_source['clinvar_20150629'] = must_not_array_dicts(tmp_source['clinvar_20150629'],
                                                     'clinvar_20150629_CLNDBN',
                                                     [indication_for_testing,],
                                                     'default')
        tmp_source['sample'] = filter_array_dicts(tmp_source['sample'],
                                                     'sample_ID',
                                                     [subject,],
                                                     'equal')[0]
        if 'ExAC_ALL' in tmp_source:
            tmp_source['AF'] = tmp_source['ExAC_ALL']
        elif '1000g2015aug_all' in tmp_source:
            tmp_source['AF'] = tmp_source['1000g2015aug_all']
        else:
            tmp_source['AF'] = 'N/A'
        if tmp_source['clinvar_20150629']:
            results.append(tmp_source)

    # print(results)
    return results if results else []

def get_relevant_gwascatalog(subject, database_name, indication_for_testing):
    dataset_obj = Dataset.objects.get(name=database_name)

    es = elasticsearch.Elasticsearch(host=dataset_obj.es_host)

    es_query_string_template = """
    {
        "query": {
            "bool": {
                "minimum_should_match": 1,
                "must": [
                    {"term": {"gwasCatalog":"%s"}},
                    {"nested": {
                        "path": "sample",
                        "query": {
                            "bool": {
                                "filter": [{"terms": {"sample.sample_ID": ["%s"]}}]
                                }
                            }
                        }
                    }
                    ],
                "should" : [
                    {"term" : {"SIFT_pred": "Deleterious"}},
                    {"term" : {"LRT_pred": "Deleterious"}},
                    {"terms" : {"Polyphen2_HDIV_pred": ["possibly_damaging","probably_damaging"]}},
                    {"terms" : {"Polyphen2_HVAR_pred": ["possibly_damaging","probably_damaging"]}}
                ]
                }
            },
        "_source": ["Variant", "Chr", "Start", "Ref", "Alt", "refGene", "gwasCatalog", "sample", "ExAC_ALL", "1000g2015aug_all", "SIFT_pred", "LRT_pred", "Polyphen2_HDIV_pred", "Polyphen2_HVAR_pred"],
        "size": 1000
    }
    """

    es_query_string = es_query_string_template %(indication_for_testing, subject)
    body = json.loads(es_query_string)
    # print(es_query_string)
    response = es.search(index=dataset_obj.es_index_name, doc_type=dataset_obj.es_type_name, body=body)

    total = response['hits']['total']
    tmp_results = response['hits']['hits']
    results = []
    for ele in tmp_results:
        tmp_source = ele['_source']
        tmp_source['es_id'] = ele['_id']
        tmp_source['sample'] = filter_array_dicts(tmp_source['sample'],
                                                     'sample_ID',
                                                     [subject,],
                                                     'equal')[0]
        if 'ExAC_ALL' in tmp_source:
            tmp_source['AF'] = tmp_source['ExAC_ALL']
        elif '1000g2015aug_all' in tmp_source:
            tmp_source['AF'] = tmp_source['1000g2015aug_all']
        else:
            tmp_source['AF'] = 'N/A'
        results.append(tmp_source)

    # print(results)
    return results if results else []

def get_not_relevant_gwascatalog(subject, database_name, indication_for_testing):
    dataset_obj = Dataset.objects.get(name=database_name)

    es = elasticsearch.Elasticsearch(host=dataset_obj.es_host)

    es_query_string_template = """
    {
        "query": {
            "bool": {
                "minimum_should_match": 1,
                "must_not": {
                    "term": {"gwasCatalog":"%s"}
                },
                "filter": [
                    {"exists": {"field": "gwasCatalog"}},
                    {"nested": {
                        "path": "sample",
                        "query": {
                            "bool": {
                                "filter": [{"terms": {"sample.sample_ID": ["%s"]}}]
                                }
                            }
                        }
                    }
                    ],
                "should" : [
                    {"term" : {"SIFT_pred": "Deleterious"}},
                    {"term" : {"LRT_pred": "Deleterious"}},
                    {"terms" : {"Polyphen2_HDIV_pred": ["possibly_damaging","probably_damaging"]}},
                    {"terms" : {"Polyphen2_HVAR_pred": ["possibly_damaging","probably_damaging"]}}
                ]
            }
        },
        "_source": ["Variant", "Chr", "Start", "Ref", "Alt", "refGene", "gwasCatalog", "sample", "ExAC_ALL", "1000g2015aug_all", "SIFT_pred", "LRT_pred", "Polyphen2_HDIV_pred", "Polyphen2_HVAR_pred"],
        "size": 25
    }
    """

    es_query_string = es_query_string_template %(indication_for_testing, subject)
    body = json.loads(es_query_string)
    response = es.search(index=dataset_obj.es_index_name, doc_type=dataset_obj.es_type_name, body=body)

    total = response['hits']['total']
    tmp_results = response['hits']['hits']
    # print(tmp_results)
    results = []
    for ele in tmp_results:
        tmp_source = ele['_source']
        tmp_source['es_id'] = ele['_id']
        tmp_source['sample'] = filter_array_dicts(tmp_source['sample'],
                                                     'sample_ID',
                                                     [subject,],
                                                     'equal')[0]
        if 'ExAC_ALL' in tmp_source:
            tmp_source['AF'] = tmp_source['ExAC_ALL']
        elif '1000g2015aug_all' in tmp_source:
            tmp_source['AF'] = tmp_source['1000g2015aug_all']
        else:
            tmp_source['AF'] = 'N/A'
        results.append(tmp_source)

    # print(results)
    return results if results else []


def get_read_depth(database_name, subject):
    with open(database_name+'.csv', 'r') as fp:
        for line in fp:
            tmp = line.strip().split(',')
            if tmp[0] == subject:
                print(tmp[2], tmp[3], tmp[4], tmp[6])
                return (tmp[2], tmp[3], tmp[4], tmp[6])

    return ('??'*4)
def get_subject(dataset_name):
    db_name = map_database_name_table_name[dataset_name][0]
    table_name = map_database_name_table_name[dataset_name][1]

    db = MySQLdb.connect(host="bigdw.ccr.buffalo.edu",
                    port=3306,
                    user="gdwdev",
                    passwd="roundNo!se84",
                    db=db_name)

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    #cursor.execute("select chr, start, ref_allele, alt_allele, avsnp142, SIFT_pred, LRT_pred, MutationTaster_pred, Polyphen2_HDIV_pred, Polyphen2_HVAR_pred, FATHMM_pred, cosmic70, clinvar_20150629, gwasCatalog from sim_variant__patient__main where sample_id=2157 AND (clinvar_20150629 IS NOT NULL OR gwasCatalog IS NOT NULL) AND SIFT_pred='D' AND type='SNV';")
    query_statement = "SELECT DISTINCT sample_id FROM %s WHERE gwasCatalog is not null or clinvar_20150629 is not null order by sample_id;" %(table_name)
    cursor.execute(query_statement)

    # Fetch a single row using fetchone() method.
    rows = cursor.fetchall()
    subjects = []
    if rows:
        subjects = [(ele[0], ele[0]) for ele in rows]
        db.close()
        return subjects
    else:
        db.close()
        return None


def get_clinvar_gwascatalog(subject, database_name, indication_for_testing):
    acceptable_clin_vars = ['probable-pathogenic', 'pathogenic', 'drug-response', 'histocompatibility']

    # Open database connection

    db_name = map_database_name_table_name[database_name][0]
    T1 = map_database_name_table_name[database_name][1]
    T2 = map_database_name_table_name[database_name][2]
    T3 = map_database_name_table_name[database_name][3]

    db = MySQLdb.connect(host="bigdw.ccr.buffalo.edu",
                        port=3306,
                        user="gdwdev",
                        passwd="roundNo!se84",
                        db=db_name)

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    query_statement = """SELECT
                            T1.variant_id_key, T1.chr, T1.start, T1.ref_allele, T1.alt_allele,
                            T1.ExAC_ALL, T1.genotype, T1.clinvar_20150629, T1.gwasCatalog,
                            T2.symbol, T2.refGene, T2.cDNA_change, T2.aa_change
                        FROM %s AS T1
                        INNER JOIN %s AS T2
                            ON T1.variant_id_key = T2.variant_id_key
                        WHERE T1.sample_id='%s'
                        AND (clinvar_20150629 IS NOT NULL OR gwasCatalog IS NOT NULL)
                        AND (SIFT_pred='D' OR LRT_pred='D' OR Polyphen2_HDIV_pred='D' OR Polyphen2_HVAR_pred='D')
                        AND type='SNV';"""
    query_statement_filled = query_statement %(T1, T2, subject)
    # print(query_statement_filled)
    cursor.execute(query_statement_filled)
    # Fetch a single row using fetchone() method.
    desc = cursor.description
    header = [ele[0].strip() for ele in desc]
    rows = cursor.fetchall()
    relevant_data = []
    incidental_data = []

    for row in rows:
        data = dict(zip(header,row))
        # pprint(data)
        tmp = data['clinvar_20150629']
        if data.get('gwasCatalog'):
            data['gwasCatalog'] = ', '.join(data['gwasCatalog'].split('Name=')[-1].split(','))
        else:
            data['gwasCatalog'] = ''

        if not data.get('clinvar_20150629'):
            data['clinvar_20150629'] = ''

        if indication_for_testing.lower() in data.get('clinvar_20150629').lower() or indication_for_testing.lower() in data.get('gwasCatalog').lower():
            relevant_data.append(data)
        else:
            incidental_data.append(data)

    # disconnect from server
    db.close()
    return(relevant_data, incidental_data)


def reformat_fields(input_dict):
    acceptable_clin_vars = ['probable-pathogenic', 'pathogenic', 'drug-response', 'histocompatibility']

    variant_id_key = input_dict['variant_id_key']
    variant_id = 'hg19:%s:%s %s/%s' %(input_dict['chr'], input_dict['start'], input_dict['ref_allele'], input_dict['alt_allele'])
    allele_frequency = input_dict['ExAC_ALL']

    if input_dict['genotype'] == "1/1":
        zygosity = 'Homozygous'
    else:
        zygosity = 'Heterozygous'

    gene = input_dict['symbol']
    refseq_ids = [ele for ele in input_dict['refGene'] if ele]


    variants = []
    for ele1, ele2 in zip(input_dict['cDNA_change'], input_dict['aa_change']):
        if ele1 and ele2:
            variants.append('%s/%s' %(ele1,ele2) if '%s/%s' %(ele1,ele2) else '')

    clinvar_20150629 = input_dict['clinvar_20150629'] if input_dict['clinvar_20150629'] else ''
    clinvar = []
    if clinvar_20150629:
        tmp = parse_qs(clinvar_20150629)
        for clndbn, clinsig in zip(tmp['CLNDBN'][0].split('|'), tmp['CLINSIG'][0].split('|')):
            if clinsig in acceptable_clin_vars:
                clinvar.append((clndbn.replace('_', ' '), clinsig))

    gwasCatalog = input_dict['gwasCatalog'].replace('_', ' ') if input_dict['gwasCatalog'] else ''


    if clinvar or gwasCatalog:
        return (variant_id_key, variant_id, allele_frequency, zygosity, gene, refseq_ids, variants, clinvar, gwasCatalog)
    else:
        return None


def group_by_variant_id_key(input_array):

    varaint_id_key_dict = defaultdict(list)
    for data in input_array:
        variant_id_key = data['variant_id_key']
        varaint_id_key_dict[variant_id_key].append(data)


    # pprint(varaint_id_key_dict)
    varaint_id_key_array = []
    for variant_id_key in list(varaint_id_key_dict):
        data = varaint_id_key_dict[variant_id_key][0]
        data_dict_combined = {}
        data_dict_combined['variant_id_key'] = data['variant_id_key']
        data_dict_combined['chr'] = data['chr']
        data_dict_combined['start'] = data['start']
        data_dict_combined['ref_allele'] = data['ref_allele']
        data_dict_combined['alt_allele'] = data['alt_allele']
        data_dict_combined['symbol'] = data['symbol']
        data_dict_combined['ExAC_ALL'] = data['ExAC_ALL']
        data_dict_combined['genotype'] = data['genotype']
        data_dict_combined['gwasCatalog'] = data['gwasCatalog']
        data_dict_combined['clinvar_20150629'] = data['clinvar_20150629'].replace('x2c', ', ').replace('_', ' ')
        if len(varaint_id_key_dict[variant_id_key]) > 1:
            for data in varaint_id_key_dict[variant_id_key]:
                if 'refGene' not in data_dict_combined:
                    data_dict_combined['refGene'] = []
                    data_dict_combined['refGene'].append(data['refGene'].replace('_', '\_') if data['refGene'] else '')
                else:
                    data_dict_combined['refGene'].append(data['refGene'].replace('_', '\_') if data['refGene'] else '')

                if 'aa_change' not in data_dict_combined:
                    data_dict_combined['aa_change'] = []
                    data_dict_combined['aa_change'].append(data['aa_change'])
                else:
                    data_dict_combined['aa_change'].append(data['aa_change'])

                if 'cDNA_change' not in data_dict_combined:
                    data_dict_combined['cDNA_change'] = []
                    data_dict_combined['cDNA_change'].append(data['cDNA_change'])
                else:
                    data_dict_combined['cDNA_change'].append(data['cDNA_change'])

        else:
            data_dict_combined['refGene'] = []
            data_dict_combined['refGene'].append(data['refGene'].replace('_', '\_') if data['refGene'] else '')
            data_dict_combined['aa_change'] = []
            data_dict_combined['aa_change'].append(data['aa_change'])
            data_dict_combined['cDNA_change'] = []
            data_dict_combined['cDNA_change'].append(data['cDNA_change'])



        formatted_fields = reformat_fields(data_dict_combined)
        if formatted_fields:
            varaint_id_key_array.append(formatted_fields)

    return varaint_id_key_array

class SubjectReportForm1(forms.Form):
    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', [])
        if not user.is_anonymous():
            user = User.objects.get(username=user)
            user_group_ids = [group.id for group in user.groups.all()]
        else:
            user_group_ids = []
        super(SubjectReportForm1, self).__init__(*args, **kwargs)

        user_dataset = Dataset.objects.all()
        user_dataset = user_dataset.filter(Q(allowed_groups__in=user_group_ids) | Q(is_public=True)).distinct()
        STUDY_CHOICES = [(ele.name, '%s (%s)' %(ele.description, ele.study.name)) for ele in user_dataset]
        STUDY_CHOICES.insert(0, ('','---'))
        self.fields['dataset'] = forms.ChoiceField(label='Dataset', choices=STUDY_CHOICES)


class SubjectReportForm2(forms.Form):
    def __init__(self, *args, **kwargs):
        database_name = kwargs.pop('database_name', [])
        super(SubjectReportForm2, self).__init__(*args, **kwargs)

        dataset_object = Dataset.objects.get(name=database_name)
        SUBJECT_CHOICES = [(ele.value, ele.value) for ele in FilterFieldChoice.objects.filter(filter_field__display_text='sample_ID',
                                                        filter_field__dataset=dataset_object)]
        SUBJECT_CHOICES.insert(0, ('','---'))
        self.fields['subject'] = forms.ChoiceField(label='Subject', choices=SUBJECT_CHOICES)
        self.fields['indication_for_testing'] = forms.CharField(max_length=100)


class SubjectReportForm3(forms.Form):
    def __init__(self, *args, **kwargs):
        extra_data = kwargs.pop('extra_data', [])
        super(SubjectReportForm3, self).__init__(*args, **kwargs)
        subject = extra_data['subject']
        database_name = extra_data['database_name']
        indication_for_testing = extra_data['indication_for_testing'].lower()
        print(indication_for_testing)

        relevant_clinvar = get_relevant_clinvar(subject, database_name, indication_for_testing)
        RELEVANT_CLINVAR_CHOICES = [(ele['es_id'], '') for ele in relevant_clinvar]

        not_relevant_clinvar = get_not_relevant_clinvar(subject, database_name, indication_for_testing)
        NOT_RELEVANT_CLINVAR_CHOICES = [(ele['es_id'], '') for ele in not_relevant_clinvar]

        relevant_gwascatalog = get_relevant_gwascatalog(subject, database_name, indication_for_testing)
        RELEVANT_GWASCATALOG_CHOICES = [(ele['es_id'], '') for ele in relevant_gwascatalog]
        not_relevant_gwascatalog = get_not_relevant_gwascatalog(subject, database_name, indication_for_testing)
        NOT_RELEVANT_GWASCATALOG_CHOICES = [(ele['es_id'], '') for ele in not_relevant_gwascatalog]

        self.fields['relevant_clinvar'] = forms.MultipleChoiceField(choices=RELEVANT_CLINVAR_CHOICES, widget=forms.CheckboxSelectMultiple(), required=False)
        self.fields['not_relevant_clinvar'] = forms.MultipleChoiceField(choices=NOT_RELEVANT_CLINVAR_CHOICES, widget=forms.CheckboxSelectMultiple(), required=False)
        self.fields['relevant_gwascatalog'] = forms.MultipleChoiceField(choices=RELEVANT_GWASCATALOG_CHOICES, widget=forms.CheckboxSelectMultiple(), required=False)
        self.fields['not_relevant_gwascatalog'] = forms.MultipleChoiceField(choices=NOT_RELEVANT_GWASCATALOG_CHOICES, widget=forms.CheckboxSelectMultiple(), required=False)


class SubjectReportForm4(forms.Form):
    def __init__(self, *args, **kwargs):
        extra_data = kwargs.pop('extra_data', [])
        database_name = extra_data['database_name']
        subject = extra_data['subject']
        indication_for_testing = extra_data['indication_for_testing']
        super(SubjectReportForm4, self).__init__(*args, **kwargs)

        dataset_object = Dataset.objects.get(name=database_name)
        try:
            sample_read_depth_obj = SampleReadDepth.objects.get(dataset=dataset_object, sample_id=subject)
            result_summary = result_summary_template %( sample_read_depth_obj.rd_15x,
                                                        sample_read_depth_obj.rd_20x,
                                                        sample_read_depth_obj.rd_40x,
                                                        sample_read_depth_obj.variant_count,
                                                        indication_for_testing)
            methodology = methodology_default_value
            additional_notes = 'DISCLAIMER: The findings in this report have not been verified by a physician.'
        except:
            result_summary = 'Demo database'
            methodology = 'Demo database'
            additional_notes = 'Demo database'

        self.fields['result_summary'] = forms.CharField(widget=forms.Textarea, initial=result_summary)
        self.fields['methodology'] = forms.CharField(widget=forms.Textarea, initial=methodology)
        self.fields['additional_notes'] = forms.CharField(widget=forms.Textarea, initial=additional_notes)

