import json
from collections import defaultdict
from pprint import pprint

import elasticsearch
from django.core.management.base import BaseCommand
from tqdm import tqdm

from inheritance_analysis.models import InheritanceAnalysisRequest
from msea.models import Gene


def query_by_gene(gene):
    query_template = """
        {
            "size": 10000,
            "query": {
                "nested" : {
                    "path" : "refGene",
                    "query" : {
                        "bool" : {
                            "must" : [
                                { "match" : {"refGene.refGene_symbol" : "%s"} }
                            ]
                        }
                    }
                }
            }
        }
    """ %(gene)
    return(query_template)

def analyse_gene(family_pedigree,res):
    not_exonic = ['splicing','ncRNA','UTR5','UTR3','intronic','upstream','downstream',
                  'intergenic','upstream;downstream','exonic;splicing','UTR5;UTR3',]
    comp_het = { family_id:{'mom':[],'dad':[],'child':[]} for family_id in family_pedigree.keys() }
    child_sample_IDs_by_family = {family_ID:str(sampleIDs[2]) for family_ID,sampleIDs in family_pedigree.items()}
    results = defaultdict(dict)
    variant_id_map = {}

    for doc in res['hits']['hits']:
        if doc['_source']['Func_refGene'] in not_exonic:
            continue

        variant_id_map[doc['_source']['Variant']] = doc['_id'] # used to link complex het variants
        esid = doc['_id']
        samples = doc['_source']['sample']
        #print(doc['_id'])
        sid_gt = {sample['sample_ID']: sample['sample_GT'] for sample in samples}
        # pprint(sid_gt)
        for family_id, family in family_pedigree.items():
            try:
                # print(family)
                momgt = sid_gt[str(family[0])]
                dadgt = sid_gt[str(family[1])]
                childgt = sid_gt[str(family[2])]
            except Exception as e: # at least one gt is not present
                # print(family_id, 'Skipped!')
                continue


            if momgt.count('0') == 2 and dadgt.count('0') == 2 and childgt.count('0') != 2:
                results[esid][child_sample_IDs_by_family[family_id]] = ['sample_denovo',family_id]
            elif momgt.count('0') == 1 and dadgt.count('0') == 1 and childgt.count('0') == 0:
                results[esid][child_sample_IDs_by_family[family_id]] = ['sample_hom-recess',family_id]
                
            # else:
            #     print('What is this?', momgt, dadgt, childgt)


            ## Complex heterozygous
            if momgt == '0/1':
                comp_het[family_id]['mom'].append(doc['_source']['Variant'])
            if dadgt == '0/1':
                comp_het[family_id]['dad'].append(doc['_source']['Variant'])
            if childgt == '0/1':
                comp_het[family_id]['child'].append(doc['_source']['Variant'])
            #gene = doc['_source']['refGene'][0]['refGene_symbol']


    for family_id,family in comp_het.items(): 
        #print(family_id,family)
        if len(set(family['mom'] + family['dad'])) <= 2:
            continue

        for var1 in family['mom']:
            if var1 in family['dad']:
                continue
            for var2 in family['dad']:
                if var2 in family['mom']:
                    continue
                if var1 in family['child'] and var2 in family['child']:
                    results[variant_id_map[var1]][child_sample_IDs_by_family[family_id]] = ['sample_comp-het',family_id]
                    results[variant_id_map[var2]][child_sample_IDs_by_family[family_id]] = ['sample_comp-het',family_id]
                    
                    # this code for adding sample_assoc-var currently overwrites any associated variants that are already there
                    # need to check if sample_assoc-var exists and if so, append to list;
                    # otherwise create it and populate the list with the associated variant
                    #results[variant_id_map[var1]][child_sample_IDs_by_family[family_id]] = [{'sample_comp-het':family_id},{'sample_assoc-var':var2}]
                    #results[variant_id_map[var2]][child_sample_IDs_by_family[family_id]] = [{'sample_comp-het':family_id},{'sample_assoc-var':var1}]
                
    #pp(results)
    #print()
    return(results)


# Form the body of the ES update. Executed once per elasticsearch id returned by analyse_gene,
# so could conceivably update every record in an index if enough families existed per variant.
def create_update_body(es,esid,dataset,trio_output):
    update_body = defaultdict(dict)
    
    record = es.get(index=dataset.es_index_name,doc_type=dataset.es_type_name,id=esid)
    sample_data = record['_source']['sample']
    
    for sample in sample_data:
        if sample['sample_ID'] in trio_output.keys():
            temp_sample = sample
            field_to_insert = trio_output[sample['sample_ID']]
            temp_sample[field_to_insert[0]] = field_to_insert[1]
            sample_data.pop(sample_data.index(sample))
            sample_data.append(temp_sample)
    
    update_body['doc']['sample'] = sample_data
    return(json.loads(json.dumps(update_body))) # the loads + dumps lets me work with defaultdict

class Command(BaseCommand):

    def add_arguments(self, parser):
        # Positional arguments
        # parser.add_argument('request_id', type=int)
        parser.add_argument(
            '--request_id',
            action='store',
            dest='request_id',
            default=False,
            help='Delete poll instead of closing it',

        )


    def handle(self, *args, **options):


        request_obj = InheritanceAnalysisRequest.objects.get(id=options['request_id'])
        dataset = request_obj.dataset
        #print(request_obj)

        # # gl = ['ADAMTSL1','VAV3','SYNE2'] # test gene set that has complex hets
        es = elasticsearch.Elasticsearch(host=dataset.es_host, port=dataset.es_port)

        family_pedigree = json.loads(request_obj.ped_json)

        #pprint(family_pedigree)
        gene_list = [gene.gene_name for gene in Gene.objects.all()]
        no_line = 1
        for gene in tqdm(gene_list, total=len(gene_list)):
        #for gene in gene_list:

            gene_query = query_by_gene(gene)
            results = es.search(index=dataset.es_index_name,doc_type=dataset.es_type_name,body=gene_query)
            if int(results['hits']['total']) > 0:
                results = analyse_gene(family_pedigree,results)
                if results:
                    for esid in results:
                        # print(esid)
                        es.update(index=dataset.es_index_name, doc_type=dataset.es_type_name, id=esid, body=create_update_body(es,esid,dataset,results[esid]))
                        #print(esid,'\n',create_update_body(es,esid,dataset,results[esid]),'\n')
            no_line += 1
