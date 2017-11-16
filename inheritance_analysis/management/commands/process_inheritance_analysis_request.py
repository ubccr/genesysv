from django.core.management.base import BaseCommand
from pprint import pprint
from inheritance_analysis.models import InheritanceAnalysisRequest
from msea.models import Gene
import json
import elasticsearch
from tqdm import tqdm

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
    results = {}
    variant_id_map = {}

    for doc in res['hits']['hits']:
        if doc['_source']['Func_refGene'] in not_exonic:
            continue

        variant_id_map[doc['_source']['Variant']] = doc['_id']
        samples = doc['_source']['sample']
        # pprint(samples)
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


            if momgt.count('0') == 2 and dadgt.count('0') == 2 and '1' in childgt:
                if doc['_id'] in results:
                    results[doc['_id']].setdefault('denovo',[]).append(family_id)
                    # results[doc['_id']]['denovo'].append(family_id)
                else:
                    results[doc['_id']] = {'denovo':[family_id]}
            elif momgt.count('0') == 1 and dadgt.count('0') == 1 and childgt.count('1') == 2:
                if doc['_id'] in results:
                    results[doc['_id']].setdefault('hom_recess',[]).append(family_id)
                    # results[doc['_id']]['hom_recess'].append(family_id)
                else:
                    results[doc['_id']] = {'hom_recess':[family_id]}

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
        #print(family)
        if len(set(family['mom'] + family['dad'])) <= 2:
            continue

        # for variant in list(set(family['mom']).symmetric_difference(family['dad']))
        for var1 in family['mom']:
            if var1 in family['dad']:
                continue
            for var2 in family['dad']:
                if var2 in family['mom']:
                    continue
                if var1 in family['child'] and var2 in family['child']:
                    #print(var1,var2)
                    if variant_id_map[var1] in results:
                        results[variant_id_map[var1]].setdefault('comp_het',[]).append({family_id:var2})
                        results[variant_id_map[var1]]['comp_het'].append({family_id:var2})
#
                    else:
                        results[variant_id_map[var1]] = {'comp_het':[{family_id:var2}]}

                    if variant_id_map[var2] in results:
                        results[variant_id_map[var2]].setdefault('comp_het',[]).append({family_id:var1})
                        # results[variant_id_map[var2]]['comp_het'].append({family_id:var1})
                    else:
                        results[variant_id_map[var2]] = {'comp_het':[{family_id:var1}]}

    #pp(results)
    #print()
    return(results)


# form the body of the ES update. Executed once per elasticsearch id returned by analyse_gene,
# so could conceivably update every record in an index if enough families existed per variant.
# It's very likely this is overcomplicated due to the format of analyse_gene's output, which itself
# is a mess.
def create_update_body(trio_output):
    comphet_string = ''
    denovo_string = ''
    homrecess_string = ''
    for var_type,var_info in trio_output.items():
        if var_type == 'comp_het':
            comphet_string = '"comp_het" : ['
            for e in var_info:
                for family_id,assoc_var in e.items():
                    comphet_string += '{{"family" : "{}",\n"assocvar" : "{}"}}'.format(family_id,assoc_var)

            comphet_string = comphet_string.replace('}{','},{')
            comphet_string += ']'

        elif var_type == 'denovo':
            denovo_string = '"denovo" : {}'.format(str(var_info).replace("'",'"'))

        elif var_type == 'hom_recess':
            homrecess_string = '"hom_recess" : {}'.format(str(var_info).replace("'",'"'))

    update_string = ','.join(filter(None,[comphet_string,denovo_string,homrecess_string]))

    body = """
        {
            "doc" : {
                %s
            }
        }"""%(update_string)
    #print(body)
    #print(json.loads(body))
    return(json.loads(body))

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
        print(request_obj)

        # # gl = ['ADAMTSL1','VAV3','SYNE2'] # test gene set that has complex hets
        es = elasticsearch.Elasticsearch(host=dataset.es_host, port=dataset.es_port)

        family_pedigree = json.loads(request_obj.ped_json)

        pprint(family_pedigree)


        gene_list = [gene.gene_name for gene in Gene.objects.all()]
        no_line = 1
        for gene in tqdm(gene_list, total=len(gene_list)):

            gene_query = query_by_gene(gene)
            results = es.search(index=dataset.es_index_name,doc_type=dataset.es_type_name,body=gene_query)
            # print(gene_count, gene, results['hits']['total'])
            if int(results['hits']['total']) > 0:
                results = analyse_gene(family_pedigree,results)
                if results:
                    #pp('final results')
                    pass
                    # pprint(results)
                    for esid in results:
                        # print(esid)
                        es.update(index=dataset.es_index_name, doc_type=dataset.es_type_name, id=esid, body=create_update_body(results[esid]))
                        # print(create_update_body(results[esid]))
            no_line += 1
