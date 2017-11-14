from timeit import default_timer as timer
import gzip
import json
import elasticsearch
import pandas
from pprint import pprint as pp


def get_user_ids_associated_with_group(user):
    user_ids = []
    for group in user.groups.all():
        for user in group.user_set.all():
            if user.id not in user_ids:
                user_ids.append(user.id)
    return user_ids

def get_datasets_associated_with_user(user):
    dataset_ids = []
    for group in user.groups.all():
        for dataset in group.dataset_set.all():
            if dataset.id not in dataset_ids:
                dataset_ids.append(dataset.id)
    return dataset_ids

# construct gene list
def build_list_from_refflat(rf):
    gene_list = []
    with gzip.open(rf,'rt') as fin:
        for line in fin:
            cols = line.split('\t')
            if cols[1].startswith('NM_'):
                gene_list.append(cols[0])

    gene_list = list(set(gene_list))
    print (len(gene_list), len(set(gene_list)))
    return(gene_list)

# query used to verify sample names listed in ped file

sample_id_query = """
{
  "size": 0,
  "aggs" : {
    "values" : {
      "nested" : {
        "path" : "sample"
      },
      "aggs" : {
        "values" : {"terms" : {"field" : "sample.sample_ID", "size" : 3000}}
      }
    }
  }
}
"""


# query used in concert with the gene list
def query_by_gene(gene):
    query_template = """
        {
            "size": 1000,
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


def sample_exist(dataset, sample_id):
    pass

# currently ignoring 'phen' field because the analysis is limited to
# unaffected parents + affected child, so phenotype is already assumed.
def validate_ped(ped, dataset):
    sheet = pandas.read_table(ped, comment='#', sep='\s+', header=0)
    expected_header = ['FAM_ID','IND_ID','MAT_ID','PAT_ID','PHEN','SEX']

    # verify headers
    sheet.columns = [i.upper() for i in sheet.columns]
    if sorted(sheet.columns) != expected_header:
        return((False, 'Header mismatch. Expected %s but found %s' %(', '.join(expected_header), ', '.join(sheet.columns))))

    # verify samples exist in database
    es = elasticsearch.Elasticsearch(host=dataset.es_host, port=dataset.es_port)
    result = es.search(index=dataset.es_index_name, doc_type=dataset.es_type_name, body=sample_id_query)
    database_sample_list = [str(ele['key']) for ele in result['aggregations']['values']['values']['buckets']]
    ped_sample_list = [str(ele) for ele in sheet['IND_ID']]

    invalid_samples = sorted(list(set(ped_sample_list)-set(database_sample_list)))

    if invalid_samples:
       return((False, 'The following sample name(s) are not in the {}: {}'.format(dataset.description, ', '.join(invalid_samples))))

    # verify 3 members in each family; verify each member is assigned different & correct status
    errors = []
    family_record = {} # key = famid, value = [mat id,pat id,child id]
    # fam_id ind_id  pat_id  mat_id  sex phen
    # ashk1   1001   1004   1000   1   0
    # ashk1   1000   0       0       2   0
    # ashk1   1004   0       0       1   0
    for family_tuple in sheet.groupby('FAM_ID'):
        family_id = family_tuple[0]
        family_info = family_tuple[1]
        if len(family_info) != 3:
            errors.append('{} members in family {}, need 3'.format(len(fam),famid))
            continue

        found_child = False
        mismatched_child = False
        for row in family_info.iterrows():
            row = row[1]
            print('row', row)
            if 0 not in [row['PAT_ID'],row['MAT_ID']]: # no 0's -> this should be child
                if sorted(family_info['IND_ID']) == sorted([row['IND_ID'],row['PAT_ID'],row['MAT_ID']]):
                    child = pandas.DataFrame(row).T
                    found_child = True
                else:
                    mismatched_child = True

        if mismatched_child:
            errors.append('pat/mat-id in child row doesn\'t match ind-id in pat/mat row for family {}'.format(family_id))
            continue

        if not found_child:
            errors.append('child row not found for family {}'.format(family_id))
            continue

        father = family_info[family_info.IND_ID.isin([child['PAT_ID'].item()])]
        sex_father = father['SEX'].item()
        mother = family_info[family_info.IND_ID.isin([child['MAT_ID'].item()])]
        sex_mother = mother['SEX'].item()
        if sex_father != 1:
            errors.append('family {}: sex of father is {}, should be 1'
                .format(family_id, sex_father))
        if sex_mother != 2:
            errors.append('family {}: sex of mother is {}, should be 2'
                .format(family_id, sex_mother))

        family_record[family_id] = [mother['IND_ID'].item(),father['IND_ID'].item(),child['IND_ID'].item()]

    if errors:
        return((False, '; '.join(errors)))
    else:
        return((True, family_record))

# discover the denovo, homozygous recessive, and compound heterozygous variants in a gene,
# per family provided in fr. store results in an overcomplicated data structure.
def analyse_gene(fr,res):
    not_exonic = ['splicing','ncRNA','UTR5','UTR3','intronic','upstream','downstream',
                  'intergenic','upstream;downstream','exonic;splicing','UTR5;UTR3',]
    comp_het = { fid:{'mom':[],'dad':[],'child':[]} for fid in fr.keys() }
    results = {}
    variant_id_map = {}

    for doc in res['hits']['hits']:
        if doc['_source']['Func_refGene'] in not_exonic:
            continue

        variant_id_map[doc['_source']['Variant']] = doc['_id']
        samples = doc['_source']['sample']
        sam_list = [sample['sample_ID'] for sample in samples]
        sid_gt = {d['sample_ID']: d['sample_GT'] for d in samples}
        for fid,family in fr.items():
            try:
                momgt = sid_gt[family[0]]
                dadgt = sid_gt[family[1]]
                childgt = sid_gt[family[2]]
            except Exception as e: # at least one gt is not present
                continue

            if momgt == '0/0' and dadgt == '0/0' and '1' in childgt:
                if doc['_id'] in results:
                    results[doc['_id']].setdefault('denovo',[]).append(fid)
                else:
                    results[doc['_id']] = {'denovo':[fid]}
                #continue
            elif momgt == '0/1' and dadgt == '0/1' and childgt == '1/1':
                if doc['_id'] in results:
                    results[doc['_id']].setdefault('hom_recess',[]).append(fid)
                else:
                    results[doc['_id']] = {'hom_recess':[fid]}
                #continue

            if momgt == '0/1':
                comp_het[fid]['mom'].append(doc['_source']['Variant'])
            if dadgt == '0/1':
                comp_het[fid]['dad'].append(doc['_source']['Variant'])
            if childgt == '0/1':
                comp_het[fid]['child'].append(doc['_source']['Variant'])
            #gene = doc['_source']['refGene'][0]['refGene_symbol']


    for fid,family in comp_het.items():
        #print(family)
        if len(set(family['mom'] + family['dad'])) <= 2:
            continue
        for var1 in family['mom']:
            if var1 in family['dad']:
                continue
            for var2 in family['dad']:
                if var2 in family['mom']:
                    continue
                if var1 in family['child'] and var2 in family['child']:
                    #print(var1,var2)
                    if variant_id_map[var1] in results:
                        results[variant_id_map[var1]].setdefault('comp_het',[]).append({fid:var2})
                    else:
                        results[variant_id_map[var1]] = {'comp_het':[{fid:var2}]}
                    if variant_id_map[var2] in results:
                        results[variant_id_map[var2]].setdefault('comp_het',[]).append({fid:var1})
                    else:
                        results[variant_id_map[var2]] = {'comp_het':[{fid:var1}]}

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


def main():
    gl = ['ADAMTSL1','VAV3','SYNE2'] # test gene set that has complex hets
    host = '199.109.192.195' # change to not be hard-coded
    port = '9200' # change to not be hard-coded
    index = 'trio' # change to not be hard-coded
    doc_type = 'trio' # change to not be hard-coded
    ped = '/home/paulspur/trio_analysis_dev/ashkenazim-trio.ped' # change to not be hard-coded
    es = elasticsearch.Elasticsearch(host=host, port=port)

    fr = validate_ped(ped,es,index,doc_type)
    if not fr:
        exit('invalid ped file') # need better system for this
    #else:
    #    print('fr: {}'.format(fr))
    gene_list = build_list_from_refflat('refFlat.txt.gz')
    for gene in gene_list:
    #for gene in gl:
        gene_query = query_by_gene(gene)
        #pp(gene_query)
        res = es.search(index=index,doc_type=doc_type,body=gene_query)
        if int(res['hits']['total']) > 0:
            results = analyse_gene(fr,res)
            if results:
                #pp('final results')
                #pp(results)
                for esid in results:
                    print(esid)
                    #es.update(index='sim_sam', doc_type='sim', id=esid,
                    #          body=create_update_body(results[esid]))
                    print(create_update_body(results[esid]))
                    print()


if __name__=='__main__':
    main()
