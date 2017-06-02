from django.db import transaction
from tqdm import tqdm
from collections import deque
import sys
import os
import statistics
from django.core.management.base import BaseCommand
from msea.models import *

VARIANT_FILE_TYPE_CHOICES = (('ass', 'All Silent SNVs'),
                         ('asbns', 'All Silent Benign non-Silent SNVs'),
                         ('ansi', 'All non-Silent SNVs and Indels'),
                         ('ans', 'All non-Silent SNVs' ),
                         ('dnsi', 'Deleterious non-Silent SNVs and Indels'),
                         ('dns', 'Deleterious non-Silent SNVs'),
                         ('lof', 'Loss of Function'),
                         ('prom', 'Variants in Promoter Region'),
                         ('miss', 'Missense'),
                         ('eigenset', 'Eigen score >= 0.65'),
                         ('revelset', 'REVEL score >= 0.50'),
                    )

def estimate_no_lines(filename, no_lines_for_estimating):
    no_lines = 0
    size_list = deque()

    with open(filename, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue

            if no_lines_for_estimating < no_lines:
                break

            size_list.append(sys.getsizeof(line))

            no_lines += 1

    filesize = os.path.getsize(filename)

    no_lines = int(filesize/statistics.median(size_list))

    return no_lines




class Command(BaseCommand):

    def handle(self, *args, **options):


        for short_name, full_name in VARIANT_FILE_TYPE_CHOICES:
            VariantSet.objects.get_or_create(short_name=short_name, full_name=full_name)


        DATASETS = (
                ("sim_wgs",
                 "SIM Sensitive Whole Genome Sequence",
                 "msea",
                 "msea",
                 "199.109.195.45",
                 "9200",
                 "20170222_SIM_WGS-20170416-expand_lookahead.txt"),


                ("sim_res",
                 "SIM Resistant Whole Exome Sequence",
                 "msea",
                 "msea",
                 "199.109.195.45",
                 "9200",
                 "20170307_SIM_Control-20170416-expand_lookahead.txt"),
        )



        for dataset, display_name, es_index_name, es_type_name, es_host, es_port, filename in DATASETS:
            msea_datset_obj, _ = MseaDataset.objects.get_or_create(dataset=dataset,
                                                              display_name=display_name,
                                                              es_index_name=es_index_name,
                                                              es_type_name=es_type_name,
                                                              es_host=es_host,
                                                              es_port=es_port,
                                                              is_public=True)


            filename_path = './msea/management/commands/data/%s' %(filename)
            no_lines = estimate_no_lines(filename_path, 200000)
            with open(filename_path, 'r') as fp:
                for line in tqdm(fp, total=no_lines):
                #for idx, line in enumerate(fp):
                    if line.startswith('#') or not line.strip():
                        continue

                    # print(line)
                    line_split = line.strip().split()
                    gene_name = line_split[0].strip()
                    rs_id = line_split[1].strip()
                    variants = line_split[2:]
                    # if (idx % 1000) == 0:
                    #     print(idx, line_split)
                    gene_object, _ = Gene.objects.get_or_create(gene_name=gene_name)


                    rs_object, _ = ReferenceSequence.objects.get_or_create(msea_dataset=msea_datset_obj,
                                                                           rs_id=rs_id,
                                                                           gene=gene_object)
                    for variant in variants:
                        var_obj = VariantSet.objects.get(short_name=variant.strip())
                        rs_object.variants.add(var_obj)

                    rs_object.save()


