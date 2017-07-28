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


STUDIES = (
    ("sim",
     "Statin Induced Myopathy (SIM)",
     "msea_sim",
     "199.109.194.243",
     "9200",
     "20170222_SIM_WGS-20170424-expand_lookahead.txt",
     25274
    ),
)

SIM_DATASETS = (
    ('sim', 'sim_wgs', 'Sensitive WGS variants'),
    ('sim', 'sim_con', 'Resistant WES variants'),
    ('sim', 'sim_wgs_minus_con', 'Sensitive minus Resistant variants'),
    ('sim', 'sim_con_minus_wgs', 'Resistant minus Sensitive variants'),
)



class Command(BaseCommand):

    def handle(self, *args, **options):


        for short_name, full_name in VARIANT_FILE_TYPE_CHOICES:
            VariantSet.objects.get_or_create(short_name=short_name, full_name=full_name)

        for short_name, display_name, es_index_name, es_host, es_port, filename, no_lines in STUDIES:
            study_obj, _ = Study.objects.get_or_create(short_name=short_name,
                                                  display_name=display_name,
                                                  es_index_name=es_index_name,
                                                  es_host=es_host,
                                                  es_port=es_port,
                                                  is_public=True)


            filename_path = './msea/management/commands/data/%s' %(filename)
            with transaction.atomic():
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


                        rs_object, _ = ReferenceSequence.objects.get_or_create(study=study_obj,
                                                                               rs_id=rs_id,
                                                                               gene=gene_object)
                        for variant in variants:
                            var_obj = VariantSet.objects.get(short_name=variant.strip())
                            rs_object.variants.add(var_obj)

                        rs_object.save()


        study_obj = Study.objects.get(short_name='sim')

        for study_short_name, short_name, display_name in SIM_DATASETS:
            Dataset.objects.get_or_create(study=study_obj,
                                          short_name=short_name,
                                          display_name=display_name)
