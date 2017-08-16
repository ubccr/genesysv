from django.core.management.base import BaseCommand
from search.models import *
from pybamview.models import SampleBamInfo



class Command(BaseCommand):



    def handle(self, *args, **options):

        dataset_object = Dataset.objects.get(name="import_sim_wgs_case_wes_control_read_depth")


        with open('./search/management/commands/data/case_wes_rd_v_count.txt','r') as fp:
            for line in fp:
                if line.strip():
                    sample_id, rd_10x, rd_15x, rd_20x, rd_40x, rd_50x, variant_count = line.split(',')
                    SampleReadDepth.objects.get_or_create(dataset=dataset_object,
                                                          sample_id=sample_id,
                                                          rd_10x=rd_10x,
                                                          rd_15x=rd_15x,
                                                          rd_20x=rd_20x,
                                                          rd_40x=rd_40x,
                                                          rd_50x=rd_50x,
                                                          variant_count=variant_count)
