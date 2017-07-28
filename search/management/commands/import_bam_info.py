from django.core.management.base import BaseCommand
from search.models import *
from pybamview.models import SampleBamInfo


class Command(BaseCommand):

    def handle(self, *args, **options):

        dataset_object = Dataset.objects.get(name='sim_wgs_case_wes_control')
        for choice in open('./search/management/commands/data/wgs_case_ids.txt','r'):
            file_path = "/gpfs/projects/academic/big/SIM/WGS_case/hc_bam/SIMWGS09292016_p%s_merged_hc.bam" %(choice.strip())
            a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                            sample_id=choice.strip(),
                                            file_path=file_path)

        for choice in open('./search/management/commands/data/wes_control_ids.txt','r'):
            file_path = "/gpfs/projects/academic/big/SIM/WES_control/hc_bam/SIMCT20160930_%s_merged_hc.bam" %(choice.strip())
            a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                            sample_id=choice.strip(),
                                            file_path=file_path)

        dataset_object = Dataset.objects.get(name='sim_wes_case')
        for choice in open('./search/management/commands/data/wes_case_ids.txt','r'):
            file_path = "/gpfs/projects/academic/big/SIM/WES_case/hc_bam/SIMWES20160930_%s_merged_hc.bam" %(choice.strip())
            a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                            sample_id=choice.strip(),
                                            file_path=file_path)

