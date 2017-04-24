''' Script to import initial static fos data.
'''

from django.core.management.base import BaseCommand
from msea.models import MseaDataset


class Command(BaseCommand):

    def handle(self, *args, **options):
        MSEA_DATASET_CHOICE = ( ('sim_wgs', 'SIM Sensitive Genome Whole Sequence', True),
                                ('sim_res', 'SIM Resistant Exome Whole Sequence', True),
                            )

        for dataset, display_name, is_public in MSEA_DATASET_CHOICE:
            MseaDataset.objects.get_or_create(dataset=dataset,
                                              display_name=display_name,
                                              is_public=is_public)
