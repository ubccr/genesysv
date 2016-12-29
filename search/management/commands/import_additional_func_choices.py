from django.core.management.base import BaseCommand
from search.models import *
from pybamview.models import SampleBamInfo


FUNC_ENSGENE_CHOICES = (
    "exonic;splicing",
    "frameshift_deletion",
    "frameshift_insertion",
    "intergenic",
    "intronic",
    "ncRNA_exonic",
    "ncRNA_exonic;splicing",
    "ncRNA_intronic",
    "ncRNA_splicing",
    "ncRNA_UTR5",
    "nonframeshift_deletion",
    "nonframeshift_insertion",
    "nonsynonymous_SNV",
    "splicing",
    "stopgain",
    "stoploss",
    "synonymous_SNV",
    "unknown",
    "upstream",
    "upstream;downstream",
    "UTR3",
    "UTR5",
    "UTR5;UTR3",
)

FUNC_REFGENE_CHOICES = (
    "downstream",
    "exonic",
    "exonic;splicing",
    "frameshift_deletion",
    "frameshift_insertion",
    "intergenic",
    "intronic",
    "ncRNA_exonic",
    "ncRNA_exonic;splicing",
    "ncRNA_intronic",
    "ncRNA_splicing",
    "ncRNA_UTR5",
    "nonframeshift_deletion",
    "nonframeshift_insertion",
    "nonsynonymous_SNV",
    "splicing",
    "stopgain",
    "stoploss",
    "synonymous_SNV",
    "unknown",
    "upstream",
    "upstream;downstream",
    "UTR3",
    "UTR5",
    "UTR5;UTR3",
)




class Command(BaseCommand):

    def handle(self, *args, **options):

        for dataset_name in ("sim_case_control", "sim_case_wes", "demo"):

            dataset_object = Dataset.objects.get(name=dataset_name)

            Func_ensGene_obj = FilterField.objects.get(dataset=dataset_object, es_name="Func_ensGene")
            for choice in Func_ensGene_obj.filterfieldchoice_set.all():
                choice.delete()
            for choice in FUNC_ENSGENE_CHOICES:
                FilterFieldChoice.objects.get_or_create(filter_field=Func_ensGene_obj, value=choice)

            Func_refGene_obj = FilterField.objects.get(dataset=dataset_object, es_name="Func_refGene")
            for choice in Func_refGene_obj.filterfieldchoice_set.all():
                choice.delete()
            for choice in FUNC_REFGENE_CHOICES:
                FilterFieldChoice.objects.get_or_create(filter_field=Func_refGene_obj, value=choice)


