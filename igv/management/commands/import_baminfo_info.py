from django.core.management.base import BaseCommand

from igv.models import AnnotationReference, SampleBamInfo
from search.models import Dataset


class Command(BaseCommand):

    def handle(self, *args, **options):

        try:
            dataset_object = Dataset.objects.get(
                name='sim_wgs_case_wes_control')
        except Exception as e:
            print("Missing dataset: sim_wgs_case_wes_control")
            dataset_object = None

        if dataset_object:
            AnnotationReference.objects.get_or_create(
                dataset=dataset_object,
                genome_id="hg19",
                annotation_name="Human Genome (GRCh37), version 18 (Ensembl 73)",
                annotation_file_format="gtf",
                annotation_url="https://bam.ccr.buffalo.edu/hg19/gencode.v18.annotation.sorted.gtf.gz",
                annotation_index_url="https://bam.ccr.buffalo.edu/hg19/gencode.v18.annotation.sorted.gtf.gz.tbi",
                reference_fasta_url="https://bam.ccr.buffalo.edu/hg19/hg19.fasta",
                reference_cytoband_url="https://bam.ccr.buffalo.edu/hg19/cytoBand.txt"
            )

            for choice in open('./igv/management/commands/data/wgs_case_ids.txt', 'r'):
                file_path = "/SIM/WGS_case/hc_bam/SIMWGS09292016_p%s_merged_hc.bam" % (
                    choice.strip())
                a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                                           bam_server='https://bam.ccr.buffalo.edu',
                                                           sample_id=choice.strip(),
                                                           file_path=file_path)

            for choice in open('./igv/management/commands/data/wes_control_ids.txt', 'r'):
                file_path = "/SIM/WES_control/hc_bam/SIMCT20160930_%s_merged_hc.bam" % (
                    choice.strip())
                a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                                           bam_server='https://bam.ccr.buffalo.edu',
                                                           sample_id=choice.strip(),
                                                           file_path=file_path)

        try:
            dataset_object = Dataset.objects.get(name='sim_wes_case')
        except Exception as e:
            print("Missing dataset: sim_wes_case")
            dataset_object = None

        if dataset_object:
            AnnotationReference.objects.get_or_create(
                dataset=dataset_object,
                genome_id="hg19",
                annotation_name="Human Genome (GRCh37), version 18 (Ensembl 73)",
                annotation_file_format="gtf",
                annotation_url="https://bam.ccr.buffalo.edu/hg19/gencode.v18.annotation.sorted.gtf.gz",
                annotation_index_url="https://bam.ccr.buffalo.edu/hg19/gencode.v18.annotation.sorted.gtf.gz.tbi",
                reference_fasta_url="https://bam.ccr.buffalo.edu/hg19/hg19.fasta",
                reference_cytoband_url="https://bam.ccr.buffalo.edu/hg19/cytoBand.txt"
            )

            for choice in open('./igv/management/commands/data/wes_case_ids.txt', 'r'):
                file_path = "/SIM/WES_case/hc_bam/SIMWES20160930_%s_merged_hc.bam" % (
                    choice.strip())
                a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                                           bam_server='https://bam.ccr.buffalo.edu',
                                                           sample_id=choice.strip(),
                                                           file_path=file_path)
        try:
            dataset_object = Dataset.objects.get(name='empiregen')
        except Exception as e:
            print("Missing dataset: empiregen")
            dataset_object = None

        if dataset_object:
            AnnotationReference.objects.get_or_create(
                dataset=dataset_object,
                genome_id="hg38",
                annotation_name="Human Genome (GRCh38), version 24 (Ensembl 83)",
                annotation_file_format="gtf",
                annotation_url="https://bam.ccr.buffalo.edu/hg38/gencode.v24.annotation.sorted.gtf.gz",
                annotation_index_url="https://bam.ccr.buffalo.edu/hg38/gencode.v24.annotation.sorted.gtf.gz.tbi",
                reference_fasta_url="https://bam.ccr.buffalo.edu/hg38/hg38.fa",
                reference_cytoband_url="https://bam.ccr.buffalo.edu/hg38/cytoBandIdeo.txt"
            )

            for choice in open('./igv/management/commands/data/empiregen_ids.txt', 'r'):
                file_path = "/EMPIREGEN/%s.bam" % (
                    choice.strip())
                a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                                           bam_server='https://bam.ccr.buffalo.edu',
                                                           sample_id=choice.strip(),
                                                           file_path=file_path)
