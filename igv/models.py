from django.db import models
from django.contrib.auth.models import Group
from common.models import TimeStampedModel


class AnnotationReference(TimeStampedModel):
    dataset = models.OneToOneField(
        'search.Dataset',
        on_delete=models.CASCADE,
        unique=True
    )
    genome_id = models.CharField(max_length=8)
    annotation_name = models.CharField(max_length=128)
    annotation_file_format = models.CharField(max_length=8)
    annotation_url = models.URLField()
    annotation_index_url = models.URLField()
    reference_fasta_url = models.URLField()
    reference_cytoband_url = models.URLField()

    class Meta:
        verbose_name_plural = 'Annotations and References'

    def __str__(self):
        return self.genome_id

class SampleBamInfo(TimeStampedModel):
    dataset = models.ForeignKey(
        'search.Dataset',
        on_delete=models.CASCADE,
    )
    bam_server = models.URLField()
    sample_id = models.CharField(max_length=64, unique=True)
    file_path = models.CharField(max_length=255)

    class Meta:
        verbose_name_plural = 'Sample BAM info'

    def __str__(self):
        return "%s: %s" % (self.sample_id, self.file_path)
