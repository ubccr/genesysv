from django.contrib import admin

from .models import AnnotationReference, SampleBamInfo


@admin.register(AnnotationReference)
class AnnotationReferenceAdmin(admin.ModelAdmin):
    list_display = ('genome_id', 'annotation_name', 'annotation_file_format', 'annotation_url',
                    'annotation_index_url', 'reference_fasta_url', 'reference_cytoband_url',)


@admin.register(SampleBamInfo)
class SampleBamInfoAdmin(admin.ModelAdmin):
    list_display = ('sample_id', 'bam_server', 'file_path', 'dataset',)
    list_filter = ('dataset',)
    search_fields = ('sample_id',)

    def __str__(self):
        return self.sample_id
