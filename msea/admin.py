from django.contrib import admin
from .models import *

class StudyAdmin(admin.ModelAdmin):
    list_display = ('short_name', 'display_name', 'es_index_name', 'es_host', 'es_port', 'is_public')

class DatasetAdmin(admin.ModelAdmin):
    list_display = ('short_name', 'display_name')


class ReferenceSequenceAdmin(admin.ModelAdmin):
    list_display = ('rs_id', 'gene',)


class VariantSetAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'short_name')


class GeneAdmin(admin.ModelAdmin):
    list_display = ('gene_name',)


admin.site.register(Study, StudyAdmin)
admin.site.register(Dataset, DatasetAdmin)
admin.site.register(ReferenceSequence, ReferenceSequenceAdmin)
admin.site.register(VariantSet, VariantSetAdmin)
admin.site.register(Gene, GeneAdmin)
