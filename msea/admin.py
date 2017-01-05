from django.contrib import admin
from .models import *

class MseaDatasetAdmin(admin.ModelAdmin):
    list_display = ('display_name', 'dataset', 'is_public')

class ReferenceSequenceAdmin(admin.ModelAdmin):
    list_display = ('rs_id', 'gene', 'msea_dataset')


class VariantSetAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'short_name')


admin.site.register(MseaDataset, MseaDatasetAdmin)
admin.site.register(ReferenceSequence, ReferenceSequenceAdmin)
admin.site.register(VariantSet, VariantSetAdmin)
