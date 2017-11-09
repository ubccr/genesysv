from django.contrib import admin
from .models import PedigreeInformation, PedigreeTrio

# Register your models here.
@admin.register(PedigreeInformation)
class PedigreeInformationAdmin(admin.ModelAdmin):
    list_display = ('pk', 'upload_user', 'dataset', 'ped_json', 'analysis_status')


@admin.register(PedigreeTrio)
class PedigreeTrioAdmin(admin.ModelAdmin):
    list_display = ('pk', 'pedigree_information', 'family_id', 'mother_id', 'father_id', 'child_id')

