from django.contrib import admin
from .models import InheritanceAnalysisRequest, FamilyPedigree

# Register your models here.
@admin.register(InheritanceAnalysisRequest)
class InheritanceAnalysisRequestAdmin(admin.ModelAdmin):
    list_display = ('pk', 'upload_user', 'dataset', 'ped_json', 'analysis_status')


@admin.register(FamilyPedigree)
class FamilyPedigreeAdmin(admin.ModelAdmin):
    list_display = ('pk', 'inheritance_analysis_request', 'family_id', 'mother_id', 'father_id', 'child_id')

