from django.contrib import admin
from core.models import (AppName,
                         AnalysisType,
                         AttributeField,
                         Dataset,
                         ESFilterType,
                         FilterField,
                         FormType,
                         Study,
                         WidgetType,
                         SearchLog)


@admin.register(AppName)
class AppNameAdmin(admin.ModelAdmin):
    list_display = ('name',)

@admin.register(AnalysisType)
class AnalysisTypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'app_name')

@admin.register(Study)
class StudyAdmin(admin.ModelAdmin):
    list_display = ('name',)

@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ('name',)
    filter_horizontal = ('analysis_type',)

@admin.register(AttributeField)
class AttributeFieldAdmin(admin.ModelAdmin):
    list_display = ('display_text',)


@admin.register(ESFilterType)
class ESFilterTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)


@admin.register(FormType)
class FormTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)


@admin.register(WidgetType)
class WidgetTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)

@admin.register(SearchLog)
class SearchLogAdmin(admin.ModelAdmin):
    list_display = ('pk', 'user', 'created', 'dataset', 'filters_used')
    list_filter = ('user', 'dataset',)
