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
                         SearchLog,
                         FilterPanel)

from django import forms

class FilterPanelForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        super(FilterPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['filter_fields'].queryset = FilterField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                               dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True
            print(self.fields['dataset'].widget)

    class Meta:
        model = FilterPanel
        fields = '__all__'

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

@admin.register(FilterField)
class FilterFieldAdmin(admin.ModelAdmin):
    list_display = ('display_text', 'dataset', 'in_line_tooltip', 'tooltip', 'default_value', 'form_type', 'widget_type',
                    'es_name', 'path', 'es_data_type', 'es_filter_type', 'place_in_panel', 'is_visible', )
    list_filter = ('dataset',)
    search_fields = ('display_text',)


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


@admin.register(FilterPanel)
class FilterPanelAdmin(admin.ModelAdmin):
    form = FilterPanelForm
    list_display = ('name', 'dataset')
    list_filter = ('dataset',)
    search_fields = ('name',)
