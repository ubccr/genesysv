from django import forms
from django.contrib import admin

from core.models import *


@admin.register(AppName)
class AppNameAdmin(admin.ModelAdmin):
    list_display = ('name',)


@admin.register(AnalysisType)
class AnalysisTypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'app_name')


@admin.register(ESFilterType)
class ESFilterTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)


@admin.register(FormType)
class FormTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)


@admin.register(WidgetType)
class WidgetTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)


@admin.register(Study)
class StudyAdmin(admin.ModelAdmin):
    list_display = ('name',)


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ('name',)
    filter_horizontal = ('analysis_type',)


@admin.register(FilterTab)
class FilterTabAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset')


class FilterPanelForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        super(FilterPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['filter_fields'].queryset = FilterField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                               dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True

    class Meta:
        model = FilterPanel
        fields = '__all__'


@admin.register(FilterPanel)
class FilterPanelAdmin(admin.ModelAdmin):
    form = FilterPanelForm
    list_display = ('name', 'dataset')
    list_filter = ('dataset',)
    search_fields = ('name',)


class FilterSubPanelForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        super(FilterSubPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['filter_fields'].queryset = FilterField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                               dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True

    class Meta:
        model = FilterSubPanel
        fields = '__all__'


@admin.register(FilterSubPanel)
class FilterSubPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'filter_panel')
    search_fields = ('name',)
    list_filter = ('dataset',)
    form = FilterSubPanelForm


@admin.register(FilterField)
class FilterFieldAdmin(admin.ModelAdmin):
    list_display = ('display_text', 'dataset', 'in_line_tooltip', 'tooltip', 'default_value', 'form_type', 'widget_type',
                    'es_name', 'path', 'es_data_type', 'es_filter_type', 'place_in_panel', 'is_visible', )
    list_filter = ('dataset',)
    search_fields = ('display_text',)


@admin.register(FilterFieldChoice)
class FilterFieldChoiceAdmin(admin.ModelAdmin):
    list_display = ('filter_field', 'dataset', 'value',)
    list_filter = ('filter_field__dataset',)
    search_fields = ('filter_field__display_text', 'value')
    raw_id_fields = ('filter_field',)

    def dataset(self, obj):
        return obj.filter_field.dataset


@admin.register(AttributeTab)
class AttributeTabAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset')
    list_filter = ('attribute_panels',)


class AttributePanelForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        super(AttributePanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['attribute_fields'].queryset = AttributeField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                                     dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True

    class Meta:
        model = AttributePanel
        fields = '__all__'


@admin.register(AttributePanel)
class AttributePanelAdmin(admin.ModelAdmin):
    form = AttributePanelForm
    list_display = ('name',)
    search_fields = ('name',)
    list_filter = ('dataset',)


class AttributeSubPanelForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        super(AttributeSubPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['attribute_fields'].queryset = AttributeField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                                     dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True

    class Meta:
        model = AttributeSubPanel
        fields = '__all__'


@admin.register(AttributeSubPanel)
class AttributeSubPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'attribute_panel')
    search_fields = ('name',)
    list_filter = ('dataset',)
    form = AttributeSubPanelForm


@admin.register(AttributeField)
class AttributeFieldAdmin(admin.ModelAdmin):
    list_display = ('display_text',)


@admin.register(SearchLog)
class SearchLogAdmin(admin.ModelAdmin):
    list_display = ('pk', 'user', 'created', 'dataset', 'filters_used')
    list_filter = ('user', 'dataset',)

@admin.register(SearchOptions)
class SearchOptionsAdmin(admin.ModelAdmin):
    list_display = ('dataset', 'es_terminate',
                    'es_terminate_size_per_shard', 'maximum_table_size')


@admin.register(SavedSearch)
class SavedSearchAdmin(admin.ModelAdmin):
    list_display = ('pk', 'user', 'dataset', 'filters_used',
                    'attributes_selected', 'description')
