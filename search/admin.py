from django import forms
from django.contrib import admin
from .models import *



class FilterTabAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset')


class FilterPanelForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(FilterPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            print(kwargs.get('instance'))
            self.fields['filter_fields'].queryset = FilterField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                               dataset=kwargs['instance'].dataset)

    class Meta:
        model = FilterPanel
        exclude = ('dataset',)

class FilterPanelAdmin(admin.ModelAdmin):
    form = FilterPanelForm
    list_display = ('name',)
    search_fields = ('name',)

class FilterSubPanelForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(FilterSubPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            print(kwargs.get('instance'))
            self.fields['filter_fields'].queryset = FilterField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                               dataset=kwargs['instance'].dataset)

    class Meta:
        model = FilterSubPanel
        exclude = ('dataset',)


class FilterSubPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'filter_panel')
    search_fields = ('name',)
    form = FilterSubPanelForm

class AttributeTabAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset')
    list_filter = ('attribute_panels',)


class AttributePanelForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(AttributePanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['attribute_fields'].queryset = AttributeField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                                     dataset=kwargs['instance'].dataset)

    class Meta:
        model = AttributePanel
        exclude = ('dataset',)


class AttributePanelAdmin(admin.ModelAdmin):
    form = AttributePanelForm
    list_display = ('name',)
    search_fields = ('name',)

class AttributeSubPanelForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(AttributeSubPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['attribute_fields'].queryset = AttributeField.objects.filter(place_in_panel=kwargs['instance'].name,
                                                                                     dataset=kwargs['instance'].dataset)

    class Meta:
        model = AttributeSubPanel
        exclude = ('dataset',)


class AttributeSubPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'attribute_panel')
    search_fields = ('name',)
    form = AttributeSubPanelForm

class FilterFieldAdmin(admin.ModelAdmin):
    list_display = ('display_text', 'dataset', 'in_line_tooltip', 'tooltip', 'form_type', 'widget_type', 'es_name', 'path', 'es_data_type', 'es_filter_type', )
    list_filter = ('dataset',)
    search_fields = ('display_text',)

class FilterFieldChoiceAdmin(admin.ModelAdmin):
    list_display = ('filter_field', 'value',)
    list_filter = ('filter_field__dataset',)
    search_fields = ('filter_field__display_text', 'value')
    raw_id_fields = ('filter_field',)

class AttributeFieldAdmin(admin.ModelAdmin):
    list_display = ('display_text', 'dataset', 'es_name', 'path',)
    list_filter = ('dataset',)
    search_fields = ('display_text',)

class FormTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)

class WidgetTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)

class ESFilterTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)

class StudyAdmin(admin.ModelAdmin):
    list_display = ('name',)

class DatasetAdmin(admin.ModelAdmin):
    list_display = ('name',)
    filter_horizontal = ('allowed_groups',)


class SearchLogAdmin(admin.ModelAdmin):
    list_display = ('pk', 'user', 'created', 'dataset', 'filters_used')
    list_filter = ('user', 'dataset',)

class SearchOptionsAdmin(admin.ModelAdmin):
    list_display = ('dataset', 'es_terminate', 'es_terminate_size_per_shard', 'maximum_table_size')


admin.site.register(FormType, FormTypeAdmin)
admin.site.register(WidgetType, WidgetTypeAdmin)
admin.site.register(ESFilterType, ESFilterTypeAdmin)
admin.site.register(Study, StudyAdmin)
admin.site.register(Dataset, DatasetAdmin)
admin.site.register(FilterTab, FilterTabAdmin)
admin.site.register(FilterPanel, FilterPanelAdmin)
admin.site.register(FilterSubPanel, FilterSubPanelAdmin)
admin.site.register(AttributeTab, AttributeTabAdmin)
admin.site.register(AttributePanel, AttributePanelAdmin)
admin.site.register(AttributeSubPanel, AttributeSubPanelAdmin)
admin.site.register(FilterField, FilterFieldAdmin)
admin.site.register(FilterFieldChoice, FilterFieldChoiceAdmin)
admin.site.register(AttributeField, AttributeFieldAdmin)
admin.site.register(SearchLog, SearchLogAdmin)
admin.site.register(SearchOptions, SearchOptionsAdmin)

