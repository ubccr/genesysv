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
    filter_horizontal = ('analysis_type', 'allowed_groups')


@admin.register(FilterTab)
class FilterTabAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset')


class FilterPanelForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        super(FilterPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['filter_fields'].queryset = FilterField.objects.filter(place_in_panel=kwargs['instance'].name, dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True
        else:
            self.fields['filter_fields'].queryset = FilterField.objects.none()

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
            self.fields['filter_panel'].queryset = FilterPanel.objects.filter(dataset=kwargs['instance'].dataset)
            self.fields['filter_fields'].queryset = FilterField.objects.filter(place_in_panel=kwargs['instance'].name, dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True
        else:
            self.fields['filter_panel'].queryset = FilterPanel.objects.none()
            self.fields['filter_fields'].queryset = FilterField.objects.none()

    class Meta:
        model = FilterSubPanel
        fields = '__all__'


@admin.register(FilterSubPanel)
class FilterSubPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'filter_panel')
    search_fields = ('name',)
    list_filter = ('dataset',)
    form = FilterSubPanelForm



class FilterFieldForm(forms.ModelForm):
    place_in_panel = forms.ChoiceField(widget=forms.Select())
    es_data_type = forms.ChoiceField(widget=forms.Select())


    ES_DATA_TYPE_CHOICES = (
        ('float', 'float'),
        ('integer', 'integer'),
        ('keyword', 'keyword'),
        ('text', 'text')
    )
    def __init__(self, *args, **kwargs):
        super(FilterFieldForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            dataset = kwargs['instance'].dataset
            PANEL_CHOICES = [(ele, ele) for ele in sorted(list(dataset.filterpanel_set.all().values_list('name', flat=True)) + list(dataset.filtersubpanel_set.all().values_list('name', flat=True)))]
        else:
            PANEL_CHOICES = [(ele, ele) for ele in sorted(list(set(list(FilterPanel.objects.all().values_list('name', flat=True)) + list(FilterSubPanel.objects.all().values_list('name', flat=True)))))]


        self.fields['es_data_type'].choices = self.ES_DATA_TYPE_CHOICES
        self.fields['place_in_panel'].choices = PANEL_CHOICES

    class Meta:
        model = FilterField
        fields = '__all__'



@admin.register(FilterField)
class FilterFieldAdmin(admin.ModelAdmin):
    form = FilterFieldForm
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
            self.fields['attribute_fields'].queryset = AttributeField.objects.filter(place_in_panel=kwargs['instance'].name, dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True
        else:
            self.fields['attribute_fields'].queryset = AttributeField.objects.none()

    class Meta:
        model = AttributePanel
        fields = '__all__'


@admin.register(AttributePanel)
class AttributePanelAdmin(admin.ModelAdmin):
    form = AttributePanelForm
    list_display = ('dataset', 'name',)
    search_fields = ('name',)
    list_filter = ('dataset',)


class AttributeSubPanelForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        super(AttributeSubPanelForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            self.fields['attribute_panel'].queryset = AttributePanel.objects.filter(dataset=kwargs['instance'].dataset)
            self.fields['attribute_fields'].queryset = AttributeField.objects.filter(
                place_in_panel=kwargs['instance'].name, dataset=kwargs['instance'].dataset)
            self.fields['dataset'].disabled = True
        else:
            self.fields['attribute_panel'].queryset = AttributePanel.objects.none()
            self.fields['attribute_fields'].queryset = AttributeField.objects.none()

    class Meta:
        model = AttributeSubPanel
        fields = '__all__'


@admin.register(AttributeSubPanel)
class AttributeSubPanelAdmin(admin.ModelAdmin):
    list_display = ('dataset', 'name', 'attribute_panel')
    search_fields = ('name',)
    list_filter = ('dataset',)
    form = AttributeSubPanelForm


class AttributeFieldForm(forms.ModelForm):
    place_in_panel = forms.ChoiceField(widget=forms.Select())

    def __init__(self, *args, **kwargs):
        super(AttributeFieldForm, self).__init__(*args, **kwargs)
        if kwargs.get('instance'):
            dataset = kwargs['instance'].dataset
            PANEL_CHOICES = [(ele, ele) for ele in sorted(list(dataset.attributepanel_set.all().values_list('name', flat=True)) + list(dataset.attributesubpanel_set.all().values_list('name', flat=True)))]
        else:
            PANEL_CHOICES = [(ele, ele) for ele in sorted(list(set(list(AttributePanel.objects.all().values_list('name', flat=True)) + list(AttributeSubPanel.objects.all().values_list('name', flat=True)))))]



        self.fields['place_in_panel'].choices = PANEL_CHOICES

    class Meta:
        model = AttributeField
        fields = '__all__'


@admin.register(AttributeField)
class AttributeFieldAdmin(admin.ModelAdmin):
    list_display = ('display_text', 'dataset', 'es_name', 'path', 'place_in_panel', 'is_visible')
    list_filter = ('dataset',)
    search_fields = ('display_text',)
    form = AttributeFieldForm


@admin.register(SearchLog)
class SearchLogAdmin(admin.ModelAdmin):
    list_display = ('pk', 'user', 'created', 'dataset', 'analysis_type', 'filters_used', 'non_nested_attribute_fields', 'nested_attribute_fields', )
    list_filter = ('user', 'dataset',)



@admin.register(SearchOptions)
class SearchOptionsAdmin(admin.ModelAdmin):
    list_display = ('dataset', 'es_terminate',
                    'es_terminate_size_per_shard', 'maximum_table_size')


@admin.register(SavedSearch)
class SavedSearchAdmin(admin.ModelAdmin):
    list_display = ('pk', 'user', 'dataset', 'analysis_type', 'additional_information', 'filters_used',
                    'attributes_selected', 'description')



@admin.register(DocumentReview)
class DocumentReviewAdmin(admin.ModelAdmin):
    pass
