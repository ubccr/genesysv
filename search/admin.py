from django.contrib import admin
from .models import *



class FilterTabAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset')

class FilterPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'filter_tab')
    filter_horizontal = ('filter_fields',)

class FilterSubPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'filter_panel')
    filter_horizontal = ('filter_fields',)

class AttributeTabAdmin(admin.ModelAdmin):
    list_display = ('name', 'dataset')

class AttributePanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'attribute_tab')
    filter_horizontal = ('attribute_fields',)

class AttributeSubPanelAdmin(admin.ModelAdmin):
    list_display = ('name', 'attribute_panel')
    filter_horizontal = ('attribute_fields',)

class FilterFieldAdmin(admin.ModelAdmin):
    list_display = ('display_name', 'dataset', 'tooltip', 'form_type', 'widget_type', 'es_name', 'es_filter_type', 'path')

class FilterFieldChoiceAdmin(admin.ModelAdmin):
    list_display = ('filter_field', 'value',)

class AttributeFieldAdmin(admin.ModelAdmin):
    list_display = ('display_name',)

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

