from django.contrib import admin
from core.models import (AppName, AttributeField, Dataset, ESFilterType, FilterField, FormType, WidgetType)


@admin.register(AppName)
class AppNameAdmin(admin.ModelAdmin):
    list_display = ('name',)

@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ('name',)

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
