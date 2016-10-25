from django.db import models
from django.contrib.auth.models import Group
from common.models import TimeStampedModel


class FormType(TimeStampedModel):
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

class WidgetType(TimeStampedModel):
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

class ESFilterType(TimeStampedModel):
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

class Study(TimeStampedModel):
    name = models.CharField(max_length=255)
    description = models.CharField(max_length=255, blank=True)

    def __str__(self):
        return self.name

class Dataset(TimeStampedModel):
    study = models.ForeignKey(
        'Study',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    description = models.CharField(max_length=255)
    es_index_name = models.CharField(max_length=255)
    es_type_name = models.CharField(max_length=255)
    es_host = models.CharField(max_length=255)
    is_public = models.BooleanField(default=False)
    allowed_groups = models.ManyToManyField(Group, blank=True)

    def __str__(self):
        return self.name

class FilterField(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    display_name = models.CharField(max_length=255)
    in_line_tooltip = models.CharField(max_length=255, null=True, blank=True)
    tooltip = models.CharField(max_length=255, null=True, blank=True)
    default_value = models.CharField(max_length=255, null=True, blank=True)
    form_type = models.ForeignKey(
        'FormType',
        on_delete=models.CASCADE,
    )
    widget_type = models.ForeignKey(
        'WidgetType',
        on_delete=models.CASCADE,
    )
    es_name = models.CharField(max_length=255)
    es_filter_type = models.ForeignKey(
        'ESFilterType',
        on_delete=models.CASCADE,
    )
    path = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        unique_together = ('dataset', 'es_name', 'es_filter_type', 'path')

    def __str__(self):
        return self.display_name

class FilterFieldChoice(TimeStampedModel):
    filter_field = models.ForeignKey(
        'FilterField',
        on_delete=models.CASCADE,
    )
    value = models.CharField(max_length=255)

    class Meta:
        unique_together = ('filter_field', 'value',)

    def __str__(self):
        return self.value



class AttributeField(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    display_name = models.CharField(max_length=255)
    es_name = models.CharField(max_length=255)
    path = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        unique_together = ('dataset', 'es_name', 'display_name', 'path')

    def __str__(self):
        return self.display_name

class FilterTab(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

class FilterPanel(TimeStampedModel):
    filter_tab = models.ForeignKey(
        'FilterTab',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    are_sub_panels_mutually_exclusive = models.BooleanField(default=False)
    filter_fields = models.ManyToManyField(FilterField, blank=True)

    def __str__(self):
        return self.name

class FilterSubPanel(TimeStampedModel):
    filter_panel = models.ForeignKey(
        'FilterPanel',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    filter_fields = models.ManyToManyField(FilterField, blank=True)

    def __str__(self):
        return self.name


class AttributeTab(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

class AttributePanel(TimeStampedModel):
    attribute_tab = models.ForeignKey(
        'AttributeTab',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    are_sub_panels_mutually_exclusive = models.BooleanField(default=False)
    attribute_fields = models.ManyToManyField(AttributeField, blank=True)

    def __str__(self):
        return self.name

class AttributeSubPanel(TimeStampedModel):
    attribute_panel = models.ForeignKey(
        'AttributePanel',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    attribute_fields = models.ManyToManyField(AttributeField, blank=True)

    def __str__(self):
        return self.name


class SearchResultDownload(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    headers = models.TextField()
    query = models.TextField()
    nested_attribute_fields = models.TextField(null=True, blank=True)
    non_nested_attribute_fields = models.TextField(null=True, blank=True)
    dict_filter_fields = models.TextField(null=True, blank=True)
    used_keys = models.TextField(null=True, blank=True)
