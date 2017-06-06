from django.db import models
from common.models import TimeStampedModel
from sortedm2m.fields import SortedManyToManyField

from django.contrib.auth.models import User, Group
import json

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

    class Meta:
        unique_together = ('name', 'description',)

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
    es_port = models.CharField(max_length=255)
    is_public = models.BooleanField(default=False)
    allowed_groups = models.ManyToManyField(Group, blank=True)

    class Meta:
        unique_together = ('name', 'description',)

    def __str__(self):
        return self.name

class SearchOptions(TimeStampedModel):
    dataset = models.OneToOneField(
        'Dataset',
        on_delete=models.CASCADE,
    )
    es_terminate = models.BooleanField(default=True)
    es_terminate_size_per_shard = models.IntegerField(default=80)
    maximum_table_size = models.IntegerField(default=400)


class SampleReadDepth(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    sample_id = models.CharField(max_length=255)
    rd_10x = models.FloatField()
    rd_15x = models.FloatField()
    rd_20x = models.FloatField()
    rd_40x = models.FloatField()
    rd_50x = models.FloatField()
    variant_count = models.IntegerField()


class FilterField(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    display_text = models.CharField(max_length=255)
    in_line_tooltip = models.CharField(max_length=255, blank=True)
    tooltip = models.CharField(max_length=255, blank=True)
    default_value = models.CharField(max_length=255, blank=True)
    form_type = models.ForeignKey(
        'FormType',
        on_delete=models.CASCADE,
    )
    widget_type = models.ForeignKey(
        'WidgetType',
        on_delete=models.CASCADE,
    )
    es_name = models.CharField(max_length=255)
    path = models.CharField(max_length=255, default=None)
    es_data_type = models.CharField(max_length=255)
    es_filter_type = models.ForeignKey(
        'ESFilterType',
        on_delete=models.CASCADE,
    )
    place_in_panel = models.CharField(max_length=255)
    is_visible = models.BooleanField(default=True)

    class Meta:
        unique_together = ('dataset', 'es_name', 'es_filter_type', 'form_type', 'widget_type')

    def __str__(self):
        return "%s %s" %(self.display_text, self.in_line_tooltip)

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
    display_text = models.CharField(max_length=255)
    es_name = models.CharField(max_length=255)
    path = models.CharField(max_length=255, null=True, blank=True)
    place_in_panel = models.CharField(max_length=255)
    is_visible = models.BooleanField(default=True)

    class Meta:
        unique_together = ('dataset', 'es_name', 'path')

    def __str__(self):
        return "%s" %(self.display_text)


class FilterPanel(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    are_sub_panels_mutually_exclusive = models.BooleanField(default=False)
    filter_fields = SortedManyToManyField(FilterField, blank=True)
    is_visible = models.BooleanField(default=True)

    def __str__(self):
        return self.name

class FilterSubPanel(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    filter_panel = models.ForeignKey(
        'FilterPanel',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    filter_fields = SortedManyToManyField(FilterField, blank=True)
    is_visible = models.BooleanField(default=True)

    def __str__(self):
        return self.name

class FilterTab(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    filter_panels = SortedManyToManyField(FilterPanel, blank=True)

    def __str__(self):
        return self.name



class AttributePanel(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    are_sub_panels_mutually_exclusive = models.BooleanField(default=False)
    attribute_fields = SortedManyToManyField(AttributeField, blank=True)
    is_visible = models.BooleanField(default=True)

    def __str__(self):
        return self.name

class AttributeSubPanel(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    attribute_panel = models.ForeignKey(
        'AttributePanel',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    attribute_fields = SortedManyToManyField(AttributeField, blank=True)
    is_visible = models.BooleanField(default=True)

    def __str__(self):
        return self.name

class AttributeTab(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    attribute_panels = SortedManyToManyField(AttributePanel, blank=True)

    def __str__(self):
        return self.name

class SearchLog(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        null=True, blank=True
    )
    headers = models.TextField()
    query = models.TextField()
    nested_attribute_fields = models.TextField(null=True, blank=True)
    non_nested_attribute_fields = models.TextField(null=True, blank=True)
    dict_filter_fields = models.TextField(null=True, blank=True)
    used_keys = models.TextField(null=True, blank=True)
    filters_used = models.TextField(null=True, blank=True)
    attributes_selected = models.TextField()



    def __str__(self):
        return self.query


class SavedSearch(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
    )
    filters_used = models.TextField(null=True, blank=True)
    attributes_selected = models.TextField()
    description = models.TextField()

    def _get_filters_used(self):
        if self.filters_used.strip():
            filters_used = json.loads(self.filters_used)
            output = []
            for key, val in filters_used.items():
                es_name, es_filter_type, path = key.split('-')
                print(es_name, es_filter_type, path)
                filter_field_obj = FilterField.objects.get(dataset=self.dataset,
                                                           es_name=es_name,
                                                           es_filter_type__name=es_filter_type)
                output.append((key, filter_field_obj.display_text, val))
            return output
        else:
            return None
    get_filters_used = property(_get_filters_used)

    def _get_attributes_selected(self):
        if self.attributes_selected.strip():
            attributes_selected = json.loads(self.attributes_selected)
            return attributes_selected
        else:
            return None
    get_attributes_selected = property(_get_attributes_selected)

    class Meta:
        unique_together = ('dataset', 'user', 'filters_used', 'attributes_selected')
