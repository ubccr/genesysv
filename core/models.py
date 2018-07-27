import json

from django.contrib.auth.models import Group, User
from django.db import models
from sortedm2m.fields import SortedManyToManyField

from common.models import TimeStampedModel
from core.utils import flush_memcache


class AppName(TimeStampedModel):
    name = models.CharField(max_length=64)

    def __str__(self):
        return self.name


class AnalysisType(TimeStampedModel):
    name = models.CharField(max_length=64)
    app_name = models.ForeignKey(
        'AppName',
        on_delete=models.CASCADE,
    )

    def __str__(self):
        return self.name


class ESFilterType(TimeStampedModel):
    name = models.CharField(max_length=64)

    def __str__(self):
        return self.name


class FormType(TimeStampedModel):
    name = models.CharField(max_length=64)

    def __str__(self):
        return self.name


class WidgetType(TimeStampedModel):
    name = models.CharField(max_length=64)

    def __str__(self):
        return self.name


class Study(TimeStampedModel):
    name = models.CharField(max_length=255)
    description = models.CharField(max_length=255, blank=True)

    class Meta:
        unique_together = ('name', 'description',)
        verbose_name_plural = 'studies'

    def __str__(self):
        return self.name


class Dataset(TimeStampedModel):
    study = models.ForeignKey(
        'Study',
        on_delete=models.CASCADE,
    )
    analysis_type = models.ManyToManyField(AnalysisType, blank=True)
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

    class Meta:
        verbose_name_plural = 'Search Options'


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
    path = models.CharField(max_length=255, blank=True)
    es_data_type = models.CharField(max_length=255)
    es_text_analyzer = models.CharField(max_length=255, blank=True)
    es_filter_type = models.ForeignKey(
        'ESFilterType',
        on_delete=models.CASCADE,
    )
    # Did not make place_in_panel a foreign key because that would lead to a circular
    # relationship. But I still need the panel name to get the admin interface to filter
    # correctly.
    place_in_panel = models.CharField(max_length=255)

    is_visible = models.BooleanField(default=True)

    class Meta:
        unique_together = ('dataset', 'es_name',
                           'es_filter_type', 'form_type', 'widget_type')

    def save(self, *args, **kwargs):
        flush_memcache()
        super(FilterField, self).save(*args, **kwargs)

    def __str__(self):
        return "%s %s" % (self.display_text, self.in_line_tooltip)


class FilterFieldChoice(TimeStampedModel):
    filter_field = models.ForeignKey(
        'FilterField',
        on_delete=models.CASCADE,
    )
    value = models.CharField(max_length=255)

    class Meta:
        unique_together = ('filter_field', 'value',)

    def save(self, *args, **kwargs):
        flush_memcache()
        super(FilterFieldChoice, self).save(*args, **kwargs)

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
    is_link_field = models.BooleanField(default=False)

    class Meta:
        unique_together = ('dataset', 'es_name', 'path')

    def save(self, *args, **kwargs):
        flush_memcache()
        super(AttributeField, self).save(*args, **kwargs)

    def __str__(self):
        return "%s" % (self.display_text)


class FilterPanel(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    are_sub_panels_mutually_exclusive = models.BooleanField(default=False)
    filter_fields = SortedManyToManyField(FilterField, blank=True)
    is_visible = models.BooleanField(default=True)

    def save(self, *args, **kwargs):
        flush_memcache()
        super(FilterPanel, self).save(*args, **kwargs)

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

    def save(self, *args, **kwargs):
        flush_memcache()
        super(FilterSubPanel, self).save(*args, **kwargs)

    def __str__(self):
        return self.name


class FilterTab(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    filter_panels = SortedManyToManyField(FilterPanel, blank=True)

    def save(self, *args, **kwargs):
        flush_memcache()
        super(FilterTab, self).save(*args, **kwargs)

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

    def save(self, *args, **kwargs):
        flush_memcache()
        super(AttributePanel, self).save(*args, **kwargs)

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

    def save(self, *args, **kwargs):
        flush_memcache()
        super(AttributeSubPanel, self).save(*args, **kwargs)

    def __str__(self):
        return self.name


class AttributeTab(TimeStampedModel):
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=255)
    attribute_panels = SortedManyToManyField(AttributePanel, blank=True)

    def save(self, *args, **kwargs):
        flush_memcache()
        super(AttributeTab, self).save(*args, **kwargs)

    def __str__(self):
        return self.name


REVIEW_STATUS_CHOICES = (
    ('Approved', 'Approved'),
    ('Rejected', 'Rejected'),
    ('Delete', 'Delete Status'),
)


class DocumentReview(TimeStampedModel):
    group = models.ForeignKey(
        Group,
        on_delete=models.CASCADE,
    )
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    document_es_id = models.CharField(max_length=64)
    description = models.CharField(max_length=64, blank=True, null=True)
    status = models.CharField(max_length=16, choices=REVIEW_STATUS_CHOICES)

    def __str__(self):
        return self.document_es_id


class DocumentReviewHistory(TimeStampedModel):
    document_review_status = models.ForeignKey(
        'DocumentReview',
        on_delete=models.CASCADE,
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
    )
    status = models.CharField(max_length=16, choices=REVIEW_STATUS_CHOICES)

    class Meta:
        verbose_name_plural = 'Document review status history'


class SearchLog(TimeStampedModel):
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        null=True, blank=True
    )
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    analysis_type = models.ForeignKey(
        'AnalysisType',
        on_delete=models.CASCADE,
    )
    header = models.TextField()
    query = models.TextField()
    nested_attribute_fields = models.TextField(null=True, blank=True)
    non_nested_attribute_fields = models.TextField(null=True, blank=True)
    filters_used = models.TextField(null=True, blank=True)
    attributes_selected = models.TextField()
    nested_attributes_selected = models.TextField(null=True, blank=True)
    non_nested_attributes_selected = models.TextField()
    additional_information = models.TextField(null=True, blank=True)
    exclude_rejected_documents = models.BooleanField(default=False)

    def __str__(self):
        return self.query


class SavedSearch(TimeStampedModel):
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
    )
    dataset = models.ForeignKey(
        'Dataset',
        on_delete=models.CASCADE,
    )
    analysis_type = models.ForeignKey(
        'AnalysisType',
        on_delete=models.CASCADE,
    )
    additional_information = models.TextField(null=True, blank=True)
    filters_used = models.TextField(null=True, blank=True)
    attributes_selected = models.TextField()
    description = models.TextField()

    @property
    def get_filters_used(self):
        if self.filters_used.strip():
            filters_used = json.loads(self.filters_used)
            output = []
            for key, val in filters_used.items():
                filter_field_obj = FilterField.objects.get(id=key)
                output.append((key, filter_field_obj.display_text, val))
            return output
        else:
            return None

    @property
    def get_attributes_selected(self):
        if self.attributes_selected.strip():
            attributes_selected = eval(self.attributes_selected)
            return attributes_selected
        else:
            return None

    @property
    def get_additional_information(self):
        if self.additional_information.strip():
            additional_information = json.loads(self.additional_information)
            output = []
            for key, val in additional_information.items():
                if val:
                    output.append((key, 'filter_field_obj.display_text', val))
            return output
        else:
            return None

    class Meta:
        verbose_name_plural = 'Saved Searches'
