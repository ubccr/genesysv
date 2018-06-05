from django.core.management.base import BaseCommand

from search.models import *

FORM_TYPES = ("CharField", "ChoiceField", "MultipleChoiceField")

WIDGET_TYPES = ("TextInput", "Select", "SelectMultiple", "Textarea", "UploadField")

ES_FILTER_TYPES = ( "filter_term",
                    "filter_terms",
                    "nested_filter_term",
                    "nested_filter_terms",
                    "filter_range_gte",
                    "filter_range_gt",
                    "filter_range_lte",
                    "filter_range_lt",
                    "nested_filter_range_gte",
                    "nested_filter_range_lte",
                    "filter_exists",
                    "must_not_exists",
                    "nested_filter_exists",
)


class Command(BaseCommand):

    def handle(self, *args, **options):

        for name in FORM_TYPES:
            FormType.objects.get_or_create(name=name)

        for name in WIDGET_TYPES:
            WidgetType.objects.get_or_create(name=name)

        for name in ES_FILTER_TYPES:
            ESFilterType.objects.get_or_create(name=name)
