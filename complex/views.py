from django.shortcuts import get_object_or_404

import core
from complex.utils import ComplexElasticsearchResponseParser


class ComplexSearchView(core.views.BaseSearchView):
    elasticsearch_response_parser_class = ComplexElasticsearchResponseParser

class ComplexDocumentView(core.views.BaseDocumentView):
    template_name = "complex/variant.html"
