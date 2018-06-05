import core
from django.shortcuts import get_object_or_404
from complex.utils import ComplexElasticsearchResponseParser


class ComplexSearchView(core.views.BaseSearchView):
    elasticsearch_response_parser_class = ComplexElasticsearchResponseParser

class ComplexDocumentView(core.views.BaseDocumentView):
    template_name = "complex/variant.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        dataset_obj = get_object_or_404(core.models.Dataset, pk=self.kwargs.get('dataset_id'))
        variant_id = self.kwargs.get('variant_id')
        return context


