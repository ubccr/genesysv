import core
from complex.utils import ComplexElasticsearchResponseParser

class ComplexSearchView(core.views.BaseSearchView):
    elasticsearch_response_parser_class = ComplexElasticsearchResponseParser

