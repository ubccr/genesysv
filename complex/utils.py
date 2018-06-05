import itertools

from core.utils import BaseElasticsearchResponseParser


class ComplexElasticsearchResponseParser(BaseElasticsearchResponseParser):
    fields_to_skip_flattening = ['FILTER', 'QUAL', ]
