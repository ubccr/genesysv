from core.utils import BaseElasticsearchResponseParser
import itertools


class ComplexElasticsearchResponseParser(BaseElasticsearchResponseParser):
    fields_to_skip_flattening = ['FILTER', 'QUAL', ]

