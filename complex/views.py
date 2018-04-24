from django.views.generic.base import TemplateView
from django.shortcuts import render


from core.models import Dataset
from core.views import AppHomeView, SearchView
from .utils import ComplexElasticsearchResponseParser

class ComplexHomeView(AppHomeView):
    template_name = "complex/home.html"


class ComplexSearchView(SearchView):
    elasticsearch_response_parser_class = ComplexElasticsearchResponseParser

