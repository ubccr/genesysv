from datetime import datetime
import pprint

from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.http import QueryDict
from django.shortcuts import get_object_or_404, render
from django.views import View
from django.views.generic.base import TemplateView

import core.forms as core_forms
import core.models as core_models
from core.models import Dataset, Study
from core.utils import BaseSearchElasticsearch, get_values_from_es
from core.views import AppHomeView, BaseSearchView
from mendelian.forms import FamilyForm, KindredForm, MendelianAnalysisForm
from mendelian.utils import (MendelianElasticSearchQueryExecutor,
                             MendelianElasticsearchResponseParser,
                             MendelianSearchElasticsearch)


class MendelianHomeView(AppHomeView):
    template_name = "mendelian/home.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        study_obj = get_object_or_404(Study, pk=self.kwargs.get('study_id'))
        context['study_obj'] = get_object_or_404(
            Study, pk=self.kwargs.get('study_id'))
        context['dataset_form'] = core_forms.DatasetForm(study_obj, self.request.user)
        context['mendelian_analysis_form'] = MendelianAnalysisForm()
        return context


class KindredSnippetView(View):
    form_class = KindredForm
    template_name = "mendelian/kindred_form_template.html"

    def generate_kindred_form(self, dataset_obj):
        family_ids = get_values_from_es(dataset_obj.es_index_name,
                                        dataset_obj.es_type_name,
                                        dataset_obj.es_host,
                                        dataset_obj.es_port,
                                        'Family_ID',
                                        'sample')
        number_of_families = len(family_ids)
        kindred_form = self.form_class(number_of_families)

        return kindred_form

    def get_kindred_form_response(self, request, dataset_obj):
        cache_name = 'kindred_form_for_{}'.format(dataset_obj.id)
        cached_form = cache.get(cache_name)
        if cached_form:
            return cached_form
        else:
            kindred_form = self.generate_kindred_form(dataset_obj)
            context = {}
            context['kindred_form'] = kindred_form
            response = render(
                request, self.template_name, context)
            cache.set(cache_name, response, None)
            return response

    def get(self, request, *args, **kwargs):
        dataset_obj = get_object_or_404(
            Dataset, pk=kwargs.get('dataset_id'))
        kindred_form_response = self.get_kindred_form_response(
            request, dataset_obj)
        return kindred_form_response


class FamilySnippetView(View):
    form_class = FamilyForm
    template_name = "mendelian/family_form_template.html"

    def generate_family_form(self, dataset_obj):
        sample_ids = get_values_from_es(dataset_obj.es_index_name,
                                        dataset_obj.es_type_name,
                                        dataset_obj.es_host,
                                        dataset_obj.es_port,
                                        'sample_ID',
                                        'sample')
        family_form = self.form_class(sample_ids)

        return family_form

    def get_family_form_response(self, request, dataset_obj):
        cache_name = 'family_form_for_{}'.format(dataset_obj.id)
        cached_form = cache.get(cache_name)
        if cached_form:
            return cached_form
        else:
            family_form = self.generate_family_form(dataset_obj)
            context = {}
            context['family_form'] = family_form
            response = render(
                request, self.template_name, context)
            cache.set(cache_name, response, None)
            return response

    def get(self, request, *args, **kwargs):
        dataset_obj = get_object_or_404(
            Dataset, pk=kwargs.get('dataset_id'))
        family_form_response = self.get_family_form_response(
            request, dataset_obj)
        return family_form_response


class MendelianSearchView(BaseSearchView):
    search_elasticsearch_class = MendelianSearchElasticsearch
    elasticsearch_query_executor_class = MendelianElasticSearchQueryExecutor
    elasticsearch_response_parser_class = MendelianElasticsearchResponseParser
    mendelian_analysis_type = None
    number_of_kindred = None

    def validate_additional_forms(self, request):
        # Validate Family Form
        family_ids = get_values_from_es(self.dataset_obj.es_index_name,
                                        self.dataset_obj.es_type_name,
                                        self.dataset_obj.es_host,
                                        self.dataset_obj.es_port,
                                        'Family_ID',
                                        'sample')
        number_of_families = len(family_ids)

        POST_data = QueryDict(request.POST['form_data'])
        kindred_form = KindredForm(number_of_families, POST_data)
        if kindred_form.is_valid():
            self.number_of_kindred = kindred_form.cleaned_data['number_of_kindred']
        else:
            raise ValidationError('Invalid Kindred form!')

        analysis_type_form = core_forms.AnalysisTypeForm(self.dataset_obj, request.user, POST_data)

        if analysis_type_form.is_valid():
            analysis_type_id = analysis_type_form.cleaned_data['analysis_type']
            analysis_type_obj = get_object_or_404(core_models.AnalysisType, pk=analysis_type_id)
            self.mendelian_analysis_type = analysis_type_obj.name
        else:
            raise ValidationError('Invalid Mendelian Analysis!')

    def get_kwargs(self, request):
        kwargs = super().get_kwargs(request)
        kwargs.update({'mendelian_analysis_type': self.mendelian_analysis_type,
                       'number_of_kindred': self.number_of_kindred})

        return kwargs

    def post(self, request, *args, **kwargs):
        self.start_time = datetime.now()

        self.validate_request_data(request)
        self.validate_additional_forms(request)

        kwargs = self.get_kwargs(request)

        search_elasticsearch_obj = self.search_elasticsearch_class(**kwargs)
        search_elasticsearch_obj.search()
        header = search_elasticsearch_obj.get_header()
        results = search_elasticsearch_obj.get_results()
        elasticsearch_response_time = search_elasticsearch_obj.get_elasticsearch_response_time()

        context = {}
        context['header'] = header
        context['results'] = results
        context['total_time'] = int((datetime.now() - self.start_time).total_seconds() * 1000)
        context['elasticsearch_response_time'] = elasticsearch_response_time
        context['save_search_form'] = None

        return render(request, self.template_name, context)
