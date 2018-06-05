from datetime import datetime
import pprint
import json

from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.http import QueryDict
from django.shortcuts import get_object_or_404, render
from django.views import View
from django.views.generic.base import TemplateView

from core.forms import (
    StudyForm,
    DatasetForm,
    AnalysisTypeForm,
    AttributeForm,
    AttributeFormPart,
    FilterForm,
    FilterFormPart,
    SaveSearchForm)
import core.models as core_models
from core.models import Dataset, Study
from core.utils import BaseSearchElasticsearch, get_values_from_es
from core.views import AppHomeView, BaseSearchView
from mendelian.forms import FamilyForm, KindredForm, MendelianAnalysisForm
from mendelian.utils import (MendelianElasticSearchQueryExecutor,
                             MendelianElasticsearchResponseParser,
                             MendelianSearchElasticsearch)

import complex.views as complex_views


class MendelianHomeView(AppHomeView):
    template_name = "mendelian/home.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        study_obj = get_object_or_404(Study, pk=self.kwargs.get('study_id'))
        context['study_obj'] = get_object_or_404(
            Study, pk=self.kwargs.get('study_id'))
        context['dataset_form'] = DatasetForm(study_obj, self.request.user)
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
    additional_information = {}

    def validate_additional_forms(self, request):
        family_ids = get_values_from_es(self.dataset_obj.es_index_name,
                                        self.dataset_obj.es_type_name,
                                        self.dataset_obj.es_host,
                                        self.dataset_obj.es_port,
                                        'Family_ID',
                                        'sample')
        number_of_families = len(family_ids)

        kindred_form = KindredForm(number_of_families, request.POST)
        if kindred_form.is_valid():
            if kindred_form.cleaned_data['number_of_kindred'].strip():
                self.additional_information = {'number_of_kindred': kindred_form.cleaned_data['number_of_kindred']}
        else:
            raise ValidationError('Invalid Kindred form!')


    def get_kwargs(self, request):
        kwargs = super().get_kwargs(request)
        if self.additional_information:
            kwargs.update(self.additional_information)
        kwargs.update({'mendelian_analysis_type': self.analysis_type_obj.name})
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
        search_log_id = search_elasticsearch_obj.get_search_log_id()
        filters_used = search_elasticsearch_obj.get_filters_used()
        attributes_selected = search_elasticsearch_obj.get_attributes_selected()

        if request.user.is_authenticated:
            save_search_form = SaveSearchForm(request.user,
                                              self.dataset_obj,
                                              self.analysis_type_obj,
                                              json.dumps(self.additional_information),
                                              filters_used,
                                              attributes_selected)
        else:
            save_search_form = None

        if self.call_get_context and request.user.is_authenticated:
            kwargs.update({'user_obj': request.user})
            kwargs.update({'search_log_obj': SearchLog.objects.get(id=search_log_id)})
            context = self.get_context_data(**kwargs)
        else:
            context = {}

        context['header'] = header
        context['results'] = results
        context['total_time'] = int((datetime.now() - self.start_time).total_seconds() * 1000)
        context['elasticsearch_response_time'] = elasticsearch_response_time
        context['search_log_id'] = search_log_id
        context['save_search_form'] = save_search_form
        context['app_name'] = self.analysis_type_obj.app_name.name
        return render(request, self.template_name, context)


class MendelianDocumentView(complex_views.ComplexDocumentView):
    pass
