from django.views.generic.base import TemplateView
from django.shortcuts import render
from django.shortcuts import get_object_or_404
from django.views import View
from django.http import QueryDict
from django.core.cache import cache
from django.core.exceptions import ValidationError

from datetime import datetime

from core.utils import get_values_from_es


from core.models import Dataset, Study
from core.views import AppHomeView, BaseSearchView
from core.forms import DatasetForm
from core.utils import BaseSearchElasticsearch
from mendelian.forms import FamilyForm, MendelianAnalysisForm
from mendelian.utils import MendelianSearchElasticsearch, MendelianElasticSearchQueryExecutor, MendelianElasticsearchResponseParser

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
            Dataset, pk=self.kwargs.get('dataset_id'))
        family_form_response = self.get_family_form_response(
            request, dataset_obj)
        return family_form_response

class MendelianSearchView(BaseSearchView):
    search_elasticsearch_class = MendelianSearchElasticsearch
    elasticsearch_query_executor_class = MendelianElasticSearchQueryExecutor
    elasticsearch_response_parser_class = MendelianElasticsearchResponseParser
    father_id = None
    mother_id = None
    child_id = None
    mendelian_analysis_type = None

    def validate_mendelian_forms(self, request):
        # Validate Family Form
        sample_ids = get_values_from_es(self.dataset_obj.es_index_name,
                       self.dataset_obj.es_type_name,
                       self.dataset_obj.es_host,
                       self.dataset_obj.es_port,
                       'sample_ID',
                       'sample')

        POST_data = QueryDict(request.POST['form_data'])
        family_form = FamilyForm(sample_ids, POST_data)

        if family_form.is_valid():
            self.father_id = family_form.cleaned_data['father_id']
            self.mother_id = family_form.cleaned_data['mother_id']
            self.child_id = family_form.cleaned_data['child_id']
        else:
            raise ValidationError('Invalid family form!')


        mendelian_analysis_form = MendelianAnalysisForm(POST_data)

        if mendelian_analysis_form.is_valid():
            self.mendelian_analysis_type = mendelian_analysis_form.cleaned_data['analysis_type']
        else:
            raise ValidationError('Invalid Mendelian Analysis!')


    def post(self, request, *args, **kwargs):
        self.start_time = datetime.now()

        self.validate_request_data(request)
        self.validate_mendelian_forms(request)

        kwargs = {
            'dataset_obj': self.dataset_obj,
            'filter_form_data': self.filter_form_data,
            'attribute_form_data': self.attribute_form_data,
            'attribute_order': self.attribute_order,
            'elasticsearch_dsl_class': self.elasticsearch_dsl_class,
            'elasticsearch_query_executor_class': self.elasticsearch_query_executor_class,
            'elasticsearch_response_parser_class': self.elasticsearch_response_parser_class,
            'father_id': self.father_id,
            'mother_id': self.mother_id,
            'child_id': self.child_id,
            'mendelian_analysis_type': self.mendelian_analysis_type,
        }

        search_elasticsearch_obj = self.search_elasticsearch_class(**kwargs)
        search_elasticsearch_obj.search()
        header = search_elasticsearch_obj.get_header()
        results = search_elasticsearch_obj.get_results()
        elasticsearch_response_time = search_elasticsearch_obj.get_elasticsearch_response_time()

        context = {}
        context['header'] = header
        context['results'] = results
        context['total_time'] = int((datetime.now()-self.start_time).total_seconds() * 1000)
        context['elasticsearch_response_time'] = elasticsearch_response_time

        return render(request, self.template_name, context)
