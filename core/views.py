from collections import deque
from datetime import datetime
import json
import csv

from pprint import pprint

from django.views.generic.edit import CreateView
from django.views.generic.detail import DetailView
from django.views.generic.base import TemplateView
from django.views import View
from django.http import JsonResponse
from django.http import StreamingHttpResponse, HttpResponseForbidden
from django.utils.html import mark_safe
from django.shortcuts import render
from django.shortcuts import get_object_or_404
from django.core.cache import cache
from django.http import HttpResponse
from django.http import QueryDict
from django.core.exceptions import ValidationError

from common.utils import Echo
from core.models import Dataset, AnalysisType, AttributeTab, FilterTab, Study, SearchLog
from core.apps import CoreConfig
from core.forms import StudyForm, DatasetForm, AnalysisTypeForm, AttributeForm, AttributeFormPart, FilterForm, FilterFormPart
from core.utils import (
    BaseElasticSearchQueryDSL,
    BaseElasticSearchQueryExecutor,
    BaseElasticsearchResponseParser,
    BaseSearchElasticsearch,
    BaseDownloadAllResults,
)

class MainPageView(TemplateView):
    template_name = "core/home.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        study_form = StudyForm(self.request.user)
        context['study_form'] = study_form
        return context


class AppHomeView(TemplateView):
    template_name = "{}/home.html".format(CoreConfig.name)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        study_obj = get_object_or_404(Study, pk=self.kwargs.get('study_id'))
        context['study_obj'] = get_object_or_404(
            Study, pk=self.kwargs.get('study_id'))
        context['form'] = DatasetForm(study_obj, self.request.user)
        return context


class DatasetSnippetView(View):
    form_class = DatasetForm
    template_name = "core/dataset_form_template.html"

    def get(self, request, *args, **kwargs):
        study_obj = get_object_or_404(
            Study, pk=self.kwargs.get('study_id'))
        dataset_form = self.form_class(study_obj, self.request.user)
        context = {}
        context['form'] = dataset_form
        return render(request, self.template_name, context)


class AnalysisTypeSnippetView(View):
    form_class = AnalysisTypeForm
    template_name = "core/analysis_type_form_template.html"

    def get(self, request, *args, **kwargs):
        dataset_obj = get_object_or_404(
            Dataset, pk=self.kwargs.get('dataset_id'))
        analysis_type_form = self.form_class(dataset_obj, self.request.user)
        context = {}
        context['form'] = analysis_type_form
        return render(request, self.template_name, context)


class FilterSnippetView(View):
    form_class = FilterFormPart
    template_name = "core/filter_form_template.html"

    def generate_filter_form_tabs(self, dataset_obj):
        filter_form_tabs = deque()
        for tab in FilterTab.objects.filter(dataset=dataset_obj):
            tmp_dict = {}
            tmp_dict['name'] = tab.name
            tmp_dict['panels'] = deque()
            for panel in tab.filter_panels.filter(is_visible=True):
                es_form = self.form_class(panel.filter_fields.filter(is_visible=True).select_related(
                    'widget_type', 'form_type', 'es_filter_type'), prefix='filter_')
                sub_panels = deque()
                for sub_panel in panel.filtersubpanel_set.filter(is_visible=True):
                    if panel.are_sub_panels_mutually_exclusive:
                        MEgroup = "MEgroup_%d_%d" % (panel.id, sub_panel.id)
                    else:
                        MEgroup = None
                    tmp_sub_panel_dict = {}
                    tmp_sub_panel_dict['display_name'] = sub_panel.name
                    tmp_sub_panel_dict['name'] = ''.join(
                        sub_panel.name.split()).lower()
                    tmp_sub_panel_dict['form'] = self.form_class(sub_panel.filter_fields.filter(
                        is_visible=True).select_related('widget_type', 'form_type', 'es_filter_type'), MEgroup, prefix='filter_')
                    sub_panels.append(tmp_sub_panel_dict)

                tmp_dict['panels'].append({'display_name': panel.name,
                                           'name': ''.join(panel.name.split()).lower(),
                                           'form': es_form,
                                           'sub_panels': sub_panels})
            filter_form_tabs.append(tmp_dict)
        return filter_form_tabs

    def get_filter_form_tabs_response(self, request, dataset_obj):
        cache_name = 'filter_form_tabs_for_{}'.format(dataset_obj.id)
        cached_form = cache.get(cache_name)
        if cached_form:
            return cached_form
        else:
            filter_form_tabs = self.generate_filter_form_tabs(dataset_obj)
            context = {}
            context['tabs'] = filter_form_tabs
            response = render(
                request, self.template_name, context)
            cache.set(cache_name, response, None)
            return response

    def get(self, request, *args, **kwargs):
        dataset_obj = get_object_or_404(
            Dataset, pk=self.kwargs.get('dataset_id'))
        filter_form_tabs_response = self.get_filter_form_tabs_response(
            request, dataset_obj)
        return filter_form_tabs_response


class AttributeSnippetView(View):
    form_class = AttributeFormPart
    template_name = "core/attribute_form_template.html"

    def generate_attribute_form_tabs(self, dataset_obj):
        attribute_form_tabs = deque()
        for tab in AttributeTab.objects.filter(dataset=dataset_obj):
            tmp_dict = {}
            tmp_dict['name'] = tab.name
            tmp_dict['panels'] = deque()
            for idx_panel, panel in enumerate(tab.attribute_panels.filter(is_visible=True), start=1):
                if panel.attribute_fields.filter(is_visible=True):
                    es_form = self.form_class(panel.attribute_fields.filter(
                        is_visible=True), prefix='%d___attribute_group' % (idx_panel))
                else:
                    es_form = None

                sub_panels = deque()
                for idx_sub_panel, sub_panel in enumerate(panel.attributesubpanel_set.filter(is_visible=True), start=1):
                    tmp_sub_panel_dict = {}
                    tmp_sub_panel_dict['display_name'] = sub_panel.name
                    tmp_sub_panel_dict['name'] = ''.join(
                        sub_panel.name.split()).lower()
                    if sub_panel.attribute_fields.filter(is_visible=True):
                        tmp_sub_panel_dict['form'] = self.form_class(sub_panel.attribute_fields.filter(
                            is_visible=True), prefix='%d_%d___attribute_group' % (idx_panel, idx_sub_panel))
                    else:
                        tmp_sub_panel_dict['form'] = None
                    tmp_sub_panel_dict['attribute_group_id'] = '%d_%d___attribute_group' % (
                        idx_panel, idx_sub_panel)

                    sub_panels.append(tmp_sub_panel_dict)

                tmp_dict['panels'].append({'display_name': panel.name,
                                           'name': ''.join(panel.name.split()).lower(),
                                           'form': es_form,
                                           'attribute_group_id': '%d___attribute_group' % (idx_panel),
                                           'sub_panels': sub_panels})

            attribute_form_tabs.append(tmp_dict)
        return attribute_form_tabs

    def get_attribute_form_tabs_response(self, request, dataset_obj):
        cache_name = 'attribute_form_tabs_for_{}'.format(dataset_obj.id)
        cached_form = cache.get(cache_name)
        if cached_form:
            return cached_form
        else:
            attribute_form_tabs = self.generate_attribute_form_tabs(
                dataset_obj)
            context = {}
            context['tabs'] = attribute_form_tabs
            response = render(
                request, self.template_name, context)
            cache.set(cache_name, response, None)
            return response

    def get(self, request, *args, **kwargs):
        dataset_obj = get_object_or_404(
            Dataset, pk=self.kwargs.get('dataset_id'))
        attribute_form_tabs_response = self.get_attribute_form_tabs_response(
            request, dataset_obj)
        return attribute_form_tabs_response


class SearchRouterView(View):

    def post(self, request):
        POST_data = QueryDict(request.POST['form_data'])
        dataset_id = POST_data.get('dataset')
        dataset_obj = get_object_or_404(Dataset, pk=dataset_id)
        analysis_type_id = POST_data.get('analysis_type')
        analysis_type_obj = get_object_or_404(AnalysisType, pk=analysis_type_id)
        app_name = analysis_type_obj.app_name.name

        if app_name == 'microbiome':
            from microbiome.views import MicrobiomeSearchView
            return_view = MicrobiomeSearchView
        elif app_name == 'complex':
            from complex.views import ComplexSearchView
            return_view = ComplexSearchView
        elif app_name == 'mendelian':
            from mendelian.views import MendelianSearchView
            return_view = MendelianSearchView
        else:
            return_view = BaseSearchView

        return return_view().post(request)

class AdditionalFormRouterView(View):

    def get(self, request, *args, **kwargs):
        analysis_type_id = kwargs.get('analysis_type_id')
        analysis_type_obj = get_object_or_404(AnalysisType, pk=analysis_type_id)
        dataset_id = kwargs.get('dataset_id')

        if analysis_type_obj.name in ['autosomal_dominant', 'autosomal_recessive', 'compound_heterozygous', 'denovo']:
            from mendelian.views import KindredSnippetView
            return KindredSnippetView().get(request, dataset_id=dataset_id)
        else:
            return HttpResponse("")


class BaseSearchView(TemplateView):
    template_name = "core/search_results_template.html"
    call_get_context = False
    start_time = None
    study_obj = None
    dataset_obj = None
    filter_form_data = None
    attribute_form_data = None
    attribute_order = None
    elasticsearch_dsl_class = BaseElasticSearchQueryDSL
    elasticsearch_query_executor_class = BaseElasticSearchQueryExecutor
    elasticsearch_response_parser_class = BaseElasticsearchResponseParser
    search_elasticsearch_class = BaseSearchElasticsearch

    def validate_request_data(self, request):

        # Get all FORM POST Data
        POST_data = QueryDict(request.POST['form_data'])

        # Get Study Object
        study_form = StudyForm(request.user, POST_data)
        if study_form.is_valid():
            study_id = study_form.cleaned_data['study']
        else:
            raise ValidationError('Invalid study!')
        self.study_obj = get_object_or_404(Study, pk=study_id)

        # Get Attribute Order
        try:
            self.attribute_order = json.loads(request.POST['attribute_order'])
        except:
            raise ValidationError('Invalid attribute!')

        # Validate Dataset
        dataset_form = DatasetForm(self.study_obj, request.user, POST_data)
        if dataset_form.is_valid():
            dataset_id = dataset_form.cleaned_data['dataset']
        else:
            raise ValidationError('Invalid dataset!')
        self.dataset_obj = Dataset.objects.prefetch_related(
            'attributefield_set').filter(id=dataset_id)[0]

        # Validate Filter Form
        filter_form = FilterForm(self.dataset_obj, POST_data, prefix='filter_')
        if filter_form.is_valid():
            self.filter_form_data = filter_form.cleaned_data
        else:
            raise ValidationError('Invalid Filter Form!')

        # Validate Attribute Form
        attribute_form = AttributeForm(
            self.dataset_obj, POST_data, prefix='attribute_group')
        if attribute_form.is_valid():
            self.attribute_form_data = attribute_form.cleaned_data
        else:
            raise ValidationError('Invalid Attribute Form!')

    def post(self, request, *args, **kwargs):
        self.start_time = datetime.now()

        self.validate_request_data(request)

        kwargs = {
            'user': request.user,
            'dataset_obj': self.dataset_obj,
            'filter_form_data': self.filter_form_data,
            'attribute_form_data': self.attribute_form_data,
            'attribute_order': self.attribute_order,
            'elasticsearch_dsl_class': self.elasticsearch_dsl_class,
            'elasticsearch_query_executor_class': self.elasticsearch_query_executor_class,
            'elasticsearch_response_parser_class': self.elasticsearch_response_parser_class,


        }
        search_elasticsearch_obj = self.search_elasticsearch_class(**kwargs)
        search_elasticsearch_obj.search()
        header = search_elasticsearch_obj.get_header()
        results = search_elasticsearch_obj.get_results()
        elasticsearch_response_time = search_elasticsearch_obj.get_elasticsearch_response_time()
        search_log_id = search_elasticsearch_obj.get_search_log_id()

        if self.call_get_context and request.user.is_authenticated:
            kwargs.update({'user_obj': request.user})
            kwargs.update({'search_log_obj': SearchLog.objects.get(id=search_log_id)})
            context = self.get_context_data(**kwargs)
        else:
            context = {}

        context['header'] = header
        context['results'] = results
        context['elasticsearch_response_time'] = elasticsearch_response_time
        context['search_log_id'] = search_log_id

        context['total_time'] = int(
            (datetime.now() - self.start_time).total_seconds() * 1000)

        return render(request, self.template_name, context)



class BaseDownloadView(View):

    def get(self, request, *args, **kwargs):
        search_log_obj = get_object_or_404(
            SearchLog, pk=self.kwargs.get('search_log_id'))


        if request.user != search_log_obj.user:
            return HttpResponseForbidden()


        download_obj = BaseDownloadAllResults(search_log_obj)
        rows = download_obj.yield_rows()
        # rows = (["Row {}".format(idx), str(idx)] for idx in range(65536))
        pseudo_buffer = Echo()
        writer = csv.writer(pseudo_buffer)
        response = StreamingHttpResponse((writer.writerow(row) for row in rows),
                                         content_type="text/csv")
        response['Content-Disposition'] = 'attachment; filename="search_results.csv"'

        return response
