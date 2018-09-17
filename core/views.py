import csv
import json
from collections import deque
from datetime import datetime
from pprint import pprint

from django.contrib.auth.models import Group
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.http import (HttpResponse, HttpResponseForbidden,
                         HttpResponseServerError, JsonResponse, QueryDict,
                         StreamingHttpResponse)
from django.shortcuts import get_object_or_404, redirect, render
from django.views import View
from django.views.generic.base import TemplateView
from django.views.generic.list import ListView
from django.views.generic.edit import FormView


from common.utils import Echo
from core.apps import CoreConfig
from core.forms import (AnalysisTypeForm, AttributeForm, AttributeFormPart,
                        DatasetForm, FilterForm, FilterFormPart,
                        SaveSearchForm, StudyForm, DocumentReviewForm)
from core.models import (AnalysisType, AttributeTab, Dataset, FilterTab,
                         SavedSearch, SearchLog, Study, DocumentReview)
from core.utils import (BaseDownloadAllResults, BaseElasticSearchQueryDSL,
                        BaseElasticSearchQueryExecutor,
                        BaseElasticsearchResponseParser,
                        BaseSearchElasticsearch, get_es_document, get_user_group_for_reviewing)
from django.urls import reverse
from django.contrib import messages
from django.http import HttpResponseRedirect


class MainPageView(TemplateView):
    template_name = "core/home.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        study_form = StudyForm(self.request.user)
        context['study_form'] = study_form
        context['load_search'] = 'false'
        context['information_json'] = 'false'
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


class DownloadRouterView(View):

    def get(self, request, *args, **kwargs):
        search_log_id = kwargs.get('search_log_id')
        search_log_obj = get_object_or_404(SearchLog, pk=search_log_id)
        app_name = search_log_obj.analysis_type.app_name.name

        if app_name == 'complex':
            return_view = BaseDownloadView
        elif app_name == 'mendelian':
            from mendelian.views import MendelianDownloadView
            return_view = MendelianDownloadView

        return return_view().get(request, **{'search_log_id': search_log_id})


class AdditionalFormRouterView(View):

    def get(self, request, *args, **kwargs):
        analysis_type_id = kwargs.get('analysis_type_id')
        analysis_type_obj = get_object_or_404(AnalysisType, pk=analysis_type_id)
        dataset_id = kwargs.get('dataset_id')

        if analysis_type_obj.name in ['autosomal_dominant', 'autosomal_recessive', 'denovo']:
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
    analysis_type_obj = None
    filter_form_data = None
    attribute_form_data = None
    attribute_order = None
    elasticsearch_dsl_class = BaseElasticSearchQueryDSL
    elasticsearch_query_executor_class = BaseElasticSearchQueryExecutor
    elasticsearch_response_parser_class = BaseElasticsearchResponseParser
    search_elasticsearch_class = BaseSearchElasticsearch
    exclude_rejected_documents = None

    def validate_request_data(self, request, POST_data):

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
        except KeyError:
            raise ValidationError('Invalid attribute!')

        # Validate Dataset
        dataset_form = DatasetForm(self.study_obj, request.user, POST_data)
        if dataset_form.is_valid():
            dataset_id = dataset_form.cleaned_data['dataset']
        else:
            raise ValidationError('Invalid dataset!')

        self.dataset_obj = Dataset.objects.prefetch_related(
            'attributefield_set').get(id=dataset_id)

        # Validate Analysis Type Form
        analysis_type_form = AnalysisTypeForm(self.dataset_obj, request.user, POST_data)
        if analysis_type_form.is_valid():
            analysis_type_id = analysis_type_form.cleaned_data['analysis_type']
        else:
            raise ValidationError('Invalid analysis type!')

        self.analysis_type_obj = AnalysisType.objects.get(id=analysis_type_id)

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

        self.exclude_rejected_documents = POST_data.get('exclude_rejected_documents')

    def get_kwargs(self, request):

        kwargs = {
            'user': request.user,
            'dataset_obj': self.dataset_obj,
            'analysis_type_obj': self.analysis_type_obj,
            'filter_form_data': self.filter_form_data,
            'attribute_form_data': self.attribute_form_data,
            'attribute_order': self.attribute_order,
            'elasticsearch_dsl_class': self.elasticsearch_dsl_class,
            'elasticsearch_query_executor_class': self.elasticsearch_query_executor_class,
            'elasticsearch_response_parser_class': self.elasticsearch_response_parser_class,
            'exclude_rejected_documents': self.exclude_rejected_documents,

        }

        return kwargs

    def post(self, request, *args, **kwargs):
        self.start_time = datetime.now()

        # Get all FORM POST Data
        POST_data = QueryDict(request.POST['form_data'])

        self.validate_request_data(request, POST_data)

        kwargs = self.get_kwargs(request)
        search_elasticsearch_obj = self.search_elasticsearch_class(**kwargs)
        search_elasticsearch_obj.search()
        header = search_elasticsearch_obj.get_header()
        results = search_elasticsearch_obj.get_results()
        elasticsearch_response_time = search_elasticsearch_obj.get_elasticsearch_response_time()
        search_log_id = search_elasticsearch_obj.get_search_log_id()
        filters_used = search_elasticsearch_obj.get_filters_used()
        attributes_selected = search_elasticsearch_obj.get_attributes_selected()

        try:
            keys = results[0].keys()
        except:
            keys = []

        if 'SYMBOL' in keys:
            genes = []
            for result in results:
                symbol = result.get('SYMBOL')
                if not symbol or symbol == 'NA':
                    continue
                genes.append(result.get('SYMBOL'))
            genes = sorted(list(set(genes)))
            if genes:
                gene_mania_link = '<a target="_blank" href="http://genemania.org/#/search/9606/%s"><i class="fa fa-external-link-square fa-1x" aria-hidden="true"></i> GeneMANIA</a>' %('|'.join(genes))
            else:
                gene_mania_link = None
        else:
            gene_mania_link = None


        if request.user.is_authenticated:
            save_search_form = SaveSearchForm(request.user,
                                              self.dataset_obj,
                                              self.analysis_type_obj,
                                              '',
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

        if self.exclude_rejected_documents == 'true':
            exclude_rejected_documents_checkbox_status = "checked"
        else:
            exclude_rejected_documents_checkbox_status = ""

        context['exclude_rejected_documents_checkbox_status'] = exclude_rejected_documents_checkbox_status
        context['gene_mania_link'] = gene_mania_link
        context['header'] = header
        context['results'] = results
        context['total_time'] = int((datetime.now() - self.start_time).total_seconds() * 1000)
        context['elasticsearch_response_time'] = elasticsearch_response_time
        context['search_log_id'] = search_log_id
        context['save_search_form'] = save_search_form
        context['app_name'] = self.analysis_type_obj.app_name.name

        return render(request, self.template_name, context)


class BaseDownloadView(View):

    def get(self, request, *args, **kwargs):
        search_log_obj = get_object_or_404(
            SearchLog, pk=kwargs.get('search_log_id'))
        if search_log_obj.user != None and request.user != search_log_obj.user:
            return HttpResponseForbidden()
        download_obj = BaseDownloadAllResults(search_log_obj)
        rows = download_obj.yield_rows()
        pseudo_buffer = Echo()
        writer = csv.writer(pseudo_buffer)
        response = StreamingHttpResponse((writer.writerow(row) for row in rows),
                                         content_type="text/csv")
        response['Content-Disposition'] = 'attachment; filename="search_results.csv"'

        return response


def save_search(request):
    if request.method == 'POST':
        try:
            dataset_obj = Dataset.objects.get(id=request.POST.get('dataset'))
            analysis_type_obj = AnalysisType.objects.get(id=request.POST.get('analysis_type'))
            additional_information = request.POST.get('additional_information')
            filters_used = request.POST.get('filters_used')
            attributes_selected = request.POST.get('attributes_selected')
            form = SaveSearchForm(request.user, dataset_obj, analysis_type_obj, additional_information,
                                  filters_used, attributes_selected, request.POST)
            if form.is_valid():
                data = form.cleaned_data
                user = data.get('user')
                dataset = data.get('dataset')
                analysis_type = data.get('analysis_type')
                additional_information = data.get('additional_information')
                filters_used = data.get('filters_used')
                attributes_selected = data.get('attributes_selected')
                description = data.get('description')
                SavedSearch.objects.create(user=user,
                                           dataset=dataset,
                                           analysis_type=analysis_type,
                                           additional_information=additional_information,
                                           filters_used=filters_used,
                                           attributes_selected=attributes_selected,
                                           description=description)
                return redirect('saved-search-list')
            else:
                print('form invalid')
                print(form.errors)
        except Exception as e:
            print(e)

        else:
            return HttpResponseServerError()


class SavedSearchListView(ListView):

    model = SavedSearch
    context_object_name = 'saved_search_list'

    def get_queryset(self, **kwargs):
        return SavedSearch.objects.filter(user=self.request.user)


class RetrieveSavedSearchView(TemplateView):
    template_name = "core/home.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        saved_search_obj = get_object_or_404(SavedSearch, pk=self.kwargs.get('saved_search_id'))
        information = {}
        information['study'] = saved_search_obj.dataset.study.id
        information['dataset'] = saved_search_obj.dataset.id
        information['analysis_type'] = saved_search_obj.analysis_type.id
        information['additional_information'] = saved_search_obj.additional_information
        information['filters_used'] = saved_search_obj.get_filters_used
        information['attributes_selected'] = saved_search_obj.get_attributes_selected

        information_json = json.dumps(information, cls=DjangoJSONEncoder)

        context = {}
        study_form = StudyForm(self.request.user)
        context['study_form'] = study_form
        context['information_json'] = information_json
        context['load_search'] = 'true'

        return context


def document_view_help(result, dataset_obj):
    panels_with_values = []
    for panel in dataset_obj.attributetab_set.first().attribute_panels.all():
        for attribute in panel.attribute_fields.all():
            if result.get(attribute.es_name):
                panels_with_values.append(panel.name)
        for sub_panel in panel.attributesubpanel_set.all():
            for subpanel_attribute in sub_panel.attribute_fields.all():
                if result.get(subpanel_attribute.es_name):
                    panels_with_values.append(sub_panel.name)

    return list(set(panels_with_values))

class BaseDocumentView(TemplateView):
    template_name = "core/document.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_es_id = self.kwargs.get('document_es_id')
        group_obj, error_message = get_user_group_for_reviewing(dataset_obj, self.request.user)

        if error_message:
            # messages.error(self.request, error_message)
            document_review = None
        else:
            if DocumentReview.objects.filter(document_es_id=document_es_id, group=group_obj).exists():
                document_review = DocumentReview.objects.get(document_es_id=document_es_id, group=group_obj)
            else:
                document_review = None

        result = get_es_document(dataset_obj, document_es_id)
        fields_to_skip = ['Variant', 'CHROM', 'POS', 'REF', 'ALT', 'VariantType', 'FILTER', 'QUAL', 'ID', 'sample']

        panels_with_values = document_view_help(result, dataset_obj)
        context['document_review'] = document_review
        context['document_es_id'] = document_es_id
        context['dataset_id'] = dataset_obj.id
        context['dataset_obj'] = dataset_obj
        context['result'] = result
        context['fields_to_skip'] = fields_to_skip
        context['panels_with_values'] = panels_with_values
        return context


class DocumentReviewCreateView(FormView):
    template_name = 'core/document_review_create.html'
    form_class = DocumentReviewForm

    def dispatch(self, request, *args, **kwargs):
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_es_id = self.kwargs.get('document_es_id')
        group_obj, error_message = get_user_group_for_reviewing(dataset_obj, request.user)

        if not error_message:
            return super().dispatch(request, *args, **kwargs)
        else:
            messages.error(request, error_message)
            return HttpResponseRedirect(reverse('core-document-list'))

    def get_success_url(self):
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_es_id = self.kwargs.get('document_es_id')
        return reverse('complex-document-view', kwargs={'dataset_id': dataset_obj.id, 'document_es_id': document_es_id})

    def form_valid(self, form):
        data = form.cleaned_data
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_es_id = self.kwargs.get('document_es_id')
        group_obj, error_message = get_user_group_for_reviewing(dataset_obj, self.request.user)

        # if DocumentReview.objects.filter(document_es_id=document_es_id, group=group_obj).exists():
        #     document_review_obj = DocumentReview.objects.get(document_es_id=document_es_id, group=group_obj)
        #     if data.get('status') == document_review_obj.status:
        #         return super().form_valid(form)
        #     if data.get('status') == 'Delete':
        #         document_review_obj.delete()
        #         return super().form_valid(form)
        #     if data.get('status') != document_review_obj.status:
        #         document_review_obj.status = data.get('status')
        #         document_review_obj.save()
        #         return super().form_valid(form)
        # else:
        result = get_es_document(dataset_obj, document_es_id)
        document_review_obj = DocumentReview.objects.create(
            group=group_obj,
            dataset=dataset_obj, document_es_id=document_es_id, status=data.get('status'), description=result.get('Variant'))
        return super().form_valid(form)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_es_id = self.kwargs.get('document_es_id')
        result = get_es_document(dataset_obj, document_es_id)
        context['result'] = result
        context['document_es_id'] = document_es_id
        context['dataset_id'] = dataset_obj.id
        return context


class DocumentReviewUpdateView(FormView):
    template_name = 'core/document_review_update.html'
    form_class = DocumentReviewForm

    def dispatch(self, request, *args, **kwargs):
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_review_id = self.kwargs.get('document_review_id')
        document_review_obj = DocumentReview.objects.get(id=document_review_id)
        group_obj, error_message = get_user_group_for_reviewing(dataset_obj, request.user)

        if not error_message:
            return super().dispatch(request, *args, **kwargs)
        else:
            messages.error(request, error_message)
            return HttpResponseRedirect(reverse('complex-document-view', kwargs={'dataset_id': dataset_obj.id, 'document_es_id': document_review_obj.document_es_id}))

    def get_success_url(self):
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_review_id = self.kwargs.get('document_review_id')
        try:
            document_review_obj = DocumentReview.objects.get(id=document_review_id)
            return reverse('complex-document-view', kwargs={'dataset_id': dataset_obj.id, 'document_es_id': document_review_obj.document_es_id})
        except:
            return reverse('core-document-list')

    def form_valid(self, form):
        data = form.cleaned_data
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_review_id = self.kwargs.get('document_review_id')
        group_obj, error_message = get_user_group_for_reviewing(dataset_obj, self.request.user)
        document_review_obj = DocumentReview.objects.get(id=document_review_id, group=group_obj)
        if data.get('status') == document_review_obj.status:
            return super().form_valid(form)
        if data.get('status') == 'Delete':
            document_review_obj.delete()
            return super().form_valid(form)
        if data.get('status') != document_review_obj.status:
            document_review_obj.status = data.get('status')
            document_review_obj.save()
            return super().form_valid(form)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        dataset_obj = get_object_or_404(Dataset, pk=self.kwargs.get('dataset_id'))
        document_review_id = self.kwargs.get('document_review_id')
        document_review_obj = DocumentReview.objects.get(id=document_review_id)
        result = get_es_document(dataset_obj, document_review_obj.document_es_id)
        context['result'] = result
        context['document_review_id'] = document_review_id
        context['document_es_id'] = document_review_obj.document_es_id
        context['dataset_id'] = dataset_obj.id
        return context


class DocumentReviewListView(ListView):
    model = DocumentReview
    context_object_name = 'document_review_list'
    template_name = 'core/document_review_list.html'

    def get_queryset(self):
        groups = self.request.user.groups.all()
        return DocumentReview.objects.filter(group__in=groups)
