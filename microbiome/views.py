from django.views import View
from django.shortcuts import redirect
from django.http import HttpResponse, HttpResponseServerError
from django.http import HttpResponseForbidden
from django.http import StreamingHttpResponse
from django.shortcuts import get_object_or_404
from django.http import QueryDict
from django.views.generic.list import ListView

import csv
import core
from common.utils import Echo

from .forms import DownloadRequestForm
from .utils import DownloadAllResultsAsOTUTable
from .models import DownloadRequest

class MicrobiomeSearchView(core.views.BaseSearchView):
    template_name = "microbiome/search_results_template.html"
    call_get_context = True

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['download_request_form'] = DownloadRequestForm(kwargs.get('user_obj'), kwargs.get('search_log_obj') )
        return context


class OtuDownloadView(View):

    def get(self, request, *args, **kwargs):
        search_log_obj = get_object_or_404(
            core.models.SearchLog, pk=self.kwargs.get('search_log_id'))

        if request.user != search_log_obj.user:
            return HttpResponseForbidden()

        download_otu_table = DownloadAllResultsAsOTUTable(search_log_obj)
        download_otu_table.execute_query()
        download_otu_table.format_otu_table()
        rows = download_otu_table.yield_rows()

        pseudo_buffer = Echo()
        writer = csv.writer(pseudo_buffer)
        response = StreamingHttpResponse((writer.writerow(row) for row in rows),
                                         content_type="text/csv")
        response['Content-Disposition'] = 'attachment; filename="otu_table.csv"'

        return response

class DownloadRequestListView(ListView):
    model = DownloadRequest
    template_name = 'microbiome/download_request_list.html'
    context_object_name = 'download_request_list'

    def get_queryset(self):
        if self.request.user.has_perm('microbiome.can_view_all_download_requests'):
            return DownloadRequest.objects.all()
        else:
            return DownloadRequest.objects.filter(user=self.request.user)

class DownloadRequestReviewListView(ListView):
    model = DownloadRequest
    template_name = 'microbiome/download_request_review_list.html'
    context_object_name = 'download_request_list'

    def get_queryset(self):

        return DownloadRequest.objects.filter(status='Pending')

def approve_download_request(request, download_request_id):
    if request.method == 'GET':

        if not request.user.has_perm('microbiome.can_approve_download_request'):
            return HttpResponseForbidden()

        download_request_obj = get_object_or_404(
                DownloadRequest, pk=download_request_id)

        download_request_obj.status = 'Approved'
        download_request_obj.save()
        print(download_request_obj.status)

        return redirect('download-request-review-list')

def deny_download_request(request, download_request_id):
    if request.method == 'GET':

        if not request.user.has_perm('microbiome.can_approve_download_request'):
            return HttpResponseForbidden()

        download_request_obj = get_object_or_404(
                DownloadRequest, pk=download_request_id)

        download_request_obj.status = 'Denied'
        download_request_obj.save()

        return redirect('download-request-review-list')

def request_download(request, search_log_id):

    if request.method == 'POST':
        user_obj = request.user
        search_log_obj = get_object_or_404(
            core.models.SearchLog, pk=search_log_id)

        POST_data = QueryDict(request.POST['form_data'])
        form = DownloadRequestForm(user_obj, search_log_obj, POST_data)
        if form.is_valid():
            print('Form is valid')
            data = form.cleaned_data
            user = data.get('user')
            search_log = data.get('search_log')
            pi = data.get('pi')
            reason = data.get('reason')
            contact_email = data.get('contact_email')

            DownloadRequest.objects.create(
                user=user,
                search_log=search_log,
                pi=pi,
                reason=reason,
                contact_email=contact_email
            )

            return HttpResponse('Success')
        else:
            return HttpResponseServerError()
