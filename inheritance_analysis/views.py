import json
from io import TextIOWrapper

from django.db import IntegrityError
from django.shortcuts import redirect, render, reverse
from django.views.generic import (CreateView, DeleteView, DetailView, FormView,
                                  ListView, UpdateView)

from .forms import PEDFileUploadForm
from .models import *
from .utils import (get_datasets_associated_with_user,
                    get_user_ids_associated_with_group, validate_ped)


class InheritanceAnalysisRequestListView(ListView):
    model = InheritanceAnalysisRequest
    template_name = "inheritance_analysis/inheritance_analysis_request_list.html"
    context_object_name = 'inheritance_analysis_requests'

    def get_context_data(self, **kwargs):
        context = super(InheritanceAnalysisRequestListView,
                        self).get_context_data(**kwargs)
        form = PEDFileUploadForm(self.request.user)
        context['form'] = form
        return context

    def get_queryset(self):
        dataset_ids = get_datasets_associated_with_user(self.request.user)

        return InheritanceAnalysisRequest.objects.filter(dataset__in=dataset_ids)


class InheritanceAnalysisRequestDelete(DeleteView):
    model = InheritanceAnalysisRequest
    template_name = "inheritance_analysis/inheritance_analysis_request_delete.html"
    context_object_name = 'inheritance_analysis_request'

    def get_success_url(self):
        return reverse('inheritance-analysis-request-list')


def inheritance_analysis_request_create(request):

    if request.method == "POST":
        form = PEDFileUploadForm(request.user, request.POST, request.FILES)

        if form.is_valid():
            data = form.cleaned_data
            dataset = Dataset.objects.get(id=data['dataset'])
            ped_file = TextIOWrapper(
                request.FILES['ped_file'], encoding='utf-8')
            status, output = validate_ped(ped_file, dataset)

            dataset_ids = get_datasets_associated_with_user(request.user)

            if status is True:
                try:
                    inheritance_analysis_request_obj = InheritanceAnalysisRequest.objects.create(upload_user=request.user,
                                                                                                 dataset=dataset,
                                                                                                 ped_json=json.dumps(output))
                except IntegrityError as e:
                    form.add_error(
                        'dataset', 'A request already exists for this dataset!')
                    context = {}
                    context['form'] = form
                    inheritance_analysis_requests = InheritanceAnalysisRequest.objects.filter(
                        dataset__in=dataset_ids)
                    context[
                        'inheritance_analysis_requests'] = inheritance_analysis_requests
                    return render(request, 'inheritance_analysis/inheritance_analysis_request_list.html', context)

                for family_id, value in output.items():
                    mother_id, father_id, child_id = value
                    FamilyPedigree.objects.create(inheritance_analysis_request=inheritance_analysis_request_obj,
                                                  family_id=family_id,
                                                  mother_id=mother_id,
                                                  father_id=father_id,
                                                  child_id=child_id)

                return redirect('inheritance-analysis-request-list')
            else:
                form.add_error('ped_file', output)
                context = {}
                context['form'] = form
                inheritance_analysis_requests = InheritanceAnalysisRequest.objects.filter(
                    dataset__in=dataset_ids)
                context[
                    'inheritance_analysis_requests'] = inheritance_analysis_requests
                return render(request, 'inheritance_analysis/inheritance_analysis_request_list.html', context)
