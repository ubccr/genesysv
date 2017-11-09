from django.shortcuts import render, redirect
from .forms import PEDForm
from io import TextIOWrapper
from .utils import validate_ped
from search.models import Dataset
from .models import PedigreeAnalysisRequest, PedigreeTrio
import json
from django.views.generic import (
    ListView, DetailView, CreateView, UpdateView, DeleteView, FormView
    )


def pedigree_analysis_request_create(request):

    if request.method == "POST":
        form = PEDForm(request.user, request.POST, request.FILES)
        if form.is_valid():
            data = form.cleaned_data
            dataset = Dataset.objects.get(id=data['dataset'])
            pef_file = TextIOWrapper(request.FILES['ped_file'], encoding='utf-8')
            status, output = validate_ped(pef_file, dataset)

            if status is True:
                pedigree_analysis_request_obj = PedigreeAnalysisRequest.objects.create(upload_user=request.user,
                                                   dataset=dataset,
                                                   ped_json=json.dumps(output))

                for family_id, value in output.items():
                    mother_id, father_id, child_id = value
                    PedigreeTrio.objects.create(pedigree_analysis_request=pedigree_analysis_request_obj,
                                                family_id=family_id,
                                                mother_id=mother_id,
                                                father_id=father_id,
                                                child_id=child_id)

                return redirect('trio-status')
            else:
                form.add_error('ped_file', output)
                context={}
                context['form'] = form
                return render(request, 'trio/trio_home.html', context)



            # text_f = TextIOWrapper(request.FILES['file'], encoding='utf-8')
            # # status, output = read_trio_file(text_f)
        else:
            context = {}
            context['form'] = form
            return render(request, 'trio/trio_home.html', context)

    else:
        form = PEDForm(request.user)
        context = {}
        context['form'] = form
        return render(request, 'trio/trio_home.html', context)

class PedigreeAnalysisRequestList(ListView):
    context = {}
    context['pedigree_analysis_request'] = PedigreeAnalysisRequest.objects.all()
    return render(request, 'trio/pedigreeanalysisrequest_list.html', context)

class PedigreeAnalysisRequestDelete(DeleteView):
    model = PedigreeAnalysisRequest
    success_url = reverse_lazy('pedigree-analysis-request-list')
    template_name = 'trio/pedigreeanalysisrequest_confirm_delete.html'
