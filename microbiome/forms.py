from django import forms
from django.forms import ModelForm

import core
from microbiome.models import DownloadRequest


class DownloadRequestForm(ModelForm):

    def __init__(self, user_obj, search_log_obj, *args, **kwargs):
       super().__init__(*args, **kwargs)

       self.fields['user'].initial = user_obj
       self.fields['search_log'].initial = search_log_obj

    class Meta:
        model = DownloadRequest
        fields = '__all__'
        widgets = {
            'user': forms.HiddenInput(attrs={'readonly': 'readonly'}),
            'search_log': forms.HiddenInput(attrs={'readonly': 'readonly'}),
            'status': forms.HiddenInput(attrs={'readonly': 'readonly', }),
        }
