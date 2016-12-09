from django import forms
from django.contrib.auth.models import User
from search.models import Study, Dataset
from django.db.models import Q
from crispy_forms.helper import FormHelper
from .models import ReferenceSequence, MSEADataset
import re

class GeneForm(forms.Form):

    def __init__(self, user, *args, **kwargs):
        super(GeneForm, self).__init__(*args, **kwargs)
        try:
            user_group_ids = [group.id for group in user.groups.all()]
        except:
            user_group_ids = []
        msea_dataset = MSEADataset.objects.all()
        msea_dataset = msea_dataset.filter(Q(allowed_groups__in=user_group_ids) | Q(is_public=True)).distinct()
        DATASET_CHOICE = [(ele.dataset, ele.display_name) for ele in msea_dataset]

        EXPAND_OPTIONS = (('expand', 'Expanded (recurrent variants are used independently for analysis)'),
                       ('noexpand', 'Not Expanded (recurrent variants are collapsed into a single variant for analysis​)'),
                       )


        self.fields['dataset'] = forms.ChoiceField(choices=DATASET_CHOICE)
        self.fields['search_term'] = forms.CharField(help_text='Enter gene name or reference sequence ID', required=True)
        self.fields['expand_option'] = forms.ChoiceField(widget=forms.RadioSelect, choices=EXPAND_OPTIONS, required=True, initial='expand')

class VariantForm(forms.Form):
    def __init__(self, rs_id, *args, **kwargs):
        super(VariantForm, self).__init__(*args, **kwargs)

        m = re.search('\((.+)\)', rs_id)
        rs_id = m.group(1)
        rf_obj = ReferenceSequence.objects.get(rs_id=rs_id)
        VARIANT_CHOICES = [(ele.short_name, ele.full_name) for ele in rf_obj.variants.all()]
        print(VARIANT_CHOICES)
        self.fields['variant_choices'] = forms.MultipleChoiceField(choices=VARIANT_CHOICES, initial=[sn for sn, fl in VARIANT_CHOICES])
