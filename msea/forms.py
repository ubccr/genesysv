from django import forms
from django.contrib.auth.models import User
from search.models import Study, Dataset
from django.db.models import Q
from crispy_forms.helper import FormHelper
from .models import ReferenceSequence
import re

class GeneForm(forms.Form):

    # VARIANT_FILE_TYPE_CHOICES = (('ass', 'All Silent SNVs'),
    #                      ('asbns', 'All Silent Benign non-Silent SNVs'),
    #                      ('ansi', 'All non-Silent SNVs and Indels'),
    #                      ('ans', 'All non-Silent SNVs' ),
    #                      ('dnsi', 'Deleterious non-Silent SNVs and Indels'),
    #                      ('dns', 'Deleterious non-Silent SNVs'),
    #                      ('prom', 'Variants in Promoter Region'),
    #                 )

    EXPAND_OPTIONS = (('expand', 'Expanded (recurrent variants are used independently for analysis)'),
                       ('noexpand', 'Not Expanded (recurrent variants are collapsed into a single variant for analysis​)'),
                       )

    DATASET_CHOICE = (  ('sim_wgs', 'SIM Sensitive Genome'),
                        ('sim_sen', 'SIM Sensitive Exome'),
                        ('sim_res', 'SIM Resistant Exome'),
    )

    dataset = forms.ChoiceField(choices=DATASET_CHOICE)
    search_term = forms.CharField(help_text='Enter gene name or reference sequence ID', required=True)
    # variant_file_type = forms.ChoiceField(choices=VARIANT_FILE_TYPE_CHOICES, required=True, initial='dnsi')
    expand_option = forms.ChoiceField(widget=forms.RadioSelect, choices=EXPAND_OPTIONS, required=True, initial='expand')


class VariantForm(forms.Form):
    def __init__(self, rs_id, *args, **kwargs):
        super(VariantForm, self).__init__(*args, **kwargs)

        m = re.search('\((.+)\)', rs_id)
        rs_id = m.group(1)
        rf_obj = ReferenceSequence.objects.get(rs_id=rs_id)
        VARIANT_CHOICES = [(ele.short_name, ele.full_name) for ele in rf_obj.variants.all()]
        print(VARIANT_CHOICES)
        self.fields['variant_choices'] = forms.MultipleChoiceField(choices=VARIANT_CHOICES, initial=[sn for sn, fl in VARIANT_CHOICES])
