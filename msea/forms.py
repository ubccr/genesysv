from django import forms
from django.contrib.auth.models import User
from search.models import Study, Dataset
from django.db.models import Q
from crispy_forms.helper import FormHelper
from .models import ReferenceSequence, Study
import re


class StudyForm(forms.Form):

    def __init__(self, user, *args, **kwargs):
        super(StudyForm, self).__init__(*args, **kwargs)
        try:
            user_group_ids = [group.id for group in user.groups.all()]
        except:
            user_group_ids = []
        studies = Study.objects.all()
        studies = studies.filter(Q(dataset__allowed_groups__in=user_group_ids) | Q(
            dataset__is_public=True)).distinct()
        STUDY_CHOICES = [(ele.short_name, ele.display_name) for ele in studies]
        STUDY_CHOICES.insert(0, ('', '----'),)

        self.fields['study'] = forms.ChoiceField(choices=STUDY_CHOICES)


class GeneForm(forms.Form):

    def __init__(self, user, *args, **kwargs):
        super(GeneForm, self).__init__(*args, **kwargs)
        try:
            user_group_ids = [group.id for group in user.groups.all()]
        except:
            user_group_ids = []
        studies = Study.objects.all()
        studies = studies.filter(Q(dataset__allowed_groups__in=user_group_ids) | Q(
            dataset__is_public=True)).distinct()
        STUDY_CHOICES = [(ele.short_name, ele.display_name) for ele in studies]

        RECURRENT_CHOICES = (('expand', 'Allow Recurrent Variants'),
                             ('noexpand', 'Do not Allow Recurrent Variants'),
                             )

        self.fields['study'] = forms.ChoiceField(choices=STUDY_CHOICES)
        self.fields['search_term'] = forms.CharField(
            help_text='Enter gene name or reference sequence ID', required=True)
        self.fields['recurrent_variant_option'] = forms.ChoiceField(
            widget=forms.RadioSelect, choices=RECURRENT_CHOICES, required=True, initial='expand')


class VariantForm(forms.Form):

    def __init__(self, rs_id, study, *args, **kwargs):
        super(VariantForm, self).__init__(*args, **kwargs)

        m = re.search('\((.+)\)', rs_id)
        rs_id = m.group(1)
        rf_obj = ReferenceSequence.objects.get(
            rs_id=rs_id, study__short_name=study)
        VARIANT_CHOICES = [(ele.short_name, ele.full_name)
                           for ele in rf_obj.variants.exclude(short_name__in=['lof', 'miss'])]

        if ('ansi', 'All non-Silent SNVs and Indels') and ('ans', 'All non-Silent SNVs') in VARIANT_CHOICES:
            pos_ansi = VARIANT_CHOICES.index(
                ('ansi', 'All non-Silent SNVs and Indels'))
            pos_ans = VARIANT_CHOICES.index(('ans', 'All non-Silent SNVs'))
            if pos_ansi < pos_ans:
                VARIANT_CHOICES[pos_ans], VARIANT_CHOICES[
                    pos_ansi] = VARIANT_CHOICES[pos_ansi], VARIANT_CHOICES[pos_ans]

        if ('dnsi', 'Deleterious non-Silent SNVs and Indels') and ('dns', 'Deleterious non-Silent SNVs') in VARIANT_CHOICES:
            pos_dnsi = VARIANT_CHOICES.index(
                ('dnsi', 'Deleterious non-Silent SNVs and Indels'))
            pos_dns = VARIANT_CHOICES.index(
                ('dns', 'Deleterious non-Silent SNVs'))
            if pos_dnsi < pos_dns:
                VARIANT_CHOICES[pos_dns], VARIANT_CHOICES[
                    pos_dnsi] = VARIANT_CHOICES[pos_dnsi], VARIANT_CHOICES[pos_dns]

        self.fields['variant_choice'] = forms.ChoiceField(
            choices=VARIANT_CHOICES, initial=[sn for sn, fl in VARIANT_CHOICES])
