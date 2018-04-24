from django import forms


class MendelianAnalysisForm(forms.Form):
    ANALYSIS_CHOICES = (('', '----'),
        ('autosomal_dominant', 'autosomal_dominant'),
        ('autosomal_recessive', 'autosomal_recessive'),
        ('compound_heterozygous', 'compound_heterozygous'),
        ('denovo', 'denovo'),
    )

    analysis_type = forms.ChoiceField(choices=ANALYSIS_CHOICES)



class FamilyForm(forms.Form):

    def __init__(self, sample_ids, *args, **kwargs):
        super(FamilyForm, self).__init__(*args, **kwargs)

        SAMPLE_CHOICES = [(ele, ele) for ele in sample_ids]
        SAMPLE_CHOICES.insert(0, ('', '---Select ID---'))

        self.fields['father_id'] = forms.ChoiceField(
                        label='Father ID', required=True, choices=SAMPLE_CHOICES)
        self.fields['mother_id'] = forms.ChoiceField(
                        label='Mother ID', required=True, choices=SAMPLE_CHOICES)
        self.fields['child_id'] = forms.CharField(
                        label='Child ID', required=True)
