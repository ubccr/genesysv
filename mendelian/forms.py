from django import forms


class MendelianAnalysisForm(forms.Form):
    ANALYSIS_CHOICES = (('', '----'),
                        ('autosomal_dominant', 'autosomal_dominant'),
                        ('autosomal_recessive', 'autosomal_recessive'),
                        ('compound_heterozygous', 'compound_heterozygous'),
                        ('denovo', 'denovo'),
                        )

    analysis_type = forms.ChoiceField(choices=ANALYSIS_CHOICES)


class KindredForm(forms.Form):

    def __init__(self, number_of_families, *args, **kwargs):
        super().__init__(*args, **kwargs)

        KINDRED_CHOICES = [(ele, '> ' + str(ele)) for ele in range(1, number_of_families)]
        KINDRED_CHOICES.insert(0, ('', '---No Kindred Filtering---'))
        self.fields['number_of_kindred'] = forms.ChoiceField(
            label='Number of Kindred', required=False, choices=KINDRED_CHOICES)


class FamilyForm(forms.Form):

    def __init__(self, sample_ids, *args, **kwargs):
        super().__init__(*args, **kwargs)

        SAMPLE_CHOICES = [(ele, ele) for ele in sample_ids]
        SAMPLE_CHOICES.insert(0, ('', '---Select ID---'))

        self.fields['father_id'] = forms.ChoiceField(
            label='Father ID', required=True, choices=SAMPLE_CHOICES)
        self.fields['mother_id'] = forms.ChoiceField(
            label='Mother ID', required=True, choices=SAMPLE_CHOICES)
        self.fields['child_id'] = forms.CharField(
            label='Child ID', required=True)
