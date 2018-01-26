from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Submit, Layout, Div
from crispy_forms.bootstrap import  FormActions, StrictButton

CHROMOSOME_CHOICES = [(str(i), str(i)) for i in range(1,22)]
CHROMOSOME_CHOICES.extend((('X','X'),('Y','Y'),('M','M')))

ALTERNATE_ALLELE_CHOICES = (
    ('A', 'A'),
    ('C', 'C'),
    ('G', 'G'),
    ('T', 'T'),
    ('I', 'I'),
    ('D', 'D'),
)

class BeaconQueryForm(forms.Form):
    chromosome = forms.ChoiceField(
        choices=CHROMOSOME_CHOICES,
        required = True,
    )
    alternate_allele = forms.ChoiceField(
        choices=ALTERNATE_ALLELE_CHOICES,
        required = True,
    )
    coordinate = forms.IntegerField(
        required = True,
        initial=150764324,
    )
