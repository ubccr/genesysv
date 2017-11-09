from django import forms
from search.models import Dataset
from trio.models import PedigreeInformation

class PEDForm(forms.Form):
    def __init__(self, user, *args, **kwargs):
        super(PEDForm, self).__init__(*args, **kwargs)

        user_dataset = Dataset.objects.filter(allowed_groups__in=user.groups.all()).distinct().exclude(id__in=[ele.dataset.id for ele in PedigreeInformation.objects.all()])
        DATASET_CHOICES = [(ele.id, ele.description) for ele in user_dataset]
        DATASET_CHOICES.insert(0, ('','---'))
        self.fields['dataset'] = forms.ChoiceField(label='Dataset', choices=DATASET_CHOICES)
        self.fields['ped_file'] = forms.FileField()

