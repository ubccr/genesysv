from django import forms

class SampleSelectForm(forms.Form):
    def __init__(self, samples, *args, **kwargs):
        super(SampleSelectForm, self).__init__(*args, **kwargs)

        for sample in samples:
            field_name = sample
            display_name = sample;

            self.fields[field_name] = forms.BooleanField(label=display_name, required=False)
