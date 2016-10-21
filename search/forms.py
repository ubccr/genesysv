from django import forms
from django.contrib.auth.models import User
from .models import FilterField, FilterFieldChoice, Dataset, Study, FilterSubPanel
from django.db.models import Q
from crispy_forms.helper import FormHelper


class StudyForm(forms.Form):
    # You have to comment out study choices before migrating
     def __init__(self, user, *args, **kwargs):
        super(StudyForm, self).__init__(*args, **kwargs)
        user_group_ids = [group.id for group in user.groups.all()]
        user_dataset = Dataset.objects.all()
        user_dataset = user_dataset.filter(Q(allowed_groups__in=user_group_ids) | Q(is_public=True)).distinct()
        user_studies = [ele.study.id for ele in user_dataset]
        # print(user_studies)
        STUDY_CHOICES = [(ele.name, ele.name) for ele in Study.objects.filter(id__in=user_studies)]
        # STUDY_CHOICES = [] #Fixme
        STUDY_CHOICES.insert(0, ('','---'))
        self.fields['study'] = forms.ChoiceField(label='Study', choices=STUDY_CHOICES)


class DatasetForm(forms.Form):

    def __init__(self, selected_study, user, *args, **kwargs):
        super(DatasetForm, self).__init__(*args, **kwargs)
        user_group_ids = [group.id for group in user.groups.all()]

        user_dataset = Dataset.objects.filter(study__name=selected_study)
        user_dataset = user_dataset.filter(Q(allowed_groups__in=user_group_ids) | Q(is_public=True)).distinct()

        DATASET_CHOICES = [(ele.description, ele.description) for ele in user_dataset]
        DATASET_CHOICES.insert(0, ('','---'))
        self.fields['dataset'] = forms.ChoiceField(label='Dataset', choices=DATASET_CHOICES)


class ESFilterFormPart(forms.Form):
    def __init__(self, fields, MEgroup=None, *args, **kwargs):
        super(ESFilterFormPart, self).__init__(*args, **kwargs)


        for field in fields:

            if field.tooltip:
                tooltip = ' <i data-toggle="popover" data-trigger="hover" data-content="%s" class="fa fa-info-circle" aria-hidden="true"></i>' %(field.tooltip)
            else:
                tooltip = ''

            label = '%s %s %s' %(field.display_name, field.in_line_tooltip, tooltip)


            field_name = '%s-%s-%s' %(field.es_name, field.es_filter_type, field.path)
            if field.form_type.name == "CharField" and field.widget_type.name == "TextInput":
                self.fields[field_name] = forms.CharField(label=label, required=False)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update({'groupId': MEgroup})

            elif field.form_type.name == "MultipleChoiceField" and field.widget_type.name == "SelectMultiple":
                CHOICES =[(ele.value, ' '.join(ele.value.split('_'))) for ele in FilterFieldChoice.objects.filter(filter_field=field)]
                self.fields[field_name] = forms.MultipleChoiceField(label=label, required=False, choices=CHOICES)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update({'groupId': MEgroup})

            elif field.form_type.name == "ChoiceField" and field.widget_type.name == "Select":
                CHOICES =[(ele.value, ele.value) for ele in FilterFieldChoice.objects.filter(filter_field=field)]
                CHOICES.insert(0,('', '----'))
                self.fields[field_name] = forms.ChoiceField(label=label, required=False, choices=CHOICES)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update({'groupId': MEgroup})

            elif field.form_type.name == "CharField" and field.widget_type.name == "Textarea":
                self.fields[field_name] = forms.CharField(widget=forms.Textarea(), label=label, required=False)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update({'groupId': MEgroup})

            elif field.form_type.name == "CharField" and field.widget_type.name == "UploadField":
                self.fields[field_name] = forms.CharField(widget=forms.Textarea(attrs={'rows': 4, 'class':'upload-field'}), label=label, required=False)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update({'groupId': MEgroup})



class ESFilterForm(forms.Form):
    def __init__(self, *args, **kwargs):
        super(ESFilterForm, self).__init__(*args, **kwargs)


        for field in FilterField.objects.all():

            if field.tooltip:
                tooltip = ' <i data-toggle="popover" data-trigger="hover" data-content="%s" class="fa fa-info-circle" aria-hidden="true"></i>' %(field.tooltip)
            else:
                tooltip = ''
            label = '%s %s %s' %(field.display_name, field.in_line_tooltip, tooltip)


            field_name = '%s-%s-%s' %(field.es_name, field.es_filter_type, field.path)
            if field.form_type.name == "CharField" and field.widget_type.name == "TextInput":
                self.fields[field_name] = forms.CharField(label=label, required=False)
                self.fields[field_name].widget.attrs.update({'Khawar': 'off'})

            elif field.form_type.name == "MultipleChoiceField" and field.widget_type.name == "SelectMultiple":
                CHOICES =[(ele.value, ' '.join(ele.value.split('_'))) for ele in FilterFieldChoice.objects.filter(filter_field=field)]
                self.fields[field_name] = forms.MultipleChoiceField(label=label, required=False, choices=CHOICES)

            elif field.form_type.name == "ChoiceField" and field.widget_type.name == "Select":
                CHOICES =[(ele.value, ele.value) for ele in FilterFieldChoice.objects.filter(filter_field=field)]
                CHOICES.insert(0,('', '----'))
                self.fields[field_name] = forms.ChoiceField(label=label, required=False, choices=CHOICES)

            elif field.form_type.name == "CharField" and field.widget_type.name == "UploadField":
                self.fields[field_name] = forms.CharField(widget=forms.Textarea(attrs={'rows': 4, 'class':'upload-field'}), label=label, required=False)


class ESAttributeFormPart(forms.Form):
    def __init__(self, fields, *args, **kwargs):
        super(ESAttributeFormPart, self).__init__(*args, **kwargs)


        for field in fields:
            label = field.display_name
            field_name = '%s-%s' %(field.es_name, field.path)
            self.fields[field_name] = forms.BooleanField(label=label, required=False)


class ESAttributeForm(forms.Form):
    def __init__(self, *args, **kwargs):
        super(ESAttributeForm, self).__init__(*args, **kwargs)


        for field in FilterField.objects.all():
            label = field.display_name
            field_name = '%s-%s' %(field.es_name, field.path)
            self.fields[field_name] = forms.BooleanField(label=label, required=False)
