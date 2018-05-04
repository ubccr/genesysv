from django import forms
from core.models import Study, Dataset
from django.db.models import Q


from core.models import AttributeField, FilterField, FilterFieldChoice


EXIST_CHOICES = [('', '----'), ("only", "only"), ("excluded", "excluded")]



class StudyForm(forms.Form):
    # You have to comment out study choices before migrating

    def __init__(self, user, *args, **kwargs):
        super(StudyForm, self).__init__(*args, **kwargs)
        user_group_ids = [group.id for group in user.groups.all()]
        # user_dataset = Dataset.objects.all()
        user_dataset = Dataset.objects.select_related('study').filter(
            Q(allowed_groups__in=user_group_ids) | Q(is_public=True)).distinct()
        user_studies = [ele.study.id for ele in user_dataset]
        # print(user_studies)
        STUDY_CHOICES = [(ele.id, ele.name)
                         for ele in Study.objects.filter(id__in=user_studies)]
        # STUDY_CHOICES = [] #Fixme
        STUDY_CHOICES.insert(0, ('', '---'))
        self.fields['study'] = forms.ChoiceField(
            label='Study', choices=STUDY_CHOICES)



class DatasetForm(forms.Form):

    def __init__(self, study_obj, user, *args, **kwargs):
        super(DatasetForm, self).__init__(*args, **kwargs)
        user_group_ids = [group.id for group in user.groups.all()]

        user_dataset = Dataset.objects.filter(study=study_obj)
        user_dataset = user_dataset.filter(
            Q(allowed_groups__in=user_group_ids) | Q(is_public=True)).distinct()

        DATASET_CHOICES = [(ele.id, ele.description)
                           for ele in user_dataset]
        DATASET_CHOICES.insert(0, ('', '---'))
        self.fields['dataset'] = forms.ChoiceField(
            label='Dataset', choices=DATASET_CHOICES)

class FilterFormPart(forms.Form):
    """Filter Form Part is used to create snippet. Filter Form is used to validate the POST data"""

    def __init__(self, fields, MEgroup=None, *args, **kwargs):
        super(FilterFormPart, self).__init__(*args, **kwargs)

        for field in fields:

            if field.tooltip:
                tooltip = ' <i data-toggle="popover" data-trigger="hover" data-content="%s" class="fa fa-info-circle" aria-hidden="true"></i>' % (
                    field.tooltip)
            else:
                tooltip = ''

            label = '%s %s %s' % (
                field.display_text, field.in_line_tooltip if field.in_line_tooltip else '', tooltip)

            field_name = '%d' % (field.id)

            if field.form_type.name == "CharField" and field.widget_type.name == "TextInput":
                self.fields[field_name] = forms.CharField(
                    label=label, required=False)

                if field.default_value:
                    self.fields[field_name].initial = field.default_value
                if MEgroup:
                    self.fields[field_name].widget.attrs.update(
                        {'groupId': MEgroup})

            elif field.form_type.name == "MultipleChoiceField" and field.widget_type.name == "SelectMultiple":
                # CHOICES = [(ele.value, ' '.join(ele.value.split('_')))
                # for ele in
                # FilterFieldChoice.objects.filter(filter_field=field).order_by('pk')]
                CHOICES = [(ele.value, ' '.join(ele.value.split('_')))
                           for ele in field.filterfieldchoice_set.all().order_by('pk')]
                self.fields[field_name] = forms.MultipleChoiceField(
                    label=label, required=False, choices=CHOICES)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update(
                        {'groupId': MEgroup})

            elif field.form_type.name == "ChoiceField" and field.widget_type.name == "Select":
                if field.es_filter_type.name == 'filter_exists':
                    self.fields[field_name] = forms.ChoiceField(
                        label=label, required=False, choices=EXIST_CHOICES)
                elif field.es_filter_type.name == 'nested_filter_exists':
                    self.fields[field_name] = forms.ChoiceField(
                        label=label, required=False, choices=EXIST_CHOICES)
                else:
                    # CHOICES = [(ele.value, ele.value) for ele in FilterFieldChoice.objects.filter(
                    #     filter_field=field).order_by('pk')]
                    CHOICES = [(ele.value, ele.value)
                               for ele in field.filterfieldchoice_set.all().order_by('pk')]
                    CHOICES.insert(0, ('', '----'))
                    self.fields[field_name] = forms.ChoiceField(
                        label=label, required=False, choices=CHOICES)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update(
                        {'groupId': MEgroup})

            elif field.form_type.name == "CharField" and field.widget_type.name == "Textarea":
                self.fields[field_name] = forms.CharField(
                    widget=forms.Textarea(), label=label, required=False)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update(
                        {'groupId': MEgroup})

            elif field.form_type.name == "CharField" and field.widget_type.name == "UploadField":
                self.fields[field_name] = forms.CharField(widget=forms.Textarea(
                    attrs={'rows': 4, 'class': 'upload-field'}), label=label, required=False)
                if MEgroup:
                    self.fields[field_name].widget.attrs.update(
                        {'groupId': MEgroup})


class FilterForm(forms.Form):
    """Filter Form Part is used to create snippet. Filter Form is used to validate the POST data"""

    def __init__(self, dataset, *args, **kwargs):
        super(FilterForm, self).__init__(*args, **kwargs)

        for field in FilterField.objects.filter(dataset=dataset).select_related(
                'widget_type', 'form_type', 'es_filter_type'):

            if field.tooltip:
                tooltip = ' <i data-toggle="popover" data-trigger="hover" data-content="%s" class="fa fa-info-circle" aria-hidden="true"></i>' % (
                    field.tooltip)
            else:
                tooltip = ''
            label = '%s %s %s' % (field.display_text,
                                  field.in_line_tooltip, tooltip)

            field_name = '%d' % (field.id)
            if field.form_type.name == "CharField" and field.widget_type.name == "TextInput":
                self.fields[field_name] = forms.CharField(
                    label=label, required=False)
                self.fields[field_name].widget.attrs.update({'Khawar': 'off'})

            elif field.form_type.name == "MultipleChoiceField" and field.widget_type.name == "SelectMultiple":
                CHOICES = [(ele.value, ' '.join(ele.value.split('_')))
                           for ele in FilterFieldChoice.objects.filter(filter_field=field).order_by('pk')]
                self.fields[field_name] = forms.MultipleChoiceField(
                    label=label, required=False, choices=CHOICES)

            elif field.form_type.name == "ChoiceField" and field.widget_type.name == "Select":
                if field.es_filter_type.name == 'filter_exists':
                    self.fields[field_name] = forms.ChoiceField(
                        label=label, required=False, choices=EXIST_CHOICES)
                elif field.es_filter_type.name == 'nested_filter_exists':
                    self.fields[field_name] = forms.ChoiceField(
                        label=label, required=False, choices=EXIST_CHOICES)
                else:
                    CHOICES = [(ele.value, ele.value) for ele in FilterFieldChoice.objects.filter(
                        filter_field=field).order_by('pk')]
                    CHOICES.insert(0, ('', '----'))
                    self.fields[field_name] = forms.ChoiceField(
                        label=label, required=False, choices=CHOICES)

            elif field.form_type.name == "CharField" and field.widget_type.name == "UploadField":
                self.fields[field_name] = forms.CharField(widget=forms.Textarea(
                    attrs={'rows': 4, 'class': 'upload-field'}), label=label, required=False)




class AttributeFormPart(forms.Form):

    def __init__(self, fields, *args, **kwargs):
        super(AttributeFormPart, self).__init__(*args, **kwargs)

        for field in fields:
            label = field.display_text
            field_name = '%d' % (field.id)
            self.fields[field_name] = forms.BooleanField(
                label=label, required=False)


class AttributeForm(forms.Form):

    def __init__(self, dataset, *args, **kwargs):
        super(AttributeForm, self).__init__(*args, **kwargs)

        for field in dataset.attributefield_set.all():
            label = field.display_text
            field_name = '%d' % (field.id)
            self.fields[field_name] = forms.BooleanField(
                label=label, required=False)
