from django.shortcuts import render
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from formtools.wizard.views import SessionWizardView
from .forms import *
import jinja2
import os
from jinja2 import Template
import subprocess
from django.conf import settings
from django.views.static import serve
from pprint import pprint
from common.utils import filter_array_dicts

def put_findings_in_array(findings, keys):
    output = []
    for finding in findings:
        if finding[0] in keys:
            output.append(finding)

    return output

def generate_latex_patient_report(report_type,
                                  result_summary,
                                  methodology,
                                  additional_notes,
                                  relevant_findings,
                                  incidental_findings,
                                  output_name):

    latex_jinja_env = jinja2.Environment(
        block_start_string = '\BLOCK{',
        block_end_string = '}',
        variable_start_string = '\VAR{',
        variable_end_string = '}',
        comment_start_string = '\#{',
        comment_end_string = '}',
        line_statement_prefix = '%%',
        line_comment_prefix = '%#',
        trim_blocks = True,
        autoescape = False,
        loader = jinja2.FileSystemLoader(os.path.abspath('.'))
    )



    basedir = settings.BASE_DIR
    template = latex_jinja_env.get_template(os.path.join('subjectreport', 'subject_report.tex'))
    rendered = template.render( report_type=report_type,
                                result_summary=result_summary,
                                methodology=methodology,
                                additional_notes=additional_notes,
                                relevant_findings=relevant_findings,
                                incidental_findings=incidental_findings)


    rendered = rendered.replace('&nbsp;','')


    pprint(relevant_findings)
    pprint(incidental_findings)
    with open(output_name, "wt") as fh:
        fh.write(rendered)


    proc = subprocess.Popen(['pdflatex', '-interaction=nonstopmode', '-halt-on-error', output_name])
    proc.communicate()


class SubjectReportWizard(SessionWizardView):
    form_list = [SubjectReportForm1, SubjectReportForm2, SubjectReportForm3, SubjectReportForm4]
    # template_name = 'subjectreport/wizard_form.html'

    def get_context_data(self, form, **kwargs):
        context = super(SubjectReportWizard, self).get_context_data(form=form, **kwargs)

        if self.steps.current == '2':
            data_0 = self.get_cleaned_data_for_step('0')
            data_1 = self.get_cleaned_data_for_step('1')

            database_name = data_0['dataset']
            subject = data_1['subject']
            indication_for_testing = data_1['indication_for_testing']

            # relevant_findings, incidental_findings = get_clinvar_gwascatalog(subject, database_name, indication_for_testing)
            relevant_clinvar = get_relevant_clinvar(subject, database_name, indication_for_testing)
            not_relevant_clinvar = get_not_relevant_clinvar(subject, database_name, indication_for_testing)
            relevant_gwascatalog = get_relevant_gwascatalog(subject, database_name, indication_for_testing)
            not_relevant_gwascatalog = get_not_relevant_gwascatalog(subject, database_name, indication_for_testing)
            # incidental_findings = group_by_variant_id_key(incidental_findings)
            self.request.session['relevant_clinvar'] = relevant_clinvar
            self.request.session['not_relevant_clinvar'] = not_relevant_clinvar
            self.request.session['relevant_gwascatalog'] = relevant_gwascatalog
            self.request.session['not_relevant_gwascatalog'] = not_relevant_gwascatalog
            # self.request.session['incidental_findings'] = incidental_findings
            context['relevant_clinvar'] = relevant_clinvar
            context['not_relevant_clinvar'] = not_relevant_clinvar
            context['relevant_gwascatalog'] = relevant_gwascatalog
            context['not_relevant_gwascatalog'] = not_relevant_gwascatalog
            # context['incidental_findings'] = incidental_findings

        elif self.steps.current == '3':
            data_2 = self.get_cleaned_data_for_step('2')
            relevant_clinvar = self.request.session.pop('relevant_clinvar')
            not_relevant_clinvar = self.request.session.pop('not_relevant_clinvar')
            relevant_gwascatalog = self.request.session.pop('relevant_gwascatalog')
            not_relevant_gwascatalog = self.request.session.pop('not_relevant_gwascatalog')

            relevant_clinvar_id_keys = data_2['relevant_clinvar']
            not_relevant_clinvar_id_keys = data_2['not_relevant_clinvar']

            relevant_gwascatalog_id_keys = data_2['relevant_gwascatalog']
            not_relevant_gwascatalog_id_keys = data_2['not_relevant_gwascatalog']


            #filter_array_dicts(array, key, values, comparison_type):
            if relevant_clinvar_id_keys:
                relevant_clinvar = filter_array_dicts(relevant_clinvar, 'es_id', relevant_clinvar_id_keys, 'equal')
            else:
                relevant_clinvar = None

            if not_relevant_clinvar_id_keys:
                not_relevant_clinvar = filter_array_dicts(not_relevant_clinvar, 'es_id', not_relevant_clinvar_id_keys, 'equal')
            else:
                not_relevant_clinvar = None

            if relevant_gwascatalog_id_keys:
                relevant_gwascatalog = filter_array_dicts(relevant_gwascatalog, 'es_id', relevant_gwascatalog_id_keys, 'equal')
            else:
                relevant_gwascatalog = None

            if not_relevant_gwascatalog_id_keys:
                not_relevant_gwascatalog = filter_array_dicts(not_relevant_gwascatalog, 'es_id', not_relevant_gwascatalog_id_keys, 'equal')
            else:
                not_relevant_gwascatalog = None

            context['relevant_clinvar'] = relevant_clinvar
            context['not_relevant_clinvar'] = not_relevant_clinvar
            context['relevant_gwascatalog'] = relevant_gwascatalog
            context['not_relevant_gwascatalog'] = not_relevant_gwascatalog

            self.request.session['relevant_clinvar'] = relevant_clinvar
            self.request.session['not_relevant_clinvar'] = not_relevant_clinvar
            self.request.session['relevant_gwascatalog'] = relevant_gwascatalog
            self.request.session['not_relevant_gwascatalog'] = not_relevant_gwascatalog

        return context

    def get_template_names(self):
        step = self.steps.current
        if step == "0":
            return 'subject_report/subject_report_step1.html'
        elif step == "1":
            return 'subject_report/subject_report_step2.html'
        elif step == "2":
            return 'subject_report/subject_report_step3.html'
        elif step == "3":
            return 'subject_report/subject_report_step4.html'

        return [TEMPLATES[self.steps.current]]

    def done(self, form_list, **kwargs):
        form_data = [form.cleaned_data for form in form_list]
        for data in form_data:
            if 'methodology' in list(data):
                methodology = data['methodology']
            if 'result_summary' in list(data):
                result_summary = data['result_summary']
            if 'additional_notes' in list(data):
                additional_notes = data['additional_notes']
            if 'dataset' in list(data):
                dataset = data['dataset']

        report_type = map_database_name_table_name[dataset][-1].title()
        relevant_findings = self.request.session['relevant_findings']
        incidental_findings = self.request.session['incidental_findings']


        generate_latex_patient_report(report_type,
                                        result_summary,
                                        methodology,
                                        additional_notes,
                                        relevant_findings,
                                        incidental_findings,
                                        'report.tex')

        test_file = open('/home/mkzia/bigdw-website/report.pdf', 'rb')
        response = HttpResponse(content=test_file)
        response['Content-Type'] = 'application/pdf'
        response['Content-Disposition'] = 'attachment; filename="report.pdf"'
        return response


    def get_form(self, step=None, data=None, files=None):

        if step is None:
            step = self.steps.current

        if step == "0":
            form = SubjectReportForm1(user=self.request.user, data=data)
        elif step == "1":
            data_0 = self.get_cleaned_data_for_step('0')
            form = SubjectReportForm2(database_name=data_0['dataset'], data=data)
        elif step == "2":
            data_0 = self.get_cleaned_data_for_step('0')
            data_1 = self.get_cleaned_data_for_step('1')

            database_name = data_0['dataset']
            subject = data_1['subject']
            indication_for_testing = data_1['indication_for_testing']

            extra_data = {'database_name': database_name,
                          'subject': subject,
                          'indication_for_testing': indication_for_testing}

            form = SubjectReportForm3(extra_data=extra_data, data=data)
        elif step == "3":
            data_0 = self.get_cleaned_data_for_step('0')
            data_1 = self.get_cleaned_data_for_step('1')
            subject = data_1['subject']
            database_name = data_0['dataset']
            indication_for_testing = data_1['indication_for_testing']
            extra_data = {'subject': subject, 'database_name':database_name, 'indication_for_testing': indication_for_testing}
            form = SubjectReportForm4(extra_data=extra_data, data=data)

        return form

