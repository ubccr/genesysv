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


def generate_predictor_results_array(array1, array2):
    output = []
    # output.append(['Variant', 'SIFT', 'LRT', 'Polyphen2 HDIV', 'Polyphen2 HVAR'])
    for ele in array1:
        Variant = ele['Variant']
        SIFT_pred = ele.get('SIFT_pred', 'None').replace('_', '').title()
        LRT_pred = ele.get('LRT_pred', 'None').replace('_', '').title()
        Polyphen2_HDIV_pred = ele.get(
            'Polyphen2_HDIV_pred', 'None').replace('_', ' ').title()
        Polyphen2_HVAR_pred = ele.get(
            'Polyphen2_HVAR_pred', 'None').replace('_', ' ').title()

        output.append([Variant, SIFT_pred, LRT_pred,
                       Polyphen2_HDIV_pred, Polyphen2_HVAR_pred])

    for ele in array2:

        Variant = ele['Variant']
        SIFT_pred = ele.get('SIFT_pred', 'None').replace('_', '').title()
        LRT_pred = ele.get('LRT_pred', 'None').replace('_', '').title()
        Polyphen2_HDIV_pred = ele.get(
            'Polyphen2_HDIV_pred', 'None').replace('_', ' ').title()
        Polyphen2_HVAR_pred = ele.get(
            'Polyphen2_HVAR_pred', 'None').replace('_', ' ').title()

        output.append([Variant, SIFT_pred, LRT_pred,
                       Polyphen2_HDIV_pred, Polyphen2_HVAR_pred])

    return output


def dict_remove_duplicate(array_dict1, array_dict2, key):
    key_value = [ele[key] for ele in array_dict1]

    output_array_dict = []
    for ele in array_dict2:
        if ele[key] not in key_value:
            output_array_dict.append(ele)

    return output_array_dict


def put_findings_in_array(findings, keys):
    output = []
    for finding in findings:
        if finding[0] in keys:
            output.append(finding)

    return output


def findings_dict_to_array(findings):
    output = []
    for ele in findings:
        row = []
        row.append(ele['es_id'])
        row.append(ele['Variant'])
        row.append(ele['AF'])

        zygocity = ele['sample']['sample_GT']
        zygocity = 'Homozygous' if zygocity == '1/1' else 'Heterozygous'

        row.append(zygocity)

        gene = []
        refseq_ids = []
        variants = []
        if ele.get('refGene', False):
            for eleGene in ele['refGene']:
                gene.append(eleGene['refGene_symbol'])
                if eleGene.get('refGene_refgene_id', False):
                    tmp = eleGene.get('refGene_refgene_id')
                    tmp = tmp.replace('_', '\\_')
                    refseq_ids.append(tmp)
                else:
                    refseq_ids.append("")
                if eleGene.get('refGene_cDNA_change', False) or eleGene.get('refGene_aa_change', False):
                    variants.append(
                        "%s/%s" % (eleGene.get('refGene_cDNA_change', ''), eleGene.get('refGene_aa_change', '')))
                else:
                    variants.append("")

        tmp_gene = list(set(gene))
        if len(tmp_gene) == 1:
            gene = tmp_gene
        row.append(gene)
        row.append(refseq_ids)
        row.append(variants)

        clinvar = []

        if ele.get('clinvar_20150629', False):
            for eleClinvar in ele['clinvar_20150629']:
                tmp = (eleClinvar['clinvar_20150629_CLNDBN'],
                       eleClinvar['clinvar_20150629_CLINSIG'])
                clinvar.append(tmp)
        row.append(clinvar)

        row.append(ele.get('gwasCatalog', None))

        output.append(row)
    return output


def generate_latex_patient_report(result_summary,
                                  methodology,
                                  additional_notes,
                                  relevant_findings,
                                  incidental_findings,
                                  predictor_array,
                                  output_name):

    latex_jinja_env = jinja2.Environment(
        block_start_string='\BLOCK{',
        block_end_string='}',
        variable_start_string='\VAR{',
        variable_end_string='}',
        comment_start_string='\#{',
        comment_end_string='}',
        line_statement_prefix='%%',
        line_comment_prefix='%#',
        trim_blocks=True,
        autoescape=False,
        loader=jinja2.FileSystemLoader(os.path.abspath('/'))
    )

    basedir = settings.BASE_DIR
    image_path = os.path.join(basedir, 'subject_report', 'image001.jpg')
    path_to_tex = os.path.join(basedir, 'subject_report', 'subject_report.tex')
    template = latex_jinja_env.get_template(path_to_tex)
    # template = latex_jinja_env.get_template(os.path.join('subject_report', 'subject_report.tex'))
    rendered = template.render(result_summary=result_summary,
                               methodology=methodology,
                               additional_notes=additional_notes,
                               relevant_findings=relevant_findings,
                               incidental_findings=incidental_findings,
                               predictor_array=predictor_array,
                               image_path=image_path)

    rendered = rendered.replace('&nbsp;', '')

    with open(os.path.join(basedir, output_name), "wt") as fh:
        fh.write(rendered)

    proc = subprocess.Popen(['pdflatex',
                             '-interaction=nonstopmode',
                             '-halt-on-error',
                             '-output-directory',
                             basedir, os.path.join(basedir, output_name)])
    #proc = subprocess.Popen(['pdflatex', '-interaction=nonstopmode', '-halt-on-error', output_name])
    proc.communicate()


class SubjectReportWizard(SessionWizardView):
    form_list = [SubjectReportForm1, SubjectReportForm2,
                 SubjectReportForm3, SubjectReportForm4]
    # template_name = 'subjectreport/wizard_form.html'

    def get_context_data(self, form, **kwargs):
        context = super(SubjectReportWizard, self).get_context_data(
            form=form, **kwargs)

        if self.steps.current == '2':
            data_0 = self.get_cleaned_data_for_step('0')
            data_1 = self.get_cleaned_data_for_step('1')

            database_name = data_0['dataset']
            subject = data_1['subject']
            indication_for_testing = data_1['indication_for_testing'].lower()

            # relevant_findings, incidental_findings = get_clinvar_gwascatalog(subject, database_name, indication_for_testing)
            relevant_clinvar = get_relevant_clinvar(
                subject, database_name, indication_for_testing)
            not_relevant_clinvar = get_not_relevant_clinvar(
                subject, database_name, indication_for_testing)
            relevant_gwascatalog = get_relevant_gwascatalog(
                subject, database_name, indication_for_testing)
            not_relevant_gwascatalog = get_not_relevant_gwascatalog(
                subject, database_name, indication_for_testing)
            # incidental_findings = group_by_variant_id_key(incidental_findings)
            self.request.session['relevant_clinvar'] = relevant_clinvar
            self.request.session['not_relevant_clinvar'] = not_relevant_clinvar
            self.request.session['relevant_gwascatalog'] = relevant_gwascatalog
            self.request.session[
                'not_relevant_gwascatalog'] = not_relevant_gwascatalog
            # self.request.session['incidental_findings'] = incidental_findings

            not_relevant_clinvar = dict_remove_duplicate(
                relevant_clinvar, not_relevant_clinvar, 'es_id')
            not_relevant_gwascatalog = dict_remove_duplicate(
                relevant_gwascatalog, not_relevant_gwascatalog, 'es_id')

            if relevant_clinvar or not_relevant_clinvar or relevant_gwascatalog or not_relevant_gwascatalog:
                hits = True
            else:
                hits = False

            context['hits'] = hits
            context['subject'] = subject
            context['indication_for_testing'] = indication_for_testing
            context['relevant_clinvar'] = relevant_clinvar
            context['not_relevant_clinvar'] = not_relevant_clinvar
            context['relevant_gwascatalog'] = relevant_gwascatalog
            context['not_relevant_gwascatalog'] = not_relevant_gwascatalog
            # context['incidental_findings'] = incidental_findings

        elif self.steps.current == '3':
            data_2 = self.get_cleaned_data_for_step('2')
            relevant_clinvar = self.request.session.pop('relevant_clinvar')
            not_relevant_clinvar = self.request.session.pop(
                'not_relevant_clinvar')
            relevant_gwascatalog = self.request.session.pop(
                'relevant_gwascatalog')
            not_relevant_gwascatalog = self.request.session.pop(
                'not_relevant_gwascatalog')

            relevant_clinvar_id_keys = data_2['relevant_clinvar']
            not_relevant_clinvar_id_keys = data_2['not_relevant_clinvar']

            relevant_gwascatalog_id_keys = data_2['relevant_gwascatalog']
            not_relevant_gwascatalog_id_keys = data_2[
                'not_relevant_gwascatalog']

            # filter_array_dicts(array, key, values, comparison_type):
            if relevant_clinvar_id_keys:
                relevant_clinvar = filter_array_dicts(
                    relevant_clinvar, 'es_id', relevant_clinvar_id_keys, 'equal')
            else:
                relevant_clinvar = None

            if not_relevant_clinvar_id_keys:
                not_relevant_clinvar = filter_array_dicts(
                    not_relevant_clinvar, 'es_id', not_relevant_clinvar_id_keys, 'equal')
            else:
                not_relevant_clinvar = None

            if relevant_gwascatalog_id_keys:
                relevant_gwascatalog = filter_array_dicts(
                    relevant_gwascatalog, 'es_id', relevant_gwascatalog_id_keys, 'equal')
            else:
                relevant_gwascatalog = None

            if not_relevant_gwascatalog_id_keys:
                not_relevant_gwascatalog = filter_array_dicts(
                    not_relevant_gwascatalog, 'es_id', not_relevant_gwascatalog_id_keys, 'equal')
            else:
                not_relevant_gwascatalog = None

            context['relevant_clinvar'] = relevant_clinvar
            context['not_relevant_clinvar'] = not_relevant_clinvar
            context['relevant_gwascatalog'] = relevant_gwascatalog
            context['not_relevant_gwascatalog'] = not_relevant_gwascatalog

            self.request.session['relevant_clinvar'] = relevant_clinvar
            self.request.session['not_relevant_clinvar'] = not_relevant_clinvar
            self.request.session['relevant_gwascatalog'] = relevant_gwascatalog
            self.request.session[
                'not_relevant_gwascatalog'] = not_relevant_gwascatalog

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
        relevant_clinvar = self.request.session['relevant_clinvar']
        not_relevant_clinvar = self.request.session['not_relevant_clinvar']
        relevant_gwascatalog = self.request.session['relevant_gwascatalog']
        not_relevant_gwascatalog = self.request.session[
            'not_relevant_gwascatalog']

        relevant_findings = []
        incidental_findings = []

        if relevant_clinvar:
            relevant_findings.extend(relevant_clinvar)

        if relevant_gwascatalog:
            relevant_findings.extend(relevant_gwascatalog)

        if not_relevant_clinvar:
            incidental_findings.extend(not_relevant_clinvar)

        if not_relevant_gwascatalog:
            incidental_findings.extend(not_relevant_gwascatalog)

        predictor_array = generate_predictor_results_array(
            relevant_findings, incidental_findings)

        relevant_findings = findings_dict_to_array(relevant_findings)
        incidental_findings = findings_dict_to_array(incidental_findings)

        generate_latex_patient_report(result_summary,
                                      methodology,
                                      additional_notes,
                                      relevant_findings,
                                      incidental_findings,
                                      predictor_array,
                                      'report.tex')

        basedir = settings.BASE_DIR
        output_path = os.path.join(basedir, 'report.pdf')
        pdf_file = open(output_path, 'rb')
        # pdf_file = open('/home/mkzia/gdw/report.pdf', 'rb')
        response = HttpResponse(content=pdf_file)
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
            form = SubjectReportForm2(
                database_name=data_0['dataset'], data=data)
        elif step == "2":
            data_0 = self.get_cleaned_data_for_step('0')
            data_1 = self.get_cleaned_data_for_step('1')

            database_name = data_0['dataset']
            subject = data_1['subject']
            indication_for_testing = data_1['indication_for_testing'].lower()

            extra_data = {'database_name': database_name,
                          'subject': subject,
                          'indication_for_testing': indication_for_testing}

            form = SubjectReportForm3(extra_data=extra_data, data=data)
        elif step == "3":
            data_0 = self.get_cleaned_data_for_step('0')
            data_1 = self.get_cleaned_data_for_step('1')
            subject = data_1['subject']
            database_name = data_0['dataset']
            indication_for_testing = data_1['indication_for_testing'].lower()
            extra_data = {'subject': subject, 'database_name': database_name,
                          'indication_for_testing': indication_for_testing}
            form = SubjectReportForm4(extra_data=extra_data, data=data)

        return form
