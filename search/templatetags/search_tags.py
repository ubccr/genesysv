from django import template
from django.utils.html import mark_safe
from django.template import Context, Template, loader
import json
register = template.Library()

@register.filter(name='splitpipe')
def splitpipe(value):
    return value.split('|')


@register.filter('json_loads')
def json_loads(input_string):
    if input_string:
        return json.loads(input_string)

@register.filter('gene_link')
def gene_link(input_string):
    """
    usage example {{ your_array|format_gatkqs_array }}
    """
    if input_string:
        output = ''
        for ele in input_string.split():
            if ele.lower() != 'none':
                output += '<a target="_blank" href="http://www.genecards.org/cgi-bin/carddisp.pl?gene=%s">%s</a> ' %(ele,ele)
            else:
                output += 'NONE '
        return output


@register.filter('ensembl_link')
def ensembl_link(input_string):
    """
    usage example {{ your_array|format_gatkqs_array }}
    """
    if input_string:
        output = ''
        for ele in input_string.split():
            if ele.lower() != 'none':
                output += '<a target="_blank" href="http://www.ensembl.org/homo_sapiens/Gene/Summary?g=%s">%s</a> ' %(ele,ele)
            else:
                output += 'NONE '
        return output


@register.filter('format_gatkqs_array')
def format_gatkqs_array(input_array):
    """
    usage example {{ your_array|format_gatkqs_array }}
    """
    if input_array:
        if isinstance(input_array, float):
            return input_array
        else:
            tmp = []
            for ele in input_array:
                tmp.append('%s: %s' %(ele['QUAL_cohort'].title(), ele['QUAL_score']))
            return '; '.join(tmp)



@register.filter('get_value_from_dict')
def get_value_from_dict(dict_data, key):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """
    if key:
        return dict_data.get(key)

@register.filter('get_gbrowser_link')
def get_gbrowser_link(variant):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """
    if variant:
        chromosome = variant.split('-')[0]
        start = int(variant.split('-')[1])
        if 'chr' in chromosome.lower():
            position ="%s:%d-%d" %(chromosome, start-25, start+25)
        else:
            position ="chr%d:%d-%d" %(int(chromosome), start-25, start+25)

        link = "http://genome.ucsc.edu/cgi-bin/hgTracks?db=hg19&position=%s" %(position)
        return link




@register.filter('get_value_from_dict_search')
def get_value_from_dict_search(dict_data, element):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """

    if element:
        data = dict_data.get(element.es_name)
        es_id = dict_data.get("es_id")
        # print(dict_data.get("es_id"))
        if element.es_name == 'Variant':
            # print(element.dataset.id)
            output = '<a target="_blank" href="/search/variant/%d/%s">%s</a>' %(element.dataset.id, es_id, data)
            return output
        elif element.es_name == "qs":
            tmp  = []
            for ele in dict_data.get('gatkQS'):
                tmp.append('%s: %s' %(ele['cohort'].title(), ele['qs']))
            return '; '.join(tmp)
        else:
            return data

@register.filter('table_ref_gene')
def table_ref_gene(dict_data, key):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """
    if key:
        data = dict_data.get(key)
        html_string = """
        <table class="table table-condensed">
            <thead>
              <tr>
                <th>Firstname</th>
                <th>Lastname</th>
                <th>Email</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>John</td>
                <td>Doe</td>
                <td>john@example.com</td>
              </tr>
              <tr>
                <td>Mary</td>
                <td>Moe</td>
                <td>mary@example.com</td>
              </tr>
              <tr>
                <td>July</td>
                <td>Dooley</td>
                <td>july@example.com</td>
              </tr>
            </tbody>
        </table>
    """

        template = Template(html_string)
        return template.render({})

