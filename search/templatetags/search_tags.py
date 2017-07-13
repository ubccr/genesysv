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
                output += '<a target="_blank" href="http://www.genecards.org/cgi-bin/carddisp.pl?gene=%s"><i class="fa fa-external-link" aria-hidden="true"></i> GeneCards</a>' %(ele)
            else:
                output += 'NONE '
        return output

@register.filter('gene_mania')
def gene_mania(input_string):
    """
    usage example {{ your_array|format_gatkqs_array }}
    """

    #http://genemania.org/link?o=9606&g=AGRN
    if input_string:
        output = ''
        for ele in input_string.split():
            if ele.lower() != 'none':
                output += '<a target="_blank" href="http://genemania.org/link?o=9606&g=%s"><i class="fa fa-external-link" aria-hidden="true"></i> GeneMania</a>' %(ele.replace(',','|'))
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


@register.filter('format_gatkfilter_array')
def format_gatkfilter_array(input_array):
    """
    usage example {{ your_array|format_gatkfilter_array }}
    """
    if input_array:
        if isinstance(input_array, str):
            return input_array
        else:
            tmp = []
            for ele in input_array:
                tmp.append('%s: %s' %(ele['FILTER_cohort'].title(), ele['FILTER_status']))
            return '; '.join(tmp)




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

@register.filter('get_decipher_link')
def get_decipher_link(variant):
    if variant:
        chromosome = variant.split('-')[0]
        position = int(variant.split('-')[1])

        if 'chr' in chromosome.lower():
            chromosome = int(chromosome.replace('chr',''))
        else:
            chromosome = int(chromosome)

        part1 ="%d:%d-%d" %(chromosome, position-25, position+25)
        part2 ="%d:%d-%d" %(chromosome, position, position)

        link = "https://decipher.sanger.ac.uk/browser#q/%s/location/%s" %(part1, part2)
        return link




@register.filter('get_value_from_dict_search')
def get_value_from_dict_search(dict_data, element):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """

    if element:
        data = dict_data.get(element.es_name)
        if not data:
            return None
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

