from django import template
from django.template import Context, Template, loader
register = template.Library()


@register.filter('format_gatkqs_array')
def format_gatkqs_array(input_array):
    """
    usage example {{ your_array|format_gatkqs_array }}
    """
    if input_array:
        tmp = []
        for ele in input_array:
            tmp.append('%s: %s' %(ele['cohort'].title(), ele['qs']))
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
        chromosome = int(variant.split('-')[0])
        start = int(variant.split('-')[1])
        position ="chr%d:%d-%d" %(chromosome, start-25, start+25)
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

