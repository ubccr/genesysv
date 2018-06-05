from django import template
from django.utils.html import mark_safe

register = template.Library()

@register.filter('get_value_from_dict')
def get_value_from_dict(dict_data, key):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """
    if key:
        return dict_data.get(key)


@register.simple_tag
def get_value_from_dict_core(dict_data, element, app_name):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """

    if element:
        path = element.path
        data = dict_data.get(element.es_name)
        if data == 0:
            return data
        if not data:
            return None
        es_id = dict_data.get("es_id")
        if element.is_link_field:
            if element.es_name == "Variant":
                output = mark_safe('<a target="_blank" href="/%s/%s-document-view/%d/%s">%s</a>' %(app_name, app_name, element.dataset.id, es_id, data))
            return output
        else:
            return data

