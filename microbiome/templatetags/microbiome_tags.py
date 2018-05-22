import json

from django import template

register = template.Library()

@register.filter('format_search_log')
def format_search_log(json_string):
    """
    usage example {{ model_object|format_search_log }}
    """
    query_json = json.loads(json_string)
    attributes_selected = sorted(query_json.get('_source'))


    context = {}
    context['attributes_selected'] = attributes_selected

    return attributes_selected
