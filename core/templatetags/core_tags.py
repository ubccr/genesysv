from django import template

register = template.Library()

@register.filter('get_value_from_dict')
def get_value_from_dict(dict_data, key):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """
    if key:
        return dict_data.get(key)


@register.filter('get_value_from_dict_core')
def get_value_from_dict_core(dict_data, element):
    """
    usage example {{ your_dict|get_value_from_dict:your_key }}
    """

    if element:
        path = element.path
        if path in ['FILTER', 'QUAL']:
            if path == "FILTER":
                data = dict_data.get(path)
                if not data:
                    return None
                if len(data) >= 1:
                    return '; '.join(["%s %s" %(ele.get('FILTER_label', ""), ele.get('FILTER_value')) for ele in data])
                else:
                    return "Report ERROR"
            elif path == "QUAL":
                data = dict_data.get(path)
                if not data:
                    return None
                if len(data) >= 1:
                    return '; '.join(["%s %s" %(ele.get('QUAL_label', ""), ele.get('QUAL_value')) for ele in data])
                else:
                    return "Report ERROR"
        else:
            data = dict_data.get(element.es_name)

        if data == 0:
            return data
        if not data:
            return None
        es_id = dict_data.get("es_id")
        # print(dict_data.get("es_id"))
        if element.es_name == 'Variant':
            # print(element.dataset.id)
            output = '<a target="_blank" href="/search/variant/%d/%s">%s</a>' %(element.dataset.id, es_id, data)
            return output
        else:
            return data

