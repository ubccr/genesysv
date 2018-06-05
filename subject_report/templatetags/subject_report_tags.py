from django import template

register = template.Library()


@register.filter(name='flatten_key')
def flatten_key(input_array, key):
    tmp = (ele[key] for ele in input_array if key in ele)
    return '<br>'.join(tmp)

@register.filter(name='zip')
def zip_lists(a, b):
    print(len(a),len(b))
    return zip(a, b)

@register.filter(name='zipstring')
def zip_strings(a, b):
    return zip(a.split(','), b.split(','))

@register.filter(name='addbr')
def add_br(input_string):
    return '<br>'.join(input_string.split(','))


@register.filter(name='addbrarray')
def addbrarray(input_string):
    return '<br>'.join(input_string)

@register.filter(name='addbrarrayremoveslash')
def addbrarrayremoveslash(input_string):
    return '<br>'.join(input_string).replace('\\','')

@register.filter(name='addbrstring')
def addbrstring(input_string):
    return '<br>'.join(input_string.split(','))
