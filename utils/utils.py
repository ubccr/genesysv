import re
from collections import defaultdict, deque, Counter

def convert_escaped_chars(input_string):
    input_string = input_string.replace("\\x3b", ";")
    input_string = input_string.replace("\\x2c", ",")
    input_string = input_string.replace("\\x3d", "=")

    return input_string

def isfloat(value):
  try:
    val_float = float(value)
    try:
        val_int = int(value)
        if val_float == val_int:
            return False
    except ValueError:
        return True
  except ValueError:
    return False

def isint(value):
  try:
    int(value)
    return True
  except ValueError:
    return False

def determine_es_datatype(value):
    if isfloat(value):
        return "float"
    elif isint(value):
        return "integer"
    else:
        return "keyword"

def parse_fields(name, vcf_filename):
    fields = {}
    with open(vcf_filename, "r") as fp:
        for line_no, line in enumerate(fp , 1):
            if line.startswith("#CHROM"):
                break
            if not line.startswith("##%s" %(name)):
                continue

            field_id, description = parse_field_id_and_description(line.strip())
            fields[field_id] = description

    return fields

def parse_field_id_and_description(input_str):


    import re
    regexp_get_between_brackets = r'\<(.+)\>'
    regexp_get_id = r'ID=([^,]+)'
    regexp_get_description = r'Description=(.+)'

    try:
        fields = re.search(regexp_get_between_brackets, input_str).groups()[0]
    except:
        raise ValueError('Could not find anything between brackets for input:\n%s' %(input_str))

    try:
        field_id = re.search(regexp_get_id, fields).groups()[0]
    except:
        raise ValueError('Could not parse "ID" from "%s"' %(fields))

    try:
        description = re.search(regexp_get_description, fields).groups()[0]
    except:
        raise ValueError('Could not parse "Description" from "%s"' %(fields))

    ### remove quotes
    if description.startswith("\"") and description.endswith("\""):
        description = description[1:-1]

    return field_id, description
