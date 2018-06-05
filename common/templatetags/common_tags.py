from django import template


register = template.Library()


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
        chromosome = variant.split('_')[0]
        start = int(variant.split('_')[1])
        if 'chr' in chromosome.lower():
            position = "%s:%d-%d" % (chromosome, start - 25, start + 25)
        else:
            position = "chr%d:%d-%d" % (int(chromosome), start - 25, start + 25)

        link = "http://genome.ucsc.edu/cgi-bin/hgTracks?db=hg19&position=%s" % (position)
        return link


@register.filter('get_decipher_link')
def get_decipher_link(variant):
    if variant:
        chromosome = variant.split('_')[0]
        position = int(variant.split('_')[1])

        if 'chr' in chromosome.lower():
            chromosome = int(chromosome.replace('chr', ''))
        else:
            chromosome = int(chromosome)

        part1 = "%d:%d-%d" % (chromosome, position - 25, position + 25)
        part2 = "%d:%d-%d" % (chromosome, position, position)

        link = "https://decipher.sanger.ac.uk/browser#q/%s/location/%s" % (part1, part2)
        return link
