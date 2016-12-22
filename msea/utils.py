import json
from itertools import repeat
from pprint import pprint
from bokeh.io import save
from bokeh.models import Legend, HoverTool, Arrow, VeeHead, Label
from bokeh.layouts import gridplot
from bokeh.plotting import figure, show, output_file, ColumnDataSource
from bokeh.palettes import brewer
from django.conf import settings
import os
#from bokeh.models import HoverTool

def format_domain_for_R(results):
    # print(results)
    formatted_results = {}
    for ele in results:
        for key,val in ele.items():
            # print(key,val)
            if key not in formatted_results:
                formatted_results[key] = []
            formatted_results[key].append(val)



    formatted_results['refseq.ID'] = formatted_results['refseq.ID'][0]
    formatted_results['symbol'] = formatted_results['symbol'][0]
    return formatted_results

import elasticsearch
def generate_variant_bplot(msea_type_name, gene, rs_id, vset):
    import json

    es = elasticsearch.Elasticsearch(host="199.109.195.45")
    if vset == "prom":
        domain_rs_id = gene
        domain_es_index_name = "tfbs"
        domain_es_type_name = "tfbs"
    else:
        domain_rs_id = rs_id
        domain_es_index_name = "protein"
        domain_es_type_name = "protein"

    ############## Gene START
    query_string_template = """
        {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"refgene_id":"%s"}},
                        {"term": {"vset":"%s"}}
                    ]
                }
            },
            "size": 1000
        }
    """

    body = query_string_template %(rs_id,vset)
    index_name = "msea"
    response = es.search(index=index_name, doc_type=msea_type_name, body=body)
    total = response['hits']['total']
    gene_results = response['hits']['hits'][0]['_source']

    gene_json_output =  os.path.join(settings.BASE_DIR, 'static/bokeh_outputs/%s_%s_gene.json' %(rs_id, vset))
    with open(gene_json_output, 'w') as outfile:
        json.dump(gene_results, outfile, indent=4, sort_keys=True)

    vset_map = {'ans':'All nonsilent SNVs',
                'ansi':'All nonsilent SNVs and Indels',
                'dns':'Deleterious nonsilent SNVs',
                'dnsi':'Deleterious nonsilent SNVs and Indels',
                'ass':'All silent SNVs',
                'asbns':'All silent SNVs plus benign nonsilent SNVs',
                'prom':'Promoter',
                'lof':'Loss of Function',
                'miss':'Missense'}

    vset = gene_results['vset']
    gene_length = gene_results['length']
    mas = gene_results['mutation_accumulation_score']
    title = '%s (%s), %s -- P-Value: %s -- NES: %s' %(
        gene_results['gene_name'],
        gene_results['refgene_id'],
        vset_map[gene_results['vset']],
        gene_results['pvalue'],
        gene_results['nes'])

    x = mas[0::2]
    y = mas[1::2]
    x = [float(ele) for ele in x]
    y = [float(ele) for ele in y]

    mutation_types_x = gene_results['mutations']['mutation_types'][0::2]
    mutation_types_x = [int(ele)-1 for ele in mutation_types_x]
    mutation_types = gene_results['mutations']['mutation_types'][1::2]

    exonic_functions_x = gene_results['mutations']['exonic_functions'][0::2]
    exonic_functions_x = [int(ele)-1 for ele in exonic_functions_x]
    exonic_functions = gene_results['mutations']['exonic_functions'][1::2]

    frequencies_x = gene_results['mutations']['frequencies'][0::2]
    frequencies_x = [int(ele)-1 for ele in frequencies_x]
    frequencies = gene_results['mutations']['frequencies'][1::2]

    deleterious_x = gene_results['mutations']['deleterious'][0::2]
    deleterious_x = [int(ele)-1 for ele in deleterious_x]
    deleterious = gene_results['mutations']['deleterious'][1::2]
    ############## Gene END


    ############## DOMAIN START
    domain_query_template = """
        {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"refseq.ID":"%s"}}
                    ]
                }
            },
            "size": 1000
        }
    """
    body = domain_query_template %(domain_rs_id)
    response = es.search(index=domain_es_index_name, doc_type=domain_es_type_name, body=body)
    total = response['hits']['total']
    tmp_results = response['hits']['hits']
    domain_results = [ele["_source"] for ele in tmp_results]


    if domain_results:
        domain_data = format_domain_for_R(domain_results)
        domain_json_output =  os.path.join(settings.BASE_DIR, 'static/bokeh_outputs/%s_%s_domain.json' %(rs_id, vset))
        with open(domain_json_output, 'w') as outfile:
            json.dump(domain_data, outfile, indent=4, sort_keys=True)


    ############## DOMAIN END


    ############# Constants
    ### Domains

    y_range = max(y)-min(y)
    gray_height = y_range *.02
    gray_y_value = min(y)-(y_range*0.1) #seq.line.y
    gray_top = gray_y_value+gray_height/2
    gray_bot = gray_y_value-gray_height/2
    palette = ('#CC79A7','#D55E00','#0072B2','#F0E442','#009E73','#56B4E9','#E69F00','#999999')

    domain_types = []
    for domain in domain_results:
        domain_types.append(domain['domain.type'])

    domain_type_count_val = len(set(domain_types))

    if domain_type_count_val <= 1:
        color_height = y_range *.04
        color_top = gray_y_value+color_height/2
        color_bot = gray_y_value-color_height/2
    elif domain_type_count_val > 1:
        color_height = y_range *.02
        color_base = y_range *.0025;

        color_specific_top = gray_y_value+color_height
        color_specific_bot = gray_y_value+color_base

        color_multi_dom_top = gray_y_value-color_base
        color_multi_dom_bot = gray_y_value-color_height


    #############


    ### Variants
    try:
        variant_y_value = color_top*0.97
    except:
        variant_y_value = color_specific_top*0.97

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save,box_select"

    ### Domain Rectangles
    top = []
    bottom = []
    left = []
    right = []
    legend = []
    if domain_type_count_val == 1:
        top.extend(repeat(color_top, len(domain_types)))
        bottom.extend(repeat(color_bot, len(domain_types)))
        for domain in domain_results:
            left.append(domain['domain.start'])
            right.append(domain['domain.end'])
            legend.append(domain['domain.name'])
    elif domain_type_count_val > 1:
        for domain in domain_results:
            left.append(domain['domain.start'])
            right.append(domain['domain.end'])
            legend.append(domain['domain.name'])
        for dtype in domain_types:
            if dtype == 'specific':
                top.append(color_specific_top)
                bottom.append(color_specific_bot)
            elif dtype == 'multi-dom':
                top.append(color_multi_dom_top)
                bottom.append(color_multi_dom_bot)

    unique_legend = list(set(legend))

    if vset in "prom":
        alphas = []
        alphanorms = [] # fill with normalized zscores between 0 and 1
        for domain in domain_results:
            alpha = float(domain['zscore'])
            alphas.append(str(alpha))
            alphanorm = (alpha-1.64)/(6.38-1.64) # 1.64,6.38 = min,max of all zscores
            if alphanorm < 0.1:
                alphanorm = 0.1 # alpha < 0.1 is not visible
            alphanorms.append(alphanorm)
        domain_source = ColumnDataSource(data=dict(
            top=top,bottom=bottom,left=left,right=right,legend=legend,zscore=alphas))
        domain_tooltips = [("(Start,End)","(@left, @right)"),
                           ("Name", "@legend"),
                           ("Zscore", "@zscore"),]

    else:
        colors = brewer["Set1"][9]
        colors_dicts = {}

        for idx, name in enumerate(unique_legend):
            colors_dicts[name] = colors[idx%9]

        colors = []
        for ele in legend:
            colors.append(colors_dicts[ele])

        domain_source = ColumnDataSource(data=dict(
                top=top,bottom=bottom,left=left,right=right,legend=legend,colors=colors,))
        domain_tooltips = [("(Start,End)","(@left, @right)"),
                               ("Name", "@legend"),]

    ### Figure
    p = figure(plot_width=1000, plot_height=600, title=title)
    ###

    # Find proper MES line and created related components accordingly
    mas_min = y.index(min(y))
    mas_max = y.index(max(y))
    mes_max_backwards = abs(max(y)-min(y[1:mas_max]))
    mes_min_forwards = abs((max(y[mas_min: ])) - min(y))

    # Largest MES starts at global minimum but does not end at global max
    if (mes_min_forwards > mes_max_backwards):
        dashed_lines = p.multi_line(
        #xs=[[mas_min*1.01,gene_length*1.01],[mas_max*1.01,gene_length*1.01]],
        #ys=[[min(y)*1.005,min(y)*1.005],[max(y)*1.005,max(y)*1.005]],
        xs=[[mas_min*1.01,gene_length*1.01],[y.index(max(y[mas_min: ]))+mas_min*1.01,gene_length*1.01]],
        ys=[[min(y)*1.005,min(y)*1.005],[max(y[mas_min: ]),max(y[mas_min: ])]],
        line_width=[1,1],
        line_color=['black','black'],
        line_dash='dashed',
        line_dash_offset=5)

        mes_line = p.add_layout(Arrow(
        start=VeeHead(fill_color='black',size=10),
        end=VeeHead(fill_color='black',size=10),
        line_width=1,
        x_start=gene_length*1.01,
        x_end=gene_length*1.01,
        #y_start=(min(y)+max(y[mas_min:]))/2,
        #y_end=max(y)))
        y_start=min(y),
        y_end=max(y[mas_min: ])))

        mes_text = p.add_layout(Label(
        text='MES',
        #text_font_size='10',
        x=gene_length*1.01,
        #y=max(y)-y_range/2,
        y=(min(y)+max(y[mas_min: ]))/2,
        angle=90,
        angle_units='deg',
        render_mode='css'))

    # Largest MES ends at global maximum
    else:
        dashed_lines = p.multi_line(
        xs=[[y.index(min(y[:mas_max]))*1.01,gene_length*1.01],[mas_max*1.01,gene_length*1.01]],
        ys=[[min(y[:mas_max]),min(y[:mas_max])],[max(y),max(y)]],
        line_width=[1,1],
        line_color=['black','black'],
        line_dash='dashed',
        line_dash_offset=5)

        mes_line = p.add_layout(Arrow(
        start=VeeHead(fill_color='black',size=10),
        end=VeeHead(fill_color='black',size=10),
        line_width=1,
        x_start=gene_length*1.01,
        x_end=gene_length*1.01,
        y_start=min(y[:mas_max]),
        y_end=max(y)))

        mes_text = p.add_layout(Label(
        text='MES',
        #text_font_size='10',
        x=gene_length*1.01,
        y=(max(y)+min(y[:mas_max]))/2,
        angle=90,
        angle_units='deg',
        render_mode='css'))


    ### MAS PLOT
    mas_line = p.line(x, y,line_width=1)
    ###


    gene_quad = p.quad(top=gray_top, bottom=gray_bot, left=0, right=gene_length,
                       color='gray')

    #tooltips=[("(Start,End)", "(@left, @right)"), ("Name", "@legend"),]
    #domain_tooltips = [("(Start,End)", "(@left, @right)"), ("Name", "@legend"),]

    if vset in 'prom':
        domain_quads = p.quad(top='top', bottom='bottom', left='left', right='right',
                              color='blue', alpha=alphanorms, source=domain_source)
    else:
        domain_quads = p.quad(top='top', bottom='bottom', left='left', right='right',
                              color='colors', source=domain_source)

    p.add_tools(HoverTool(renderers=[domain_quads], tooltips=domain_tooltips))

    ###
    keyDict = ['x', 'y', 'frequency', 'function', 'mutation_types']
    rects = dict([(key, []) for key in keyDict])
    for idx, (status, function) in enumerate(zip(mutation_types, exonic_functions)):
        color = None
        if status in 'SNP':
            color = '#1b9e77'
        elif status in 'INDEL':
            color = '#d95f02'

        x_value = frequencies_x[idx]
        frequency = frequencies[idx]
        rects['x'].append(x_value)
        rects['y'].append(variant_y_value)
        rects['frequency'].append(frequency)
        rects['function'].append(function)
        rects['mutation_types'].append(color)

    variant_tooltips = [("Location", "@x"),
                        ("Annotation", "@function"),
                        ("Frequency", "@frequency"),]

    hover_tools = []
    variant_rects = p.rect(x='x', y='y', width=1.0, height=7, color='mutation_types',
                           width_units='screen', height_units='screen', source=ColumnDataSource(data=rects))

    hover_tools.append(variant_rects)
    p.add_tools(HoverTool(renderers=hover_tools, tooltips=variant_tooltips))

    p.yaxis.axis_label = 'Mutation Accumulation Score (MAS)'
    p.yaxis.axis_label_text_font_style = "normal"

    if vset in "prom":
        p.xaxis.axis_label = 'Nucleotide Sequence'
        p.xaxis.axis_label_text_font_style = "normal"
    else:
        p.xaxis.axis_label = 'Amino Acid Sequence'
        p.xaxis.axis_label_text_font_style = "normal"

    output_folder = os.path.join(settings.BASE_DIR, 'static/bokeh_outputs')
    output_name = os.path.join(output_folder,"%s_%s_%s_%s.html" %(gene, rs_id, msea_type_name, vset))
    print('*'*20, output_name)
    save(p, output_name)

    return output_name
