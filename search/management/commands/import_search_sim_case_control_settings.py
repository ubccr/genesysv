from django.core.management.base import BaseCommand
from search.models import *
from pybamview.models import SampleBamInfo


FORM_TYPES = ("CharField", "ChoiceField", "MultipleChoiceField")

WIDGET_TYPES = ("TextInput", "Select", "SelectMultiple", "Textarea", "UploadField")

ES_FILTER_TYPES = ( "filter_term",
                    "filter_terms",
                    "nested_filter_term",
                    "nested_filter_terms",
                    "filter_range_gte",
                    "filter_range_gt",
                    "filter_range_lte",
                    "filter_range_lt",
                    "nested_filter_range_gte",
                    "filter_exists",
                    "must_not_exists",
                    "nested_filter_exists",
)

CHR_CHOICES = (
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "X",
    "Y",
    "MT",
)

FUNC_ENSGENE_CHOICES = (
    "exonic;splicing",
    "frameshift_deletion",
    "frameshift_insertion",
    "intergenic",
    "intronic",
    "ncRNA_exonic",
    "ncRNA_exonic;splicing",
    "ncRNA_intronic",
    "ncRNA_splicing",
    "ncRNA_UTR5",
    "nonframeshift_deletion",
    "nonframeshift_insertion",
    "nonsynonymous_SNV",
    "splicing",
    "stopgain",
    "stoploss",
    "synonymous_SNV",
    "unknown",
    "upstream",
    "upstream;downstream",
    "UTR3",
    "UTR5",
    "UTR5;UTR3",
)

FUNC_REFGENE_CHOICES = (
    "downstream",
    "exonic",
    "exonic;splicing",
    "frameshift_deletion",
    "frameshift_insertion",
    "intergenic",
    "intronic",
    "ncRNA_exonic",
    "ncRNA_exonic;splicing",
    "ncRNA_intronic",
    "ncRNA_splicing",
    "ncRNA_UTR5",
    "nonframeshift_deletion",
    "nonframeshift_insertion",
    "nonsynonymous_SNV",
    "splicing",
    "stopgain",
    "stoploss",
    "synonymous_SNV",
    "unknown",
    "upstream",
    "upstream;downstream",
    "UTR3",
    "UTR5",
    "UTR5;UTR3",
)



EXONICFUNC_ENSGENE = (
    "frameshift_deletion",
    "frameshift_insertion",
    "nonframeshift_deletion",
    "nonframeshift_insertion",
    "nonsynonymous_SNV",
    "stopgain",
    "stoploss",
    "synonymous_SNV",
    "unknown",
)

EXONICFUNC_REFGENE = (
    "frameshift_deletion",
    "frameshift_insertion",
    "nonframeshift_deletion",
    "nonframeshift_insertion",
    "nonsynonymous_SNV",
    "stopgain",
    "stoploss",
    "synonymous_SNV",
    "unknown",
)



TYPE_CHOICES = ("SNV", "INDEL")

CLINSIG_CHOICES = ( "unknown",
                    "untested",
                    "non-pathogenic",
                    "probable-non-pathogenic",
                    "probable-pathogenic",
                    "pathogenic",
                    "drug-response",
                    "histocompatibility",
                    "other",
            )


EXIST_CHOICES = ("only", "excluded")

GENOTYPE_CHOICES = (
    '1/1',
    '1/0',
    '0/1',
)


class Command(BaseCommand):



    def handle(self, *args, **options):

        studies = (
            ('Statin Induced Myopathy', ''),
        )
        datasets = (
            # ('sim_wgs_one_node_cluster',
            #  'Static Induced Myopathy WGS (1-node Cluster)',
            #  'http://199.109.192.242:9200/',
            #  'sim',
            #  'wgs_hg19_multianno'),
            ('sim_case_control',
             'SIM Case (WGS) and Control (WES), GRCh37/hg19',
             'sim',
             'SIM_case_control',
             '199.109.195.58',
             '9200'),
        )

        for name, description in studies:
            Study.objects.get_or_create(name=name,
                                        description=description)

        study_obj = Study.objects.get(name='Statin Induced Myopathy')

        for name, description, es_index_name, es_type_name, es_host, es_port in datasets:
            dataset_object, _ = Dataset.objects.get_or_create(study=study_obj,
                                          name=name,
                                          description=description,
                                          es_index_name=es_index_name,
                                          es_type_name=es_type_name,
                                          es_host=es_host,
                                          es_port=es_port,
                                          is_public=True)



        SearchOptions.objects.get_or_create(dataset=dataset_object)

        for name in FORM_TYPES:
            FormType.objects.get_or_create(name=name)

        for name in WIDGET_TYPES:
            WidgetType.objects.get_or_create(name=name)

        for name in ES_FILTER_TYPES:
            ESFilterType.objects.get_or_create(name=name)

        dataset_object = Dataset.objects.get(name='sim_case_control')

        # Load case/control read depth and variant count and store in database
        with open('./search/management/commands/data/case_control_rd_v_count.txt','r') as fp:
            for line in fp:
                if line.strip():
                    sample_id, rd_10x, rd_15x, rd_20x, rd_40x, rd_50x, variant_count = line.split(',')
                    SampleReadDepth.objects.get_or_create(dataset=dataset_object,
                                                          sample_id=sample_id,
                                                          rd_10x=rd_10x,
                                                          rd_15x=rd_15x,
                                                          rd_20x=rd_20x,
                                                          rd_40x=rd_40x,
                                                          rd_50x=rd_50x,
                                                          variant_count=variant_count)

        with open('./search/management/commands/data/sim_WGS_filter.txt','r') as fp:
            for line in fp:
                print(line)
                if line.startswith("#"):
                    continue

                print(line)
                row = [ele.strip() for ele in line.split('\t')]
                es_name, display_name, in_line_tooltip, tooltip, default_value, form_type, widget_type, es_filter_type, path = row

                if path == 'None':
                    path = ''

                form_type_obj = FormType.objects.get(name=form_type)
                widget_type_obj = WidgetType.objects.get(name=widget_type)
                es_filter_type_obj = ESFilterType.objects.get(name=es_filter_type)

                FilterField.objects.get_or_create(
                                            dataset=dataset_object,
                                            es_name=es_name.strip(),
                                            display_name=display_name.strip(),
                                            in_line_tooltip=in_line_tooltip.strip(),
                                            tooltip=tooltip.strip(),
                                            default_value=default_value.strip(),
                                            form_type=form_type_obj,
                                            widget_type=widget_type_obj,
                                            es_filter_type=es_filter_type_obj,
                                            path=path
                                            )


                if display_name in 'Sample ID':
                    sample_id_obj = FilterField.objects.get(dataset=dataset_object, display_name=display_name)
                    for choice in open('./search/management/commands/data/wgs_case_ids.txt','r'):
                        FilterFieldChoice.objects.get_or_create(filter_field=sample_id_obj, value=choice.strip())
                        file_path = "/gpfs/projects/academic/big/SIM/WGS_case/hc_bam/SIMWGS09292016_p%s_merged_hc.bam" %(choice.strip())
                        print(file_path)
                        a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                                        sample_id=choice.strip(),
                                                        file_path=file_path)
                        print(a)

                    for choice in open('./search/management/commands/data/wes_control_ids.txt','r'):
                        FilterFieldChoice.objects.get_or_create(filter_field=sample_id_obj, value=choice.strip())
                        file_path = "/gpfs/projects/academic/big/SIM/WES_control/hc_bam/SIMCT20160930_%s_merged_hc.bam" %(choice.strip())
                        print(file_path)
                        a, b = SampleBamInfo.objects.get_or_create(dataset=dataset_object,
                                                        sample_id=choice.strip(),
                                                        file_path=file_path)

                        print(a)
                if display_name in 'Sample Cohort Label':
                    sample_cohort_obj = FilterField.objects.get(dataset=dataset_object, display_name=display_name)
                    for choice in ["case", "control"]:
                        FilterFieldChoice.objects.get_or_create(filter_field=sample_cohort_obj, value=choice)

                if display_name in 'Genotype':
                    genotype_obj = FilterField.objects.get(dataset=dataset_object, display_name=display_name)
                    for choice in GENOTYPE_CHOICES:
                        FilterFieldChoice.objects.get_or_create(filter_field=genotype_obj, value=choice)


        with open('./search/management/commands/data/sim_WGS_attribute.txt','r') as fp:
            for line in fp:
                if line.startswith("#"):
                    continue

                print(line)
                row = [ele.strip() for ele in line.split('\t')]
                es_name, display_name, path = row

                if path == 'None':
                    path = ''

                AttributeField.objects.get_or_create(dataset=dataset_object,
                                                     es_name=es_name,
                                                     display_name=display_name,
                                                     path=path
                                            )

        chr_obj = FilterField.objects.get(dataset=dataset_object, es_name="Chr")
        for choice in CHR_CHOICES:
            FilterFieldChoice.objects.get_or_create(filter_field=chr_obj, value=choice)

        Func_ensGene_obj = FilterField.objects.get(dataset=dataset_object, es_name="Func_ensGene")
        for choice in FUNC_ENSGENE_CHOICES:
            FilterFieldChoice.objects.get_or_create(filter_field=Func_ensGene_obj, value=choice)

        Func_refGene_obj = FilterField.objects.get(dataset=dataset_object, es_name="Func_refGene")
        for choice in FUNC_REFGENE_CHOICES:
            FilterFieldChoice.objects.get_or_create(filter_field=Func_refGene_obj, value=choice)

        ExonicFunc_ensGene_obj = FilterField.objects.get(dataset=dataset_object, es_name="ExonicFunc_ensGene")
        for choice in EXONICFUNC_ENSGENE:
            FilterFieldChoice.objects.get_or_create(filter_field=ExonicFunc_ensGene_obj, value=choice)

        ExonicFunc_refGene_obj = FilterField.objects.get(dataset=dataset_object, es_name="ExonicFunc_refGene")
        for choice in EXONICFUNC_REFGENE:
            FilterFieldChoice.objects.get_or_create(filter_field=ExonicFunc_refGene_obj, value=choice)


        Type_obj = FilterField.objects.get(dataset=dataset_object, es_name="Type")
        for choice in TYPE_CHOICES:
            FilterFieldChoice.objects.get_or_create(filter_field=Type_obj, value=choice)

        CLINSIG_obj = FilterField.objects.get(dataset=dataset_object, es_name="clinvar_20150629_CLINSIG")
        for choice in CLINSIG_CHOICES:
            FilterFieldChoice.objects.get_or_create(filter_field=CLINSIG_obj, value=choice)



        cytoBand = FilterField.objects.get(dataset=dataset_object, es_name="cytoBand")
        for choice in open('./search/management/commands/data/cytoBand.txt','r'):
            FilterFieldChoice.objects.get_or_create(filter_field=cytoBand, value=choice.strip())

        FATHMM_MKL_pred = FilterField.objects.get(dataset=dataset_object, es_name="fathmm_MKL_coding_pred")
        for choice in ["Neutral", "Deleterious"]:
            FilterFieldChoice.objects.get_or_create(filter_field=FATHMM_MKL_pred, value=choice)

        FATHMM_pred = FilterField.objects.get(dataset=dataset_object, es_name="FATHMM_pred")
        for choice in ["Tolerated", "Deleterious"]:
            FilterFieldChoice.objects.get_or_create(filter_field=FATHMM_pred, value=choice)

        LRT_pred = FilterField.objects.get(dataset=dataset_object, es_name="LRT_pred")
        for choice in ["Deleterious", "Neutral", "Unknown"]:
            FilterFieldChoice.objects.get_or_create(filter_field=LRT_pred, value=choice)

        LR_pred = FilterField.objects.get(dataset=dataset_object, es_name="LR_pred")
        for choice in ["Tolerated", "Deleterious"]:
            FilterFieldChoice.objects.get_or_create(filter_field=LR_pred, value=choice)

        MetaLR_pred = FilterField.objects.get(dataset=dataset_object, es_name="MetaLR_pred")
        for choice in ["Tolerated", "Deleterious"]:
            FilterFieldChoice.objects.get_or_create(filter_field=MetaLR_pred, value=choice)

        MetaSVM_pred = FilterField.objects.get(dataset=dataset_object, es_name="MetaSVM_pred")
        for choice in ["Tolerated", "Deleterious"]:
            FilterFieldChoice.objects.get_or_create(filter_field=MetaSVM_pred, value=choice)

        MutationAssessor_pred = FilterField.objects.get(dataset=dataset_object, es_name="MutationAssessor_pred")
        for choice in ["High", "Low", "Medium", "Neutral"]:
            FilterFieldChoice.objects.get_or_create(filter_field=MutationAssessor_pred, value=choice)

        MutationTaster_pred = FilterField.objects.get(dataset=dataset_object, es_name="MutationTaster_pred")
        for choice in ["disease_causing_automatic", "disease_causing", "polymorphism", "polymorphism_automatic"]:
            FilterFieldChoice.objects.get_or_create(filter_field=MutationTaster_pred, value=choice)

        PROVEAN_pred = FilterField.objects.get(dataset=dataset_object, es_name="PROVEAN_pred")
        for choice in ["Deleterious", "Neutral"]:
            FilterFieldChoice.objects.get_or_create(filter_field=PROVEAN_pred, value=choice)

        Polyphen2_HDIV_pred = FilterField.objects.get(dataset=dataset_object, es_name="Polyphen2_HDIV_pred")
        for choice in ["benign", "possibly_damaging", "probably_damaging"]:
            FilterFieldChoice.objects.get_or_create(filter_field=Polyphen2_HDIV_pred, value=choice)

        Polyphen2_HVAR_pred = FilterField.objects.get(dataset=dataset_object, es_name="Polyphen2_HVAR_pred")
        for choice in ["benign", "possibly_damaging", "probably_damaging"]:
            FilterFieldChoice.objects.get_or_create(filter_field=Polyphen2_HVAR_pred, value=choice)

        RadialSVM_pred = FilterField.objects.get(dataset=dataset_object, es_name="RadialSVM_pred")
        for choice in ["Tolerated", "Deleterious"]:
            FilterFieldChoice.objects.get_or_create(filter_field=RadialSVM_pred, value=choice)

        SIFT_pred = FilterField.objects.get(dataset=dataset_object, es_name="SIFT_pred")
        for choice in ["Tolerated", "Deleterious"]:
            FilterFieldChoice.objects.get_or_create(filter_field=SIFT_pred, value=choice)


        for es_name in [
                        'avsnp142',
                        'snp138NonFlagged',
                        'wgRna',
                        'cosmic70',
                        'genomicSuperDups',
                        'phastConsElements100way',
                        'phastConsElements46way',
                        'targetScanS',
                        'tfbsConsSites',
                        'gwasCatalog'
                    ]:
            filter_field = FilterField.objects.get(dataset=dataset_object, es_name=es_name, es_filter_type__name='filter_exists')
            for choice in EXIST_CHOICES:
                FilterFieldChoice.objects.get_or_create(filter_field=filter_field, value=choice)


        for es_name in ['clinvar_20150629_CLNDBN',]:
            filter_field = FilterField.objects.get(dataset=dataset_object, es_name=es_name, es_filter_type__name='nested_filter_exists')
            for choice in EXIST_CHOICES:
                FilterFieldChoice.objects.get_or_create(filter_field=filter_field, value=choice)

        for es_name in ['AC_',]:
            filter_field = FilterField.objects.get(dataset=dataset_object, es_name=es_name, es_filter_type__name='must_not_exists')
            for choice in ["case", "control"]:
                FilterFieldChoice.objects.get_or_create(filter_field=filter_field, value=choice)


        ### create tabs
        dataset_object = Dataset.objects.get(name='sim_case_control')

        filter_tab_obj, _ = FilterTab.objects.get_or_create(dataset=dataset_object, name='Simple')

        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Variant Related Information')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Cohort Information')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Functional Consequence')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Gene/Transcript')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Pathogenic Prediction')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Conserved/Constrained Genomic Elements/Positions')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Minor Allele Frequency (MAF)')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Disease Association')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Patient/Sample Related Information')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='microRNA/snoRNA')



        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Case/Control')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='Case')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='Control')


        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Gene/Transcript')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='RefSeq Identifier')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='Ensembl Identifier')


        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Pathogenic Prediction')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='Coding Region')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='Splice Junction')

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Minor Allele Frequency (MAF)')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='The Exome Aggregation Consortium (ExAC)')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='1000 Genomes Project (Aug. 2015)')

        FilterSubPanel.objects.get_or_create(filter_panel=filter_panel_obj,
                                     name='Exome Sequencing Project (ESP6500siv2)')

        attribute_tab_obj, _ = AttributeTab.objects.get_or_create(dataset=dataset_object, name='Simple')

        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Variant Related Information')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Case/Control')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Functional Consequence')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Gene/Transcript')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Pathogenic Prediction')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Conserved/Constrained Genomic Elements/Positions')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Minor Allele Frequency (MAF)')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Disease Association')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Patient/Sample Related Information')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='microRNA/snoRNA')




        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                                         attribute_tab__name='Simple',
                                                         name='Case/Control')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                                name='Case')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                     name='Control')


        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                                         attribute_tab__name='Simple',
                                                         name='Gene/Transcript')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                                name='RefSeq Identifier')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                     name='Ensembl Identifier')


        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                                         attribute_tab__name='Simple',
                                                         name='Pathogenic Prediction')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                             name='Coding Region')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                                name='Splice Junction')


        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                                         attribute_tab__name='Simple',
                                                         name='Minor Allele Frequency (MAF)')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                             name='The Exome Aggregation Consortium (ExAC)')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                             name='1000 Genomes Project (Aug. 2015)')

        AttributeSubPanel.objects.get_or_create(attribute_panel=attribute_panel_obj,
                                                name='Exome Sequencing Project (ESP6500siv2)')



       ##
        filter_panel = (
            ('Variant', 'filter_term'),
            ('Chr', 'filter_terms'),
            ('Start', 'filter_term'),
            ('Ref', 'filter_term'),
            ('Alt', 'filter_term'),
            ('Type', 'filter_term'),
            ('cytoBand', 'filter_terms'),
            ('qs', 'nested_filter_range_gte'),
            ('avsnp142', 'filter_terms'),
            ('avsnp142', 'filter_exists')
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Variant Related Information')
        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)


        filter_panel = (
            ('AC_', 'must_not_exists'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Cohort Information')

        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)



        filter_panel = (
            ('AC_case', 'filter_range_gte'),
            ('AC_case', 'filter_range_lte'),
            ('AN_case', 'filter_range_gte'),
            ('AN_case', 'filter_range_lte'),
            ('AF_case', 'filter_range_gte'),
            ('AF_case', 'filter_range_lte'),
        )

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='Case')

        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)


        filter_panel = (
            ('AC_control', 'filter_range_gte'),
            ('AC_control', 'filter_range_lte'),
            ('AN_control', 'filter_range_gte'),
            ('AN_control', 'filter_range_lte'),
            ('AF_control', 'filter_range_gte'),
            ('AF_control', 'filter_range_lte'),
        )

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='Control')

        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)

       ##
        filter_panel = (
            ('Func_refGene', 'filter_terms'),
            ('Func_ensGene', 'filter_terms'),
            ('ExonicFunc_refGene', 'filter_terms'),
            ('ExonicFunc_ensGene', 'filter_terms'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Functional Consequence')
        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)


        ##
        filter_panel = (
            ('tfbsConsSites', 'filter_exists'),
            ('gerp_plus_gt2', 'filter_range_gte'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Conserved/Constrained Genomic Elements/Positions')
        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)

       ##
        filter_panel = (
            ('refGene_symbol', 'nested_filter_terms'),
            ('refGene_refgene_id', 'nested_filter_terms'),
            ('refGene_cDNA_change', 'nested_filter_term'),
            ('refGene_aa_change', 'nested_filter_term'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Gene/Transcript')

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='RefSeq Identifier')

        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)

       ##
        filter_panel = (
            ('ensGene_gene_id', 'nested_filter_terms'),
            ('ensGene_transcript_id', 'nested_filter_terms'),
            ('ensGene_cDNA_change', 'nested_filter_term'),
            ('ensGene_aa_change', 'nested_filter_term'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Gene/Transcript')

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='Ensembl Identifier')
        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)

       ##
        filter_panel = (
            ('FATHMM_pred', 'filter_term'),
            ('fathmm_MKL_coding_pred', 'filter_term'),
            ('LRT_pred', 'filter_terms'),
            ('LR_pred', 'filter_term'),
            ('MetaLR_pred', 'filter_term'),
            ('MetaSVM_pred', 'filter_term'),
            ('MutationAssessor_pred', 'filter_terms'),
            ('MutationTaster_pred', 'filter_terms'),
            ('PROVEAN_pred', 'filter_term'),
            ('Polyphen2_HDIV_pred', 'filter_terms'),
            ('Polyphen2_HVAR_pred', 'filter_terms'),
            ('RadialSVM_pred', 'filter_term'),
            ('SIFT_pred', 'filter_term'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Pathogenic Prediction')

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='Coding Region')


        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)


       ##
        filter_panel = (
            ('dbscSNV_ADA_SCORE', 'filter_range_gte'),
            ('dbscSNV_RF_SCORE', 'filter_range_gte'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Pathogenic Prediction')

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='Splice Junction')


        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)



        ##
        filter_panel = (
            ('ExAC_ALL', 'filter_range_lt'),
            ('ExAC_AFR', 'filter_range_lt'),
            ('ExAC_AMR', 'filter_range_lt'),
            ('ExAC_EAS', 'filter_range_lt'),
            ('ExAC_FIN', 'filter_range_lt'),
            ('ExAC_NFE', 'filter_range_lt'),
            ('ExAC_OTH', 'filter_range_lt'),
            ('ExAC_SAS', 'filter_range_lt'),
        )



        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Minor Allele Frequency (MAF)')

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='The Exome Aggregation Consortium (ExAC)')


        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)


        ##
        filter_panel = (
            ('1000g2015aug_all', 'filter_range_lt'),
            ('1000g2015aug_afr', 'filter_range_lt'),
            ('1000g2015aug_amr', 'filter_range_lt'),
            ('1000g2015aug_eur', 'filter_range_lt'),
            ('1000g2015aug_sas', 'filter_range_lt'),
        )

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='1000 Genomes Project (Aug. 2015)')


        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)




        ##
        filter_panel = (
            ('esp6500siv2_all', 'filter_range_lt'),
            ('esp6500siv2_aa', 'filter_range_lt'),
            ('esp6500siv2_ea', 'filter_range_lt'),
        )

        filter_sub_panel_obj = FilterSubPanel.objects.get(filter_panel=filter_panel_obj,
                                     name='Exome Sequencing Project (ESP6500siv2)')

        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_sub_panel_obj.filter_fields.add(filter_field_obj)



         ##
        filter_panel = (
            ('gwasCatalog', 'filter_exists'),
            ('clinvar_20150629_CLNDBN', 'nested_filter_exists'),
            #('gwasCatalog', 'filter_term'),
            #('clinvar_20150629_CLNDBN', 'nested_filter_term'),
            ('clinvar_20150629_CLINSIG', 'nested_filter_terms'),
            ('snp138NonFlagged', 'filter_exists'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Disease Association')


        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)


         ##
        filter_panel = (
            ('sample_ID', 'nested_filter_terms'),
            ('sample_GT', 'nested_filter_terms'),
            ('sample_DP', 'nested_filter_range_gte'),
            ('sample_cohort', 'nested_filter_term'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='Patient/Sample Related Information')


        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)




        ##
        filter_panel = (
            ('wgRna', 'filter_exists'),
            ('targetScanS', 'filter_exists'),
        )

        filter_panel_obj = FilterPanel.objects.get(filter_tab__dataset=dataset_object,
                                     filter_tab__name='Simple',
                                     name='microRNA/snoRNA')


        for es_name, es_filter_type in filter_panel:
            print(es_name, es_filter_type)
            filter_field_obj = FilterField.objects.get(dataset=dataset_object,
                                                       es_name=es_name, es_filter_type__name=es_filter_type)
            filter_panel_obj.filter_fields.add(filter_field_obj)


       ##
        attribute_panel = (
            ('Variant', ''),
            ('Chr', ''),
            ('Start', ''),
            ('Ref', ''),
            ('Alt', ''),
            ('Type', ''),
            ('cytoBand', ''),
            ('qs', 'gatkQS'),
            ('avsnp142', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Variant Related Information')

        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_panel_obj.attribute_fields.add(attribute_field_obj)



       ##
        attribute_panel = (
            ('AC_case', ''),
            ('AN_case', ''),
            ('AF_case', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Cohort Information')

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='Case')

        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)


       ##
        attribute_panel = (
            ('AC_control', ''),
            ('AN_control', ''),
            ('AF_control', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Cohort Information')

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='Control')

        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)



       ##
        attribute_panel = (
            ('Func_refGene', ''),
            ('Func_ensGene', ''),
            ('ExonicFunc_refGene', ''),
            ('ExonicFunc_ensGene', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Functional Consequence')
        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_panel_obj.attribute_fields.add(attribute_field_obj)


       ##
        attribute_panel = (
            ('gerp_plus_gt2', ''),
            ('phastConsElements46way', ''),
            ('phastConsElements100way', ''),
            ('tfbsConsSites', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Conserved/Constrained Genomic Elements/Positions')

        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_panel_obj.attribute_fields.add(attribute_field_obj)


       ##
        attribute_panel = (
            ('refGene_symbol', 'refGene'),
            ('refGene_refgene_id', 'refGene'),
            ('refGene_cDNA_change', 'refGene'),
            ('refGene_aa_change', 'refGene'),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Gene/Transcript')

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='RefSeq Identifier')

        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)

       ##
        attribute_panel = (
            ('ensGene_gene_id', 'ensGene'),
            ('ensGene_transcript_id', 'ensGene'),
            ('ensGene_cDNA_change', 'ensGene'),
            ('ensGene_aa_change', 'ensGene'),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Gene/Transcript')

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='Ensembl Identifier')
        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)

       ##
        attribute_panel = (
            ('FATHMM_pred', ''),
            ('fathmm_MKL_coding_pred', ''),
            ('LRT_pred', ''),
            ('LR_pred', ''),
            ('MetaLR_pred', ''),
            ('MetaSVM_pred', ''),
            ('MutationAssessor_pred', ''),
            ('MutationTaster_pred', ''),
            ('PROVEAN_pred', ''),
            ('Polyphen2_HDIV_pred', ''),
            ('Polyphen2_HVAR_pred', ''),
            ('RadialSVM_pred', ''),
            ('SIFT_pred', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Pathogenic Prediction')

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='Coding Region')


        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)


       ##
        attribute_panel = (
            ('dbscSNV_ADA_SCORE', ''),
            ('dbscSNV_RF_SCORE', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Pathogenic Prediction')

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='Splice Junction')


        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)



        ##
        attribute_panel = (
            ('ExAC_ALL', ''),
            ('ExAC_AFR', ''),
            ('ExAC_AMR', ''),
            ('ExAC_EAS', ''),
            ('ExAC_FIN', ''),
            ('ExAC_NFE', ''),
            ('ExAC_OTH', ''),
            ('ExAC_SAS', ''),
        )



        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Minor Allele Frequency (MAF)')

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='The Exome Aggregation Consortium (ExAC)')


        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)


        ##
        attribute_panel = (
            ('1000g2015aug_all', ''),
            ('1000g2015aug_afr', ''),
            ('1000g2015aug_amr', ''),
            ('1000g2015aug_eur', ''),
            ('1000g2015aug_sas', ''),
        )

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='1000 Genomes Project (Aug. 2015)')


        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)




        ##
        attribute_panel = (
            ('esp6500siv2_all', ''),
            ('esp6500siv2_aa', ''),
            ('esp6500siv2_ea', ''),
        )

        attribute_sub_panel_obj = AttributeSubPanel.objects.get(attribute_panel=attribute_panel_obj,
                                     name='Exome Sequencing Project (ESP6500siv2)')

        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_sub_panel_obj.attribute_fields.add(attribute_field_obj)



         ##
        attribute_panel = (
            ('gwasCatalog', ''),
            ('clinvar_20150629_CLNDBN', 'clinvar_20150629'),
            ('clinvar_20150629_CLINSIG', 'clinvar_20150629'),
            ('snp138NonFlagged', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Disease Association')


        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_panel_obj.attribute_fields.add(attribute_field_obj)


         ##
        attribute_panel = (
            ('sample_ID', 'sample'),
            ('sample_GT', 'sample'),
            ('sample_DP', 'sample'),
            ('sample_cohort', 'sample'),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='Patient/Sample Related Information')


        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_panel_obj.attribute_fields.add(attribute_field_obj)


        ##
        attribute_panel = (
            ('wgRna', ''),
            ('targetScanS', ''),
        )

        attribute_panel_obj = AttributePanel.objects.get(attribute_tab__dataset=dataset_object,
                                     attribute_tab__name='Simple',
                                     name='microRNA/snoRNA')


        for es_name, path in attribute_panel:
            print(es_name, path)
            attribute_field_obj = AttributeField.objects.get(dataset=dataset_object,
                                                             es_name=es_name, path=path)
            attribute_panel_obj.attribute_fields.add(attribute_field_obj)
