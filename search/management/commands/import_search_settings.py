from django.core.management.base import BaseCommand
from search.models import *


FORM_TYPES = ("CharField", "ChoiceField", "MultipleChoiceField")

WIDGET_TYPES = ("TextInput", "Select", "SelectMultiple", "Textarea")

ES_FILTER_TYPES = ( "must_term",
                    "should_term",
                    "nested_must_term",
                    "nested_should_term",
                    "must_wildcard",
                    "nested_must_wildcard",
                    "must_range_gte",
                    "must_range_lte",
                    "must_range_lt",
                    "nested_must_range_gte",
                    "exist",
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
    "intergenic",
    "intronic",
    "ncRNA_exonic",
    "ncRNA_exonic;splicing",
    "ncRNA_intronic",
    "ncRNA_splicing",
    "ncRNA_UTR5",
    "splicing",
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
    "intergenic",
    "intronic",
    "ncRNA_exonic",
    "ncRNA_exonic;splicing",
    "ncRNA_intronic",
    "ncRNA_splicing",
    "ncRNA_UTR5",
    "splicing",
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


AVNSP142_EXIST_CHOICES = ("only", "excluded")


SAMPLE_CHOICES = (
    "1344",
    "1394",
    "1406",
    "1409",
    "2003",
    "2017",
    "2021",
    "2023",
    "2024",
    "2042",
    "2048",
    "2052",
    "2059",
    "2063",
    "2066",
    "2072",
    "2074",
    "2077",
    "2078",
    "2087",
    "2102",
    "2134",
    "2142",
    "2145",
    "2152",
    "2155",
    "2157",
    "2161",
    "2162",
    "2166",
    "2168",
    "2170",
    "2171",
    "2173",
    "2179",
    "2181",
    "2184",
    "2185",
    "2193",
    "2194",
    "2196",
    "2198",
    "2208",
    "2214",
    "2221",
    "2239",
    "2257",
    "2261",
    "2269",
    "2517",
)

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
            ('sim_wgs',
             'Static Induced Myopathy WGS',
             'sim3',
             'wgs_hg19_multianno',
             'http://199.109.195.45:9200/'),
        )

        for name, description in studies:
            Study.objects.get_or_create(name=name,
                                        description=description)

        study_obj = Study.objects.get(name='Statin Induced Myopathy')

        for name, description, es_index_name, es_type_name, es_host in datasets:
            dataset_object, _ = Dataset.objects.get_or_create(study=study_obj,
                                          name=name,
                                          description=description,
                                          es_index_name=es_index_name,
                                          es_type_name=es_type_name,
                                          es_host=es_host,
                                          is_public=True)


        for name in FORM_TYPES:
            FormType.objects.get_or_create(name=name)

        for name in WIDGET_TYPES:
            WidgetType.objects.get_or_create(name=name)

        for name in ES_FILTER_TYPES:
            ESFilterType.objects.get_or_create(name=name)

        dataset_object = Dataset.objects.get(name='sim_wgs')


        with open('./search/management/commands/data/sim_WGS_filter.txt','r') as fp:
            for line in fp:
                if line.startswith("#"):
                    continue

                print(line)
                row = [ele.strip() for ele in line.split('\t')]
                es_name, display_name, tooltip, form_type, widget_type, es_filter_type, path = row

                if path == 'None':
                    path = ''

                form_type_obj = FormType.objects.get(name=form_type)
                widget_type_obj = WidgetType.objects.get(name=widget_type)
                es_filter_type_obj = ESFilterType.objects.get(name=es_filter_type)

                FilterField.objects.get_or_create(
                                            dataset=dataset_object,
                                            es_name=es_name,
                                            display_name=display_name,
                                            tooltip=tooltip,
                                            form_type=form_type_obj,
                                            widget_type=widget_type_obj,
                                            es_filter_type=es_filter_type_obj,
                                            path=path
                                            )


                if display_name in 'Sample ID':
                    sample_id_obj = FilterField.objects.get(dataset=dataset_object, display_name="Sample ID")
                    for choice in SAMPLE_CHOICES:
                        FilterFieldChoice.objects.get_or_create(filter_field=sample_id_obj, value=choice)

                if display_name in 'Genotype':
                    genotype_obj = FilterField.objects.get(dataset=dataset_object, display_name="Genotype")
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



        ### create tabs
        dataset_object = Dataset.objects.get(name='sim_wgs')

        filter_tab_obj, _ = FilterTab.objects.get_or_create(dataset=dataset_object, name='Simple')

        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Variant Related Information')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Functional Consequence')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Gene/Transcript')
        FilterPanel.objects.get_or_create(filter_tab=filter_tab_obj, name='Pathogenic Prediction')



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

        attribute_tab_obj, _ = AttributeTab.objects.get_or_create(dataset=dataset_object, name='Simple')

        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Variant Related Information')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Functional Consequence')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Gene/Transcript')
        AttributePanel.objects.get_or_create(attribute_tab=attribute_tab_obj, name='Pathogenic Prediction')



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


