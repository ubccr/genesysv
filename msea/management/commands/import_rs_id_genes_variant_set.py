''' Script to import initial static fos data.
'''

from django.core.management.base import BaseCommand
from msea.models import ReferenceSequence, Gene, VariantSet


class Command(BaseCommand):

    def handle(self, *args, **options):
        VARIANT_FILE_TYPE_CHOICES = (('ass', 'All Silent SNVs'),
                         ('asbns', 'All Silent Benign non-Silent SNVs'),
                         ('ansi', 'All non-Silent SNVs and Indels'),
                         ('ans', 'All non-Silent SNVs' ),
                         ('dnsi', 'Deleterious non-Silent SNVs and Indels'),
                         ('dns', 'Deleterious non-Silent SNVs'),
                         ('prom', 'Variants in Promoter Region'),
                    )

        for short_name, full_name in VARIANT_FILE_TYPE_CHOICES:
            VariantSet.objects.get_or_create(short_name=short_name, full_name=full_name)

        with open('./msea/management/commands/data/mohammad_gene_info.txt', 'r') as fp:
            for idx, line in enumerate(fp):
                if line.startswith('#') or not line.strip():
                    continue

                line_split = line.strip().split()
                gene_name = line_split[0].strip()
                rs_id = line_split[1].strip()
                variants = line_split[2:]
                if (idx % 1000) == 0:
                    print(idx, line_split)
                gene_object, _ = Gene.objects.get_or_create(gene_name=gene_name)


                rs_object, _ = ReferenceSequence.objects.get_or_create(rs_id=rs_id, gene=gene_object)
                for variant in variants:
                    var_obj = VariantSet.objects.get(short_name=variant.strip())
                    rs_object.variants.add(var_obj)

                rs_object.save()


