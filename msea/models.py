from django.db import models
from django.contrib.auth.models import User
from common.models import TimeStampedModel



class VariantSet(TimeStampedModel):
    full_name = models.CharField(max_length=128)
    short_name = models.CharField(max_length=32)

    def __str__(self):
        return self.full_name

class Gene(TimeStampedModel):
    gene_name = models.CharField(max_length=32)

    def __str__(self):
        return self.gene_name

class ReferenceSequence(TimeStampedModel):
    rs_id = models.CharField(max_length=12)
    gene = models.ForeignKey(Gene, on_delete=models.CASCADE)
    variants = models.ManyToManyField(VariantSet)

    def __str__(self):
        return self.rs_id
