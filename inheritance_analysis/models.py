from django.db import models
from django.contrib.auth.models import User

from common.models import TimeStampedModel
from search.models import Dataset


class InheritanceAnalysisRequest(TimeStampedModel):

    ANALYSIS_STATUS_CHOICES = (
        ('Submitted', 'Submitted'),
        ('Processing', 'Processing'),
        ('Completed', 'Completed'),
        ('Failed', 'Failed'),
    )

    upload_user = models.ForeignKey(User, on_delete=models.CASCADE)
    dataset = models.OneToOneField(Dataset, on_delete=models.CASCADE)
    ped_json = models.TextField()
    analysis_status = models.CharField(
        max_length=16, choices=ANALYSIS_STATUS_CHOICES, default='Submitted')

    def __str__(self):
        return('PED file for %s' % (self.dataset.description))


class FamilyPedigree(TimeStampedModel):
    inheritance_analysis_request = models.ForeignKey(
        InheritanceAnalysisRequest, on_delete=models.CASCADE)
    family_id = models.CharField(max_length=64)
    father_id = models.CharField(max_length=64)
    mother_id = models.CharField(max_length=64)
    child_id = models.CharField(max_length=64)
