from django.db import models
from common.models import TimeStampedModel
from django.contrib.auth.models import User
from search.models import Dataset


class PedigreeAnalysisRequest(TimeStampedModel):

    ANALYSIS_STATUS_CHOICES = (
        ('submitted', 'Submitted'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    )

    upload_user = models.ForeignKey(User,on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset,on_delete=models.CASCADE)
    ped_json = models.TextField()
    analysis_status = models.CharField(max_length=16, choices=ANALYSIS_STATUS_CHOICES, default='submitted')

    def __str__(self):
        return('PED file for %s'  %(self.dataset.description))


class PedigreeTrio(TimeStampedModel):
    pedigree_analysis_request = models.ForeignKey(PedigreeAnalysisRequest,on_delete=models.CASCADE)
    family_id = models.CharField(max_length=64)
    father_id = models.CharField(max_length=64)
    mother_id = models.CharField(max_length=64)
    child_id = models.CharField(max_length=64)
