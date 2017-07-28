from django.db import models
from django.contrib.auth.models import Group
from common.models import TimeStampedModel


class SampleBamInfo(TimeStampedModel):
    dataset = models.ForeignKey(
        'search.Dataset',
        on_delete=models.CASCADE,
    )
    sample_id = models.CharField(max_length=255)
    file_path = models.CharField(max_length=255)

    def __str__(self):
        return "%s: %s" %(self.sample_id, self.file_path)
