from django.db import models
from tinymce.models import HTMLField
from common.models import TimeStampedModel
from django.contrib.auth.models import User


class News(TimeStampedModel):
    title = models.CharField(max_length = 150)
    content = HTMLField()

    class Meta:
        verbose_name_plural = 'News'
