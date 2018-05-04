from django.db import models
from common.models import TimeStampedModel
from django.contrib.auth.models import User, Group

import core


class DownloadRequest(TimeStampedModel):
    STATUS_CHOICES = (
        ('Approved', 'Approved'),
        ('Denied', 'Denied'),
        ('Pending', 'Pending'),
    )

    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
    )
    search_log = models.ForeignKey(
        core.models.SearchLog,
        on_delete=models.CASCADE,
    )
    pi = models.CharField(max_length=256, verbose_name='Principle investigator')
    reason = models.TextField()
    contact_email = models.EmailField()
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='Pending')

    class Meta:
        permissions = (
            ("can_download_results_without_request", "Can download results without request"),
            ("can_download_OTU_without_request", "Can download OTU without request"),
            ("can_approve_download_request", "Can approve download request"),
            ("can_view_all_download_requests", "Can view all download requests"),
        )

