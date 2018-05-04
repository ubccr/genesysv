from django.contrib import admin
from microbiome.models import (DownloadRequest)


@admin.register(DownloadRequest)
class DownloadRequestAdmin(admin.ModelAdmin):
    list_display = ('pk', 'user', 'search_log', 'pi', 'reason', 'contact_email', 'status',)
