from django.contrib import admin
from .models import *


class SampleBamInfoAdmin(admin.ModelAdmin):
    list_display = ('sample_id', 'file_path')


admin.site.register(SampleBamInfo, SampleBamInfoAdmin)
