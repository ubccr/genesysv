from django.contrib import admin
from .models import *

class MseaDatasetAdmin(admin.ModelAdmin):
    list_display = ('display_name', 'dataset', 'is_public')


admin.site.register(MseaDataset, MseaDatasetAdmin)
