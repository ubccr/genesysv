from django.contrib import admin

from .models import News


class NewsAdmin(admin.ModelAdmin):
    list_display = ('title', 'content', 'created', 'modified')

admin.site.register(News, NewsAdmin)
