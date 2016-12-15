from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.gzip import gzip_page
from datetime import datetime
from news.models import News

@gzip_page
def home(request):
    context = {}
    context['news'] = News.objects.order_by('-created')[:3]
    return render(request, 'home.html', context)


def change_password_done(request):
    return render(request, 'change_password_done.html', {})
