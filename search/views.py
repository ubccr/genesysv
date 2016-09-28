from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.gzip import gzip_page
from datetime import datetime

def search_home(request):
    return HttpResponse('test')
