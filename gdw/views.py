from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.gzip import gzip_page
from datetime import datetime

def home(request):
    return render(request, 'home.html', {})
