from django.shortcuts import render
from django.views.decorators.gzip import gzip_page


@gzip_page
def viz_home(request):
    return render(request, "visualization/viz_home.html", {})

