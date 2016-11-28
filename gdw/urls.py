"""gdw URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
from django.contrib import admin
import search.urls as search_urls
import pybamview.urls as  pybamview_urls
from .views import home
import subject_report.urls as subject_report_urls
import visualization.urls as visualization_urls

urlpatterns = [
    url('^', include('django.contrib.auth.urls')),
    url(r'^admin/', admin.site.urls),
    url(r'^$', home, name='home'),
    url(r'^search/', include(search_urls)),
    url(r'^pybamview/', include(pybamview_urls)),
    url(r'^subject-report/', include(subject_report_urls)),
    url(r'^visualization/', include(visualization_urls)),
]
