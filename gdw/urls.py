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
from .views import home, change_password_done
import subject_report.urls as subject_report_urls
import visualization.urls as visualization_urls
import beacon.urls as beacon_urls
import msea.urls as msea_urls
import igv.urls as igv_urls
import inheritance_analysis.urls as inheritance_analysis_urls
from django.views.generic.base import TemplateView
from django.contrib.auth import views as auth_views
from django.views.generic import RedirectView


urlpatterns = [
    url('^password_change/done/$', change_password_done),
    url('^', include('django.contrib.auth.urls')),
    url(
        '^change-password/$',
        auth_views.password_change,
        {'template_name': 'change_password.html'}
    ),
    url(r'^admin/', admin.site.urls),
    url(r'^$', home, name='home'),
    url(r'^search/', include(search_urls)),
    url(r'^pybamview/', include(pybamview_urls)),
    url(r'^subject-report/', include(subject_report_urls)),
    url(r'^visualization/', include(visualization_urls)),
    url(r'^beacon/', include(beacon_urls)),
    url(r'^msea/', include(msea_urls)),
    url(r'^igv/', include(igv_urls)),
    url(r'^inheritance-analysis/', include(inheritance_analysis_urls)),
    url(r'^about/$', TemplateView.as_view(template_name="about.html"), name='about'),
    url(r'^help/$', TemplateView.as_view(template_name="help.html"), name='help'),
    url(r'^faq/$', TemplateView.as_view(template_name="faq.html"), name='faq'),
    url(r'^dataset-summary/$', TemplateView.as_view(template_name="dataset_summary.html"), name='dataset-summary'),
    url(r'^tutorial/$', TemplateView.as_view(template_name="tutorial.html"), name='tutorial'),
    url(r'^tinymce/', include('tinymce.urls')),
]
