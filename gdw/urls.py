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
from django.contrib import admin
from django.urls import include, path

import core.views as core_views

urlpatterns = [
    path('', core_views.MainPageView.as_view(), name='home'),
    path('core/', include('core.urls')),
    path('complex/', include('complex.urls')),
    path('mendelian/', include('mendelian.urls')),
    path('microbiome/', include('microbiome.urls')),
    path('admin/', admin.site.urls),
    path('', include('django.contrib.auth.urls')),

]


# urlpatterns = [
#     url('^password_change/done/$', change_password_done),
#     url('^', include('django.contrib.auth.urls')),
#     url(
#         '^change-password/$',
#         auth_views.password_change,
#         {'template_name': 'change_password.html'}
#     ),
#     url(r'^admin/', admin.site.urls),
#     url(r'^$', core_views.Home, name='home'),
#     # url(r'^search/', include(search_urls)),
#     #url(r'^pybamview/', include(pybamview_urls)),
#     # url(r'^subject-report/', include(subject_report_urls)),
#     # url(r'^beacon/', include(beacon_urls)),
#     # url(r'^msea/', include(msea_urls)),
#     url(r'^igv/', include(igv_urls)),
#     # url(r'^inheritance-analysis/', include(inheritance_analysis_urls)),
#     url(r'^about/$', TemplateView.as_view(template_name="about.html"), name='about'),
#     url(r'^help/$', TemplateView.as_view(template_name="help.html"), name='help'),
#     url(r'^faq/$', TemplateView.as_view(template_name="faq.html"), name='faq'),
#     url(r'^dataset-summary/$',
#         TemplateView.as_view(template_name="dataset_summary.html"), name='dataset-summary'),
#     url(r'^tutorial/$',
#         TemplateView.as_view(template_name="tutorial.html"), name='tutorial'),
#     url(r'^tinymce/', include('tinymce.urls')),
# ]
