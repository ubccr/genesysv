from django.conf.urls import include, url
from .views import *

urlpatterns = [
    url(r'^$', trio_home, name='trio-home'),
    url(r'^pedigree-analysis-request/list/$', pedigree_analysis_request_list, name='pedigree-analysis-request-list'),
    url(r'^pedigree-analysis-request/delete/(?P<pk>[0-9]+)/$', PedigreeAnalysisRequestDelete.as_view(), name='pedigree-analysis-request-delete'),
]
