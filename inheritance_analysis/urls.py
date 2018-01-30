from django.conf.urls import include, url
from .views import *

urlpatterns = [
    url(r'^$', InheritanceAnalysisRequestListView.as_view(),
        name='inheritance-analysis-request-list'),
    url(r'^create/$', inheritance_analysis_request_create,
        name='inheritance-analysis-request-create'),
    url(r'^delete/(?P<pk>[0-9]+)/$', InheritanceAnalysisRequestDelete.as_view(),
        name='inheritance-analysis-request-delete'),
]
