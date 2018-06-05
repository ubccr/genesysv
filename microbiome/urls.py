from django.urls import include, path

import microbiome.views as microbiome_views

urlpatterns = (
    path('search', microbiome_views.MicrobiomeSearchView.as_view(), name='microbiome-search'),
    path('otu-download/<int:search_log_id>', microbiome_views.OtuDownloadView.as_view(), name='otu-download'),
    path('request-download/<int:search_log_id>', microbiome_views.request_download, name='request-download'),
    path('download-request-list', microbiome_views.DownloadRequestListView.as_view(), name='download-request-list'),
    path('download-request-review-list', microbiome_views.DownloadRequestReviewListView.as_view(), name='download-request-review-list'),
    path('approve-download-request/<int:download_request_id>', microbiome_views.approve_download_request, name='download-requested-data'),
    path('deny-download-request/<int:download_request_id>', microbiome_views.deny_download_request, name='download-requested-data'),
)
