from django.urls import include, path

import mendelian.views as mendelian_views

urlpatterns = (
    path('<int:study_id>', mendelian_views.MendelianHomeView.as_view(), name='mendelian-home'),
    path('family-snippet/<int:dataset_id>', mendelian_views.FamilySnippetView.as_view(), name='family-snippet'),
    path('mendelian-search', mendelian_views.MendelianSearchView.as_view(), name='mendelian-search'),
    path('mendelian-download/<int:search_log_id>', mendelian_views.MendelianDownloadView.as_view(), name='mendelian-download'),
    path('mendelian-document-view/<int:dataset_id>/<document_es_id>/',
         mendelian_views.MendelianDocumentView.as_view(), name='mendelian-document-view'),
)
