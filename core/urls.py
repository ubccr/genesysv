from django.urls import include, path

import core.views as core_views

urlpatterns = (
    path('dataset-snippet/<int:study_id>',
         core_views.DatasetSnippetView.as_view(), name='dataset-snippet'),
    path('analysis-type-snippet/<int:dataset_id>',
         core_views.AnalysisTypeSnippetView.as_view(), name='analysis-type-snippet'),
    path('filter-snippet/<int:dataset_id>',
         core_views.FilterSnippetView.as_view(), name='filter-snippet'),
    path('attribute-snippet/<int:dataset_id>',
         core_views.AttributeSnippetView.as_view(), name='attribute-snippet'),
    path('search-router/', core_views.SearchRouterView.as_view(), name='search-router'),
    path('download-router/<int:search_log_id>', core_views.DownloadRouterView.as_view(), name='download-router'),
    path('additional-form-router/<int:dataset_id>/<int:analysis_type_id>', core_views.AdditionalFormRouterView.as_view(), name='additional-form-router'),
    path('base-search/', core_views.BaseSearchView.as_view(), name='base-search'),
    path('base-download/<int:search_log_id>', core_views.BaseDownloadView.as_view(), name='base-download'),
    path('save-search/', core_views.save_search, name='save-search'),
    path('saved-search-list/', core_views.SavedSearchListView.as_view(), name='saved-search-list'),
    path('retrieve-saved-search/<int:saved_search_id>', core_views.RetrieveSavedSearchView.as_view(), name='retrieve-saved-search'),
    path('core-document-view/<int:dataset_id>/<document_id>/', core_views.BaseDocumentView.as_view(), name='core-document-view'),
    path('core-document-review-create/<int:dataset_id>/<document_id>/create', core_views.DocumentReviewCreateView.as_view(), name='core-document-review-create'),
    path('core-document-review-update/<int:dataset_id>/<document_id>/update', core_views.DocumentReviewUpdateView.as_view(), name='core-document-review-update'),
)
