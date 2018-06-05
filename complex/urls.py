from django.urls import include, path

import complex.views as complex_views

urlpatterns = (
    path('complex-search', complex_views.ComplexSearchView.as_view(), name='complex-search'),
    path('complex-document-view/<int:dataset_id>/<document_id>/',
         complex_views.ComplexDocumentView.as_view(), name='complex-document-view'),
)
