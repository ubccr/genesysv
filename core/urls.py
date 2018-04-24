from django.urls import include, path
from core.views import AttributeSnippetView, FilterSnippetView

urlpatterns = (
    path('filter-snippet/<int:dataset_id>',
         FilterSnippetView.as_view(), name='filter-snippet'),
    path('attribute-snippet/<int:dataset_id>',
         AttributeSnippetView.as_view(), name='attribute-snippet'),
)
