from django.urls import include, path

from mendelian.views import (FamilySnippetView, MendelianHomeView,
                             MendelianSearchView)

urlpatterns = (
    path('<int:study_id>', MendelianHomeView.as_view(), name='mendelian-home'),
    path('family-snippet/<int:dataset_id>', FamilySnippetView.as_view(), name='family-snippet'),
    path('search', MendelianSearchView.as_view(), name='mendelian-search'),
)
