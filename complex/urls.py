from django.urls import include, path
from complex.views import ComplexHomeView, ComplexSearchView
from core.views import FilterSnippetView

urlpatterns = (
    path('<int:study_id>', ComplexHomeView.as_view(), name='complex-home'),
    path('search', ComplexSearchView.as_view(), name='complex-search'),
)
