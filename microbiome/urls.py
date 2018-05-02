from django.urls import include, path
import microbiome.views as microbiome_views


urlpatterns = (
    path('search', microbiome_views.MicrobiomeSearchView.as_view(), name='microbiome-search'),
)
