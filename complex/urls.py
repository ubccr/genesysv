from django.urls import include, path
import complex.views as complex_views


urlpatterns = (
    path('search', complex_views.ComplexSearchView.as_view(), name='complex-search'),
)
