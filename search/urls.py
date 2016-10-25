from django.conf.urls import include, url
from .views import *

urlpatterns = (
    url(r'^$', search_home, name='search-home'),
    url(r'^get-study-form/$', get_study_form , name='get-study-form'),
    url(r'^get-dataset-form/$', get_dataset_form , name='get-dataset-form'),
    url(r'^get-filter-form/$', get_filter_form , name='get-filter-form'),
    url(r'^get-attribute-form/$', get_attribute_form , name='get-attribute-form'),
    url(r'^search_old_home$', search_old_home, name='search-old-home'),
    # url(r'^result$', search_result, name='search-result'),
    url(r'^results$', search_result, name='search-result'),
     url(r'^result_download$', search_result_download, name='search-result-download'),
)
