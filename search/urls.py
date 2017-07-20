from django.conf.urls import include, url
from .views import *

urlpatterns = (
    url(r'^$', search_home, name='search-home'),
    url(r'^variant/(?P<dataset_id>[0-9]+)/(?P<variant_id>.*)/$', get_variant, name='get-variant'),
    url(r'^get-study-form/$', get_study_form , name='get-study-form'),
    url(r'^get-dataset-form/$', get_dataset_form , name='get-dataset-form'),
    url(r'^get-filter-form/$', get_filter_form , name='get-filter-form'),
    url(r'^get-attribute-form/$', get_attribute_form , name='get-attribute-form'),
    url(r'^search$', search, name='search'),
    url(r'^download_result$', download_result, name='download-result'),
    url(r'^save_search/$', save_search , name='save-search'),
    url(r'^saved_search/$', SavedSearchList.as_view() , name='saved-search-list'),
    url(r'^(?P<pk>[0-9]+)$', retrieve_saved_search , name='retrieve-saved-search'),
    url(r'^saved_search/update/(?P<pk>[0-9]+)/$', SavedSearchUpdate.as_view(), name='saved-search-update'),
    url(r'^saved_search/delete/(?P<pk>[0-9]+)/$', SavedSearchDelete.as_view(), name='saved-search-delete'),
    url(r'^update-variant-review-status/$', update_variant_review_status, name='update-variant-review-status'),
    url(r'^list-variant-review-status/$', list_variant_review_status, name='list-variant-review-status'),
    url(r'^delete-variant/(?P<pk>[0-9]+)$', delete_variant, name='delete-variant'),
    url(r'^edit-variant/(?P<pk>[0-9]+)$', VariantReviewStatusUpdateView.as_view(), name='edit-variant'),
    # url(r'^list_user_variant_status/(?P<review_status>[\w-]+)/$', list_user_variant_status, name='list-user-variant-status'),
    url(r'^list_variant_status/(?P<review_status>[\w-]+)/$', list_variant_status, name='list-variant-status'),
)
