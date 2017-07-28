from django.conf.urls import url
from .views import *

urlpatterns = (
    url(r'^$', beacon, name='beacon'),
    url(r'^get-beacon-form/$', get_beacon_form, name='get-beacon-form'),
    url(r'^beacon-query$', beacon_query, name='beacon-query'),
)
