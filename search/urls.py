from django.conf.urls import include, url
from .views import *

urlpatterns = (
    url(r'^$', search_home, name='search-home'),
)
