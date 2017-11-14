from django.conf.urls import include, url
from .views import *

urlpatterns = [
    url(r'^$', trio_home, name='trio-home'),
    url(r'^$', trio_dbdata, name='trio-dbdata'),
]
