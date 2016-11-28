from django.conf.urls import include, url
from .views import *

urlpatterns = (
    url(r'^$', viz_home, name='viz-home'),
)
