from django.conf.urls import url
from .views import *

urlpatterns = (
    url(r'^$', msea_home, name='msea-home'),
    url(r'^plots/$', plots, name='plots'),
     url(r'^bokeh-plot/$', bokeh_plot, name='bokeh-plot'),
    url(r'^search-gene-rs-id/$', search_gene_rs_id, name='search-gene-rs-id'),
    url(r'^get-variant-form/$', get_variant_form, name='get-variant-form'),
    url(r'^download-svg/$', download_svg, name='download-svg'),
)
