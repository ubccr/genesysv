from django.conf.urls import url
from .views import *

urlpatterns = (
    url(r'^$', msea_home, name='msea-home'),
    url(r'^msea-pvalue/$', msea_pvalue, name='msea-pvalue'),
    url(r'^get-plot/$', get_plot, name='get-plot'),
    url(r'^msea-plot/$', msea_plot, name='msea-plot'),
    url(r'^search-gene-rs-id/$', search_gene_rs_id, name='search-gene-rs-id'),
    url(r'^get-variant-form/$', get_variant_form, name='get-variant-form'),
    url(r'^download-tiff/$', download_tiff, name='download-tiff'),
)
