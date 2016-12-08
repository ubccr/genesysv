from django.conf.urls import url
from .views import msea_home, plots, search_gene_rs_id, get_variant_form, bokeh_plot

urlpatterns = (
    url(r'^$', msea_home, name='msea-home'),
    url(r'^plots/$', plots, name='plots'),
     url(r'^bokeh-plot/$', bokeh_plot, name='bokeh-plot'),
    url(r'^search-gene-rs-id/$', search_gene_rs_id, name='search-gene-rs-id'),
    url(r'^get-variant-form/$', get_variant_form, name='get-variant-form'),
)
