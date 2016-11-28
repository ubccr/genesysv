from django.conf.urls import include, url
from .views import SubjectReportWizard

urlpatterns = (
    url(r'^$', SubjectReportWizard.as_view(), name='subject-report'),
)
