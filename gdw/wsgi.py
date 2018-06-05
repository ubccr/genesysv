"""
WSGI config for gdw project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.10/howto/deployment/wsgi/
"""

import os
import sys

from django.core.wsgi import get_wsgi_application

sys.path.append('/data/www')

# add the virtualenv site-packages path to the sys.path
sys.path.append('/data/www/env/lib/python3.5/site-packages')


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gdw.settings")

application = get_wsgi_application()
