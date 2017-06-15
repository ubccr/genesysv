
from celery import Celery

app = Celery('import_celery',
             broker='amqp://jimmy:jimmy123@localhost/jimmy_vhost',
             backend='rpc://',
             include=['import_celery.tasks'])
