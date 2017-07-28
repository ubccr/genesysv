
from celery import Celery

app = Celery('es_celery',
             broker='amqp://jimmy:jimmy123@localhost/jimmy_vhost',
             backend='rpc://',
             include=['es_celery.tasks'])
