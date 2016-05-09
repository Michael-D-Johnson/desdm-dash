from flask import Flask
import jinja2 
from celery import Celery
import redis

app=Flask(__name__)
#app.jinja_loader = jinja2.FileSystemLoader(['/work/devel/mjohns44/git/desdm-dash/app/templates',
#                                            '/work/QA/'])

# Initialize redis
app.redis = redis.StrictRedis(host='127.0.0.1',port= 6379, db=0)

# Initialize Celery
app.config['CELERY_BROKER_URL']='redis://127.0.0.1:6739/0',
app.config['BROKER_URL'] = 'redis://127.0.0.1:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://127.0.0.1:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

from app import views
