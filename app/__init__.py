from flask import Flask
import jinja2 
from celery import Celery
import redis

app=Flask(__name__)
app.jinja_loader = jinja2.FileSystemLoader(['/work/devel/mjohns44/git/desdm-dash/app/templates',
                                            '/work/QA/'])



from app import views
