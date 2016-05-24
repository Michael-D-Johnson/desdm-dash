from flask import Flask
import jinja2 

app=Flask(__name__)
#app.jinja_loader = jinja2.FileSystemLoader(['/work/devel/mjohns44/git/desdm-dash/app/templates',
#                                            '/work/QA/'])

app.jinja_loader = jinja2.FileSystemLoader(['/work/devel/mjohns44/git/desdm-dash/app/templates',
                                            '/work/QA/',
                                            '/Users/mjohns44/GIT_DESDM/desdm-dash-mjohns44/desdm-dash/app/templates'])

from app import views
