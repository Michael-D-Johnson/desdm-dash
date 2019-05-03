import os
from flask import Flask
import jinja2


app=Flask(__name__)
app.jinja_loader = jinja2.FileSystemLoader([os.getenv("TEMPLATES_PATH"),
                                            '/work/QA/',
                                            os.getenv("STATIC_PATH"),
                                          ])

from app import views
