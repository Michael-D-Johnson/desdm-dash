import os
from flask import Flask
import jinja2


app=Flask(__name__)
app.config.from_pyfile("../settings.cfg")

app.jinja_loader = jinja2.FileSystemLoader([app.config["TEMPLATES_PATH"],
                                            '/work/QA/',
                                            app.config["STATIC_PATH"],
                                          ])

from app import views
