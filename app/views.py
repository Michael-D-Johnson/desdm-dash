#! /usr/bin/env python
from flask import render_template
import get_data
import plotter
from app import app
from bokeh.embed import components
from bokeh.io import vplot

@app.route('/')
@app.route('/index')
def index():
    return render_template('home.html')

@app.route('/processing_archive')
def processing_archive():
    reqnums = get_data.processing_archive().sorted()
    return render_template('processing_archive.html',reqnums = reqnums)

@app.route('/processing_summary')
def processing_summary():
    current_dict,rest_dict,columns,updated = get_data.processing_summary('db-desoper','OPS')
    return render_template('processing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,updated=updated)

@app.route('/testing_summary')
def testing_summary():
    current_dict,rest_dict,columns,updated = get_data.processing_summary('db-desoper','TEST')
    tcurrent_dict,trest_dict,tcolumns,tupdated = get_data.processing_summary('db-destest','TEST')

    return render_template('testing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,tcurrent_dict=tcurrent_dict,trest_dict=trest_dict,tcolumns=tcolumns,updated=updated)

@app.route('/processing_detail/<db>/<reqnum>')
def processing_detail(db,reqnum):
    reportname = 'reports/{reqnum}/{reqnum}_report.html'.format(reqnum=reqnum)
    return render_template('processing_detail_include.html',db = db,reportname=reportname)

@app.route('/teff')
def teff():
    return render_template('teff.html')

@app.route('/dts')
def dts():
    return render_template('dts_monitor.html')

@app.route('/backups')
def backups():
    return render_template('back_ups.html')

@app.route('/supernova_summary')
def supernova_summary():
    return render_template('supernova_summary.html')
