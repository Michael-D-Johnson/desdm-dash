#! /usr/bin/env python
from flask import render_template
import get_data
import plotter
from app import app
from bokeh.embed import components
from bokeh.io import gridplot,vplot
from bokeh.resources import CDN
@app.route('/')
@app.route('/index')
def index():
    return render_template('home.html')

@app.route('/processing_summary')
def processing_summary():
    summary = get_data.processing_summary.delay('db-desoper','OPS')
    summary.wait()
    current_dict,rest_dict,columns,updated = summary.get()
    return render_template('processing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,updated=updated)

@app.route('/testing_summary')
def testing_summary():
    csum = get_data.processing_summary.delay('db-desoper','TEST')
    csum.wait()
    current_dict,rest_dict,columns,updated = csum.get()

    tsum = get_data.processing_summary.delay('db-destest','TEST')
    tsum.wait()
    tcurrent_dict,trest_dict,tcolumns,tupdated = tsum.get()

    return render_template('testing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,tcurrent_dict=tcurrent_dict,trest_dict=trest_dict,tcolumns=tcolumns,updated=updated)

@app.route('/processing_detail/<db>/<reqnum>')
def processing_detail(db,reqnum):
    detail = get_data.processing_detail.delay(db,reqnum)
    detail.wait()
    df,columns,reqnum,mean_times,updated = detail.get()
    filename = 'reports/plots_{reqnum}.html'.format(reqnum=reqnum)
    return render_template('processing_detail_include.html',columns=columns,df = df,
           reqnum=reqnum, mean_times=mean_times,db=db,updated = updated,filename=filename)

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
