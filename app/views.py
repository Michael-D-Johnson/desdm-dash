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
    reqnums = sorted(get_data.processing_archive(),reverse=True)
    return render_template('processing_archive.html',reqnums = reqnums,title='Processing Archive')

@app.route('/processing_summary')
def processing_summary():
    current_dict,rest_dict,columns,updated = get_data.processing_summary('db-desoper','OPS')
    return render_template('processing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,updated=updated,title='Processing Summary')

@app.route('/testing_summary')
def testing_summary():
    current_dict,rest_dict,columns,updated = get_data.processing_summary('db-desoper','TEST')
    tcurrent_dict,trest_dict,tcolumns,tupdated = get_data.processing_summary('db-destest','TEST')

    return render_template('testing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,tcurrent_dict=tcurrent_dict,trest_dict=trest_dict,tcolumns=tcolumns,updated=updated,title='Testing Summary')

@app.route('/processing_detail/<reqnum>')
def processing_detail(reqnum):
    reportname = 'reports/{reqnum}/report_{reqnum}.html'.format(reqnum=reqnum)
    return render_template('processing_detail_include.html',reportname=reportname,title='Report {reqnum}'.format(reqnum=reqnum))

@app.route('/dts')
def dts():
    return render_template('dts_monitor.html',title='DTS')

@app.route('/backups')
def backups():
    return render_template('back_ups.html',title='Backups')

@app.route('/supernova_summary')
def supernova_summary():
    return render_template('supernova_summary.html',title = 'Supernova Summary')

@app.route('/coadd_map')
def coadd_map():
    all_df, processed_df = get_data.create_coadd_map('db-destest',"ME_TEST")
    p = plotter.plot_coadd(all_df, processed_df, "ME_TEST")
    script,div = components(p)
    return render_template('coadd_map.html', scripts=script, div=div,title = 'Coadd Map')
