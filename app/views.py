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
    current_dict,rest_dict,columns,updated,curr_df,rest_df = get_data.processing_summary('db-desoper','OPS')
    return render_template('processing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,updated=updated,title='Processing Summary')

@app.route('/testing_summary')
def testing_summary():
    current_dict,rest_dict,columns,updated,curr_df,rest_df = get_data.processing_summary('db-desoper','TEST')
    tcurrent_dict,trest_dict,tcolumns,tupdated,tcurr_df,trest_df = get_data.processing_summary('db-destest','TEST')

    if len(curr_df) > 0:
        curr_df = curr_df[curr_df.db=='db-desoper']
    if len(rest_df) > 0:
        rest_df = rest_df[rest_df.db=='db-desoper']
    if len(tcurr_df) > 0:
        tcurr_df = tcurr_df[tcurr_df.db=='db-destest']
    if len(trest_df) > 0:
        trest_df = trest_df[trest_df.db=='db-destest']
    return render_template('testing_summary.html',curr_df = curr_df, trest_df = trest_df,rest_df = rest_df, tcurr_df = tcurr_df, columns=columns,current_dict=current_dict,rest_dict=rest_dict,tcurrent_dict=tcurrent_dict,trest_dict=trest_dict,tcolumns=tcolumns,updated=updated,title='Testing Summary')

@app.route('/processing_detail/<reqnum>')
def processing_detail(reqnum):
    reportname = 'reports/{reqnum}/report_{reqnum}.html'.format(reqnum=reqnum)
    return render_template('processing_detail_include.html',reportname=reportname,title='Report {reqnum}'.format(reqnum=reqnum))

@app.route('/dts')
def dts():
    return render_template('dts_monitor.html',title='DTS')

@app.route('/system')
def backups():
    sys_df, res, desdf_df = get_data.create_system_data('db-destest','db-databot')
    plots = make_system_plots(sys_df, res, desdf_df)
    script,div = components(plots)
    return render_template('system.html', scripts=script, div=div, title='System')

@app.route('/supernova_summary')
def supernova_summary():
    return render_template('supernova_summary.html',title = 'Supernova Summary')

@app.route('/coadd_map')
def coadd_map():
    name = 'reports/coadd/coadd_map_save.html'
    return render_template('coadd_map.html', name=name, title='Coadd Map')
