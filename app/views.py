#! /usr/bin/env python
from flask import render_template
import get_data
import plotter
from app import app

@app.route('/')
@app.route('/index')
def index():
    return render_template('home.html')

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
    df,columns,reqnum,mean_times,updated = get_data.processing_detail(db,reqnum)
    df2 = df.dropna()
    df_pass = df[df.status==0].dropna()
    df_teff = df_pass.t_eff.replace(0,-.00001)
    df.t_eff = df_teff

    try:
        times_figscript,times_figdiv=plotter.plot_times(df_pass)
    except:
        times_figscript,times_figdiv=None,None
    try:
        assess_figscript,assess_figdiv=plotter.plot_accepted(df_pass)
    except:
        assess_figscript,assess_figdiv=None,None
    try:
        exechost_figscript,exechost_figdiv= plotter.plot_exec_time(df_pass)
    except:
        exechost_figscript,exechost_figdiv=None,None
    try:
        fails_figscript,fails_figdiv = plotter.plot_status_per_host(df2)
    except:
        fails_figscript,fails_figdiv=None,None 

    try:
        teff_figscript,teff_figdiv = plotter.plot_t_eff(df_pass[(df_pass.t_eff !='None')])
    except:
        teff_figscript,teff_figdiv = (None,None)
    return render_template('processing_detail.html',columns=columns,df = df,
           reqnum=reqnum, mean_times=mean_times,db=db,updated = updated,
           assess_figdiv=assess_figdiv,assess_figscript=assess_figscript,
           times_figscript=times_figscript,times_figdiv=times_figdiv,
           exechost_figscript=exechost_figscript,exechost_figdiv=exechost_figdiv,
           fails_figscript=fails_figscript,fails_figdiv=fails_figdiv,
           teff_figscript=teff_figscript,teff_figdiv=teff_figdiv)

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
