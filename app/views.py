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
    current_dict,rest_dict,columns = get_data.processing_summary('db-desoper','OPS')
    return render_template('processing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict)

@app.route('/testing_summary')
def testing_summary():
    current_dict,rest_dict,columns = get_data.processing_summary('db-desoper','TEST')
    tcurrent_dict,trest_dict,tcolumns = get_data.processing_summary('db-destest','TEST')
    return render_template('testing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,tcurrent_dict=tcurrent_dict,trest_dict=trest_dict,tcolumns=tcolumns)

@app.route('/processing_detail/<db>/<operator>/<reqnum>')
def processing_detail(db,operator,reqnum):
    df,columns,reqnum,mean_times = get_data.processing_detail(db,operator,reqnum)
    
    try:
        times_figscript,times_figdiv=plotter.plot_times(df)
        assess_figscript,assess_figdiv=plotter.plot_accepted(df)
        exechost_figscript,exechost_figdiv= plotter.plot_exec_time(df)
    except:
        times_figscript,times_figdiv=None,None
        assess_figscript,assess_figdiv=None,None
        exechost_figscript,exechost_figdiv=None,None
    
    return render_template('processing_detail.html',columns=columns,df = df,reqnum=reqnum,assess_figdiv=assess_figdiv,assess_figscript=assess_figscript,mean_times=mean_times,times_figscript=times_figscript,times_figdiv=times_figdiv,db=db,operator=operator,exechost_figscript=exechost_figscript,exechost_figdiv=exechost_figdiv)

@app.route('/dts')
def dts():
    return render_template('dts_monitor.html')

@app.route('/backups')
def backups():
    return render_template('back_ups.html')

@app.route('/supernova_summary')
def supernova_summary():
    return render_template('supernova_summary.html')
