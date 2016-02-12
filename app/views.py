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
    all_dict,operator_list,columns = get_data.processing_summary('OPS')
    return render_template('processing_summary.html',columns=columns,dict_list=all_dict,operator_list=operator_list)

@app.route('/testing_summary')
def testing_summary():
    all_dict,operator_list,columns = get_data.processing_summary('TEST')
    return render_template('processing_summary.html',columns=columns,dict_list=all_dict,operator_list=operator_list)

@app.route('/processing_detail/<reqnum>')
def processing_detail(reqnum):
    df,columns,reqnum,mean_times = get_data.processing_detail(reqnum)
    tfigscript,tfigdiv=plotter.plot_times(df)
    figscript,figdiv=plotter.plot_accepted(df)

    return render_template('processing_detail.html',columns=columns,df = df,reqnum=reqnum,figdiv=figdiv,figscript=figscript,mean_times=mean_times,tfigscript=tfigscript,tfigdiv=tfigdiv)

@app.route('/dts')
def dts():
    return render_template('dts_monitor.html')

@app.route('/backups')
def backups():
    return render_template('back_ups.html')

@app.route('/supernova_summary')
def supernova_summary():
    return render_template('supernova_summary.html')
