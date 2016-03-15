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
    all_dict,operator_list,columns = get_data.processing_summary('db-desoper','OPS')
    return render_template('processing_summary.html',columns=columns,dict_list=all_dict,operator_list=operator_list)

@app.route('/testing_summary')
def testing_summary():
    all_dict,operator_list,columns = get_data.processing_summary('db-desoper','TEST')
    tall_dict,toperator_list,tcolumns = get_data.processing_summary('db-destest','TEST')
    return render_template('testing_summary.html',columns=columns,dict_list=all_dict,operator_list=operator_list,tdict_list=tall_dict,toperator_list=toperator_list,tcolumns=tcolumns)

@app.route('/processing_detail/<db>/<operator>/<reqnum>')
def processing_detail(db,operator,reqnum):
    df,columns,reqnum,mean_times = get_data.processing_detail(db,operator,reqnum)
    
    try:
        tfigscript,tfigdiv=plotter.plot_times(df)
        figscript,figdiv=plotter.plot_accepted(df)
        #efigscript,efigdiv=plotter.plot_times_per_host(df)
    except:
        tfigscript,tfigdiv=None,None
        figscript,figdiv=None,None
    efigscript,efigdiv=None,None
    
    return render_template('processing_detail.html',columns=columns,df = df,reqnum=reqnum,figdiv=figdiv,figscript=figscript,mean_times=mean_times,tfigscript=tfigscript,tfigdiv=tfigdiv,db=db,operator=operator,efigscript=efigscript,efigdiv=efigdiv)

@app.route('/dts')
def dts():
    return render_template('dts_monitor.html')

@app.route('/backups')
def backups():
    return render_template('back_ups.html')

@app.route('/supernova_summary')
def supernova_summary():
    return render_template('supernova_summary.html')
