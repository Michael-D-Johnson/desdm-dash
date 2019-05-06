#! /usr/bin/env python
from flask import render_template
import query
import get_data
import plotter
import pandas as pd
from datetime import datetime
from app import app
from bokeh.embed import components

@app.route('/desdm-dash')
def index():
    return render_template('home.html')

@app.route('/desdm-dash/processing_archive')
def processing_archive():
    reqnums = sorted(get_data.processing_archive(),reverse=True)
    return render_template('processing_archive.html',reqnums = reqnums,title='Processing Archive')

@app.route('/desdm-dash/processing_summary')
def processing_summary():
    current_dict,rest_dict,columns,updated,curr_df,rest_df = get_data.processing_summary('desoper','OPS')
    return render_template('processing_summary.html',columns=columns,current_dict=current_dict,rest_dict=rest_dict,updated=updated,title='Processing Summary')

@app.route('/desdm-dash/testing_summary')
def testing_summary():
    current_dict,rest_dict,columns,updated,curr_df,rest_df = get_data.processing_summary('desoper','TEST')
    tcurrent_dict,trest_dict,tcolumns,tupdated,tcurr_df,trest_df = get_data.processing_summary('destest','TEST')

    if len(curr_df) > 0:
        curr_df = curr_df[curr_df.db=='desoper']
    if len(rest_df) > 0:
        rest_df = rest_df[rest_df.db=='desoper']
    if len(tcurr_df) > 0:
        tcurr_df = tcurr_df[tcurr_df.db=='destest']
    if len(trest_df) > 0:
        trest_df = trest_df[trest_df.db=='destest']
    return render_template('testing_summary.html',curr_df = curr_df, trest_df = trest_df,rest_df = rest_df, tcurr_df = tcurr_df, columns=columns,current_dict=current_dict,rest_dict=rest_dict,tcurrent_dict=tcurrent_dict,trest_dict=trest_dict,tcolumns=tcolumns,updated=updated,title='Testing Summary')

@app.route('/desdm-dash/processing_detail/<reqnum>')
def processing_detail(reqnum):
    reportname = 'reports/{reqnum}/report_{reqnum}.html'.format(reqnum=reqnum)
    return render_template('processing_detail_include.html',reportname=reportname,title='Report {reqnum}'.format(reqnum=reqnum))


@app.route('/desdm-dash/error_summary/<db>/<reqnum>')
def error_summary(db,reqnum):
    try:
        if db=='desoper' or db=='decade':
            message_dict = query.query_task_messages(query.connect_to_db(db)[1], reqnum)
        else:
            message_dict = query.query_qcf_messages(query.connect_to_db(db)[1], reqnum)
        error_dict = get_data.find_errors(message_dict)
        plot = plotter.plot_qcf_bar(error_dict, reqnum)
        script, div = components(plot)
        return render_template('error_summary.html', scripts=script, div=div, columns=[i for i in error_dict.iterkeys()], error_dict=error_dict, updated='#{time}'.format(time=datetime.now()), title='Error Summary')
    except TypeError:
        # If there was no failures for the run
        print("No errors were found for reqnum {}, no failure graph was produced".format(reqnum))
        return render_template('error_summary.html', scripts=None, div=None, columns=None, error_dict={}, updated='#{time}'.format(time=datetime.now()), title='Error Summary')
