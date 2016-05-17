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
    df2 = df.dropna()
    df_pass = df[df.status==0].dropna()
    df_teff = df_pass
    df_teff = df_teff.t_eff.replace(0,'None')
    plots = []
    try:
        times = plotter.plot_times.delay(df_pass)
        times.wait()
        #times_figscript,times_figdiv= times.get()
        plots.append(times.get())
    except:
        #times_figscript,times_figdiv=(None,None)
        pass
    try:
        assess = plotter.plot_accepted.delay(df_pass)
        assess.wait()
        #assess_figscript,assess_figdiv = assess.get() 
        plots.append(assess.get())
    except:
        #assess_figscript,assess_figdiv=(None,None)
        pass
    try:
        exechost = plotter.plot_exec_time.delay(df_pass)
        exechost.wait()
        #exechost_figscript,exechost_figdiv = exechost.get()
        plots.append(exechost.get())
    except:
        #exechost_figscript,exechost_figdiv = (None,None)
        pass
    try:
        fails = plotter.plot_status_per_host.delay(df2)
        fails.wait()
        #fails_figscript,fails_figdiv = list(fails.collect())
        plots.append(fails.get())
    except:
        pass
        #fails_figscript,fails_figdiv=(None,None) 
    try:
        teff =plotter.plot_t_eff.delay(df_teff[(df_teff.t_eff !='None')])
        teff.wait()
        plots.append(teff.get())
        #teff_figscript,teff_figdiv = teff.get()
    except:
        pass
        #teff_figscript,teff_figdiv = (None,None)
    figscript,figdiv = components(vplot(*plots))
    return render_template('processing_detail.html',columns=columns,df = df,
           reqnum=reqnum, mean_times=mean_times,db=db,updated = updated,
           figscript=figscript,figdiv=figdiv)
"""
           assess_figscript=assess_figscript,assess_figdiv=assess_figdiv,
           times_figscript=times_figscript,times_figdiv=times_figdiv,
           exechost_figscript=exechost_figscript,exechost_figdiv=exechost_figdiv,
           fails_figscript=fails_figscript,fails_figdiv=fails_figdiv,
           teff_figscript=teff_figscript,teff_figdiv=teff_figdiv
"""

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
