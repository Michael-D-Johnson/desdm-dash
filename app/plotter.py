#!/usr/bin/env python
import os
from bokeh.resources import CDN
from bokeh.embed import components,file_html
from bokeh.plotting import figure,ColumnDataSource
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool,
                          glyphs,Legend,NumeralTickFormatter)
from bokeh.palettes import Spectral6,PuOr9
from bokeh.charts import Scatter,Bar,Histogram,color
from bokeh.io import vplot,hplot,gridplot

def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Histogram(dataframe,'total time',bins=24, color = 'green',
        title="Processing Times",height=500,width=1000)
    figscript,figdiv = components(p)

    #return (figscript,figdiv)
    return p

def plot_accepted(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, label='program',values='program', agg='count', group='assessment',
        title="Accepted by Program", legend='top_right',height=500,width=1000)

    figscript,figdiv = components(p)
    #return (figscript,figdiv)
    return p

def plot_exec_time(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, values='total time', label='exec_host', agg='mean', color='blue',
        title='Mean Processing Time for Each Exec Host',height=500,width=1000)
    figscript,figdiv = components(p)
    #return (figscript,figdiv)
    return p

def plot_status_per_host(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    mycolors = ['green','red','blue','orange','yellow','purple','black']
    p = Bar(dataframe, label='exec_host',values='exec_host', agg='count', group='status',
        title="Status per Exec Host", legend='top_right',color=color('status', palette=mycolors),
        height=500,width=1000)

    figscript,figdiv = components(p)
    #return (figscript,figdiv)
    return p

def plot_t_eff(dataframe):
    def create_data_source(df):
        return ColumnDataSource(data=dict(t_eff=df['t_eff'],expnum=df['expnum'],program=df['program'],accepted=df['assessment'],attnum=df['attnum'],band=df['band']))
    plots = []
    df_false = dataframe[dataframe['assessment']=='False']
    df_true = dataframe[dataframe['assessment']=='True']
    p = figure(tools = [HoverTool(tooltips = [('expnum','@expnum'),('band','@band'),('program', '@program'),('teff','@t_eff'),('accepted','@assessment'),('attempt','@attnum')]),BoxZoomTool(),ResetTool(),WheelZoomTool()], x_axis_label = "t_eff", y_axis_label = "expnum", title = 't_eff',width=1000,height=500)
    p.scatter('expnum','t_eff',source=create_data_source(df_false),fill_color='red',line_color='white',alpha=0.5)
    p.scatter('expnum','t_eff',source=create_data_source(df_true),fill_color='green',line_color='white',alpha=0.5)

    p.xaxis[0].formatter = NumeralTickFormatter(format="000000")
    plots.append(p)

    try:
        h =  Histogram(dataframe['t_eff'], bins=25, color='skyblue', width=1000, height=500)
        plots.append(h)
    except:
        pass
    return plots
