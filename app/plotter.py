#!/usr/bin/env python
import os

import numpy as np

from bokeh.plotting import figure,ColumnDataSource
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool, ResizeTool,
                          glyphs,Legend,NumeralTickFormatter)
from bokeh.charts import Scatter,Bar,Histogram,color

def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    p = Histogram(dataframe,'total time',bins=24, color = 'green',
        title="Processing Times",height=500,width=1000)
    text = """Mean: {mean}
            Min: {min}
            Max: {max}""".format(mean=round(dataframe[dataframe.status==0]['total time'].mean(skipna=True),3),min = dataframe[dataframe.status==0]['total time'].min(),max=dataframe[dataframe.status==0]['total time'].max())
    mytext = glyphs.Text(x=p.x_range.end-(p.x_range.end/3), y=p.y_range.end - (p.y_range.end/3),text=[text],text_font_size='10pt')
    p.add_glyph(mytext)
    return p

def plot_accepted(dataframe):
    p = Bar(dataframe, label='program',values='program', agg='count', group='assessment',
        title="Accepted by Program", legend='top_right',height=500,width=1000)

    return p

def plot_exec_time(dataframe):
    p = Bar(dataframe, values='total time', label='exec_host', agg='mean', color='blue',
        title='Mean Processing Time for Each Exec Host',height=500,width=1000)
    return p

def plot_status_per_host(dataframe):
    mycolors = ['green','red','blue','orange','yellow','purple','black']
    p = Bar(dataframe, label='exec_host',values='exec_host', agg='count', group='status',
        title="Status per Exec Host", legend='top_right',color=color('status', palette=mycolors),
        height=500,width=1000)

    return p

def plot_t_eff(dataframe):
    def create_data_source(df):
        return ColumnDataSource(data=dict(t_eff=df['t_eff'],expnum=df['expnum'],program=df['program'],attnum=df['attnum'],band=df['band']))
    plots = []
    df_false = dataframe[dataframe['assessment']=='False']
    df_true = dataframe[dataframe['assessment']=='True']

    # Creating scatter plot
    p = figure(lod_threshold=100,tools = [PanTool(),BoxZoomTool(),ResizeTool(),WheelZoomTool(),ResetTool(),HoverTool(tooltips = [('expnum','@expnum'),('band','@band'),('program', '@program'),('teff','@t_eff'),('attempt','@attnum')])], x_axis_label = "expnum", y_axis_label = "t_eff", title = 't_eff',width=1000,height=500)
    p.scatter('expnum','t_eff',source=create_data_source(df_false),fill_color='red',line_color='white',alpha=0.5)
    p.scatter('expnum','t_eff',source=create_data_source(df_true),fill_color='green',line_color='white',alpha=0.5)

    p.xaxis[0].formatter = NumeralTickFormatter(format="000000")
    plots.append(p)
    
    # Creating histogram
    dataframe.t_eff = dataframe.t_eff.convert_objects(convert_numeric=True)

    p2 = figure(x_axis_label = "t_eff", y_axis_label = "expnum", title = 't_eff',width=1000,height=500)

    h,edges = np.histogram(dataframe.t_eff.values, bins=np.linspace(float(min(dataframe.t_eff)),float(max(dataframe.t_eff)),35))
    p2.quad(top=h, bottom=0, left=edges[:-1], right=edges[1:], fill_color="#036564", line_color="#033649")
    text = """Mean: {mean}
            Min: {min}
            Max: {max}""".format(mean=round(dataframe.t_eff.mean(skipna=True),3),min = dataframe.t_eff.min(),max=dataframe.t_eff.max())
    mytext = glyphs.Text(x=edges[-1]-(edges[-1]/3),y=h.max()-(h.max()/3),text=[text],text_font_size='10pt')
    p2.add_glyph(mytext)

    plots.append(p2)

    return plots
