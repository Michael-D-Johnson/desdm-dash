#!/usr/bin/env python
import os
import numpy as np
import pandas as pd
import math
from datetime import datetime, timedelta
from bokeh.plotting import figure,ColumnDataSource
from bokeh.models.widgets import Panel, Tabs
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,FixedTicker,
                          PanTool, ResetTool,WheelZoomTool, 
                          glyphs,Legend,NumeralTickFormatter, LinearAxis, Range1d)
from bokeh.layouts import column
from bokeh.palettes import d3
from bokeh.transform import factor_cmap

def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    dataframe = dataframe[dataframe["total time"] !=-1]
    p = figure(title="Processing Times",height=500,width=1000)
    hist, edges = np.histogram(dataframe["total time"],bins = 24)
    p.quad(top=hist,bottom=0, left=edges[:-1], right=edges[1:],line_color="white")
    return p

def plot_accepted(dataframe):
    dataframe = dataframe.fillna('None')
    grouped_df = dataframe.groupby(by=['program','assessment'])
    p = figure(title="Accepted by Program",height=500,width=1000, x_range=grouped_df,  tooltips=[("Count", "@pfw_attempt_id_count"), ("Program, Assessment", "@program_assessment")])

    num_factors = len(dataframe.assessment.unique())
    if num_factors == 1:
        index_cmap = factor_cmap('program_assessment', palette=d3['Category20'][20], factors=dataframe.assessment.unique(), start=1, end=num_factors+1)
    else:
        index_cmap = factor_cmap('program_assessment', palette=d3['Category20'][20], factors=dataframe.assessment.unique(), start=1, end=num_factors)

    p.vbar(x='program_assessment',top='pfw_attempt_id_count', source =grouped_df,width=0.9, line_color=None)
    return p

def plot_exec_time(dataframe):
    dataframe["total_time"] = dataframe["total time"] 
    dataframe = dataframe.dropna(axis=0, subset=['total_time'])
    for i,row in dataframe.iterrows():
        if isinstance(row['exec_host'],str):
            try:
                dataframe.loc[i,'exec_host'] = str(row['exec_host']).split('-0-')[1]
            except:
                dataframe.loc[i,'exec_host'] = str(row['exec_host']).split('.')[0]
        else:
            continue
    dataframe = dataframe.fillna('None')
    dataframe = dataframe[dataframe.exec_host != 'None']
    grouped_df = dataframe.groupby('exec_host') 
    p = figure(title='Mean Processing Time for Each Exec Host',height=500,width=1000,x_range=grouped_df,
                tooltips=[("Mean time", "@total_time_mean"), ("Exec host", "@exec_host")])

    p.vbar(x='exec_host',top='total_time_mean',source = grouped_df,width=0.9)
    p.xaxis.major_label_orientation = -math.pi/3
    return p

def plot_status_per_host(dataframe):
    dataframe = dataframe.fillna('None')
    for i,row in dataframe.iterrows():
        if row['exec_host']:
            try:
                dataframe.loc[i,'exec_host'] = str(row['exec_host']).split('-0-')[1]
            except:
                dataframe.loc[i,'exec_host'] = str(row['exec_host']).split('.')[0]
        else:
            continue
    dataframe = dataframe.replace(np.nan,'None')
    dataframe = dataframe[(dataframe.exec_host != None) | (dataframe.exec_host !='None')]
    dataframe.status = dataframe.status.astype(str)
    dataframe = dataframe[dataframe.status !='None']

    grouped_df = dataframe.groupby(by=['exec_host','status'])
    p = figure(title="Status per Exec Host", height=500,width=1000, x_range= grouped_df, tooltips = [("Count", "@pfw_attempt_id_count"), ("Exec host, Status", "@exec_host_status")])
    num_factors = len(dataframe.status.unique())
    if num_factors ==1:
        index_cmap = factor_cmap('exec_host_status', palette=d3['Category20'][20], factors=dataframe.status.unique(), start=1, end=num_factors+1)
    else:
        index_cmap = factor_cmap('exec_host_status', palette=d3['Category20'][20], factors=dataframe.status.unique(), start=1, end=num_factors)

    p.vbar(x='exec_host_status',top='pfw_attempt_id_count',source = grouped_df,width=0.9,line_color=None,
            fill_color=index_cmap)
    p.xaxis.group_label_orientation = "vertical"
    p.xaxis.major_label_orientation = -math.pi/3
    return p

def plot_t_eff(dataframe):
    dataframe = dataframe.replace('None',None)
    dataframe = dataframe.dropna()
    dataframe.t_eff = dataframe.t_eff.astype('float')
    if dataframe.t_eff.empty:
        pass
    else:
        def create_data_source(df):
            return ColumnDataSource(data=dict(t_eff=df['t_eff'],expnum=df['expnum'],program=df['program'],attnum=df['attnum'],band=df['band'],b_eff=df['b_eff'],c_eff=df['c_eff'],f_eff=df['f_eff']))
        line_colors = ['black','blue','yellow','pink']
        plots = []
        # Creating scatter plot
        p = figure(tools = [PanTool(),BoxZoomTool(),WheelZoomTool(),ResetTool(),HoverTool(tooltips = [('expnum','@expnum'),('band','@band'),('program', '@program'),('t_eff','@t_eff'),('b_eff','@b_eff'),('c_eff','@c_eff'),('f_eff','@f_eff'),('attempt','@attnum')])], x_axis_label = "expnum", y_axis_label = "t_eff", title = 't_eff',width=1000,height=500 )
        for i,prog in enumerate(dataframe.program.unique()):
            df_false = dataframe[(dataframe['assessment']=='False') & (dataframe['program'] ==prog)]
            df_true = dataframe[(dataframe['assessment']=='True') & (dataframe['program'] ==prog)]
            df_unknown = dataframe[(dataframe['assessment']=='Unknown') & (dataframe['program']==prog)]
    
            p.circle('expnum','t_eff',source=create_data_source(df_false),fill_color='red',line_color=line_colors[i],size=8,line_width=3,legend = prog)
            p.circle('expnum','t_eff',source=create_data_source(df_true),fill_color='green',line_color=line_colors[i],size=8,line_width = 3, legend = prog)
            p.circle('expnum','t_eff',source=create_data_source(df_unknown),fill_color='orange',line_color=line_colors[i],size=8,line_width=3,legend = prog)

        p.xaxis[0].formatter = NumeralTickFormatter(format="000000")
    
        plots.append(p)

        # Creating histogram
        p2 = figure(x_axis_label = "t_eff", y_axis_label = "expnum", title = 't_eff',width=1000,height=500)

        h,edges = np.histogram(dataframe.t_eff.values, bins=np.linspace(float(min(dataframe.t_eff)),float(max(dataframe.t_eff)),35))
        p2.quad(top=h, bottom=0, left=edges[:-1], right=edges[1:], fill_color="#036564", line_color="#033649")
        plots.append(p2)

        return plots

# plot jobs running over time
def plot_exec_job_time(df, start_time):

    # Setup bokeh plot
    p = figure(plot_height=500, plot_width=1000, x_axis_type="datetime", title='Exec Job Time')

    lasthost = []
    x_values = []
    # Setup x_axis data
    for i in range(0,len(df[list(df.keys())[0]])):
        x_values.append(start_time)
        lasthost.append(0)
        start_time += timedelta(minutes=10)
    j=0
    colors = d3['Category20'][20]*3
    # Generate y_axis data while ploting
    for host in df.keys():
        if isinstance(host,float):
            continue
        else:
            y_values = []
            for i in range(0, len(df[host])):
                y_values.append(int(lasthost[i]) + int(df[host][i]))
                lasthost[i] = int(lasthost[i]) + int(df[host][i])
        p.line(x=x_values , y=y_values , legend=host,color=colors[j], alpha=0.8,line_width=1.5)
        j = (j + 1) % 20


    return p
