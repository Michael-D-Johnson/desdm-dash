#!/usr/bin/env python
import os

import numpy as np

from bokeh.plotting import figure,ColumnDataSource
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool,
                          glyphs,Legend,NumeralTickFormatter)
from bokeh.charts import Scatter,Bar,Histogram,color

def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Histogram(dataframe,'total time',bins=24, color = 'green',
        title="Processing Times",height=500,width=1000)
    text = """Mean: {mean}
            Min: {min}
            Max: {max}""".format(mean=round(dataframe[dataframe.status==0]['total time'].mean(skipna=True),3),min = dataframe[dataframe.status==0]['total time'].min(),max=dataframe[dataframe.status==0]['total time'].max())
    mytext = glyphs.Text(x=p.x_range.end-(p.x_range.end/3), y=p.y_range.end - (p.y_range.end/3),text=[text],text_font_size='10pt')
    p.add_glyph(mytext)
    return p

def plot_accepted(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, label='program',values='program', agg='count', group='assessment',
        title="Accepted by Program", legend='top_right',height=500,width=1000)

    return p

def plot_exec_time(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, values='total time', label='exec_host', agg='mean', color='blue',
        title='Mean Processing Time for Each Exec Host',height=500,width=1000)
    return p

def plot_status_per_host(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
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

    # Creating scatter pllot
    p = figure(lod_threshold=100,tools = [HoverTool(tooltips = [('expnum','@expnum'),('band','@band'),('program', '@program'),('teff','@t_eff'),('attempt','@attnum')]),BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()], x_axis_label = "expnum", y_axis_label = "t_eff", title = 't_eff',width=1000,height=500)
    p.scatter('expnum','t_eff',source=create_data_source(df_false),fill_color='red',line_color='white',alpha=0.5)
    p.scatter('expnum','t_eff',source=create_data_source(df_true),fill_color='green',line_color='white',alpha=0.5)

    p.xaxis[0].formatter = NumeralTickFormatter(format="000000")
    plots.append(p)
    
    # Creating histogram
    p2 = figure(tools = TOOLS, x_axis_label = "t_eff", y_axis_label = "expnum", title = 't_eff',width=1000,height=500)

    h,edges = np.histogram(dataframe['t_eff'].values, bins=np.linspace(min(dataframe.t_eff),max(dataframe.t_eff),35))
    p2.quad(top=h, bottom=0, left=edges[:-1], right=edges[1:], fill_color="#036564", line_color="#033649")
    text = """Mean: {mean}
            Min: {min}
            Max: {max}""".format(mean=round(dataframe.t_eff.mean(skipna=True),3),min = dataframe.t_eff.min(),max=dataframe.t_eff.max())
    mytext = glyphs.Text(x=edges[-1]-(edges[-1]/3),y=h.max()-(h.max()/3),text=[text],text_font_size='10pt')
    p2.add_glyph(mytext)

    plots.append(p2)

    return plots

# Plots time running for each exec 
def plot_exec_wall_time(dataframe):
### For Line Method ###
#    def create_data_source(df):
#        return ColumnDataSource(data=dict(exec_host=df['exec_host'],unitname=df['unitname'],attnum=df['attnum'],start_time=df['start_time'],end_time=df['end_time']))

    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    Colors=['red','navy','olive','firebrick','lightskyblue','yellowgreen','lightcoral','yellow', 'green','blue','gold','red']
    pd.to_datetime(dataframe['start_time'])
    pd.to_datetime(dataframe['end_time'])
    dataframe = dataframe.sort(['exec_host','start_time','end_time'] ,ascending=True)
    dataframe.dropna(axis=0)
    df = dataframe.groupby(by = ['exec_host'])

    # Setup bokeh plot
    p = figure(plot_height=500, plot_width=1000, x_axis_type="datetime", y_range=list(tempframe.exec_host.unique()), title='Exec Wall Time')
    
    # Loop though each exec_host 1 at a time changing y value on each one.
    count = 0
    print "Starting Plot"
    for exechost, group in df:
        count = count+1
        print exechost
        
        for attnum in group.attnum.unique():
            p.segment( x0=group[group.attnum==attnum].start_time, y0=len(group) * [count+(0.1*(attnum-1))], x1=group[group.attnum==attnum].end_time, y1=len(group) * [count+(0.1*(attnum-1))], color=Colors[attnum], line_width=3, legend="Attempt Num: " + str(attnum))

### Line Method ###
### If you want to use it, add hovertool to the list of tools and comment out segment ###    
#        for attnum in group.attnum.unique():
#            for i, row in group[group.attnum==attnum].iterrows():
#                p.line(x=[row['start_time'],row['end_time']], y=[count+(0.1*(attnum-1)),count+(0.1*(attnum-1))], source=create_data_source(group), color=Colors[attnum], line_width=3)
#                print row['start_time'] 

    return p

def plot_coadd(df):
    def create_data_source(df):
        return ColumnDataSource(data=dict(tilename=df['tilename']))

    Colors=['red','Green','Blue','olive','firebrick','yellow','gold']

    newdf = pd.DataFrame()
    xlist, ylist, tilelist =[],[],[]
    for i, row in df.iterrows():
        xlist.append([row['rac1'], row['rac2'], row['rac3'], row['rac4']])
        ylist.append([row['decc1'], row['decc2'], row['decc3'], row['decc4']])
        tilelist.append(row['tilename'])

    newdf['x']=xlist
    newdf['y']=ylist
    newdf['tilename']=tilelist

    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool(),HoverTool()]

    p = figure(height=1000, width=1000, tools=TOOLS, title='Coadd Map')

    p.patches(xs=newdf['x'], ys=newdf['y'], source=create_data_source(newdf),fill_color='grey',fill_alpha=0.1, line_color='black')
    
    hover = p.select_one(HoverTool)
    hover.point_policy = "follow_mouse"
    hover.tooltips = [("Tilename", "@tilename"),("Status","@status")]

    return p
