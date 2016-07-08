#!/usr/bin/env python
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from bokeh.plotting import figure,ColumnDataSource
from bokeh.models.widgets import Panel, Tabs
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,FixedTicker,
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
    df_unknown = dataframe[dataframe['assessment']=='Unknown']

    # Creating scatter plot
    p = figure(tools = [PanTool(),BoxZoomTool(),ResizeTool(),WheelZoomTool(),ResetTool(),HoverTool(tooltips = [('expnum','@expnum'),('band','@band'),('program', '@program'),('teff','@t_eff'),('attempt','@attnum')])], x_axis_label = "expnum", y_axis_label = "t_eff", title = 't_eff',width=1000,height=500)
    p.scatter('expnum','t_eff',source=create_data_source(df_false),fill_color='red',line_color='white',alpha=0.5)
    p.scatter('expnum','t_eff',source=create_data_source(df_true),fill_color='green',line_color='white',alpha=0.5)
    p.scatter('expnum','t_eff',source=create_data_source(df_unknown),fill_color='orange',line_color='white',alpha=0.5)

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
    p = figure(plot_height=500, plot_width=1000, x_axis_type="datetime", y_range=list(dataframe.exec_host.unique()), title='Exec Wall Time')
    
    # Loop though each exec_host 1 at a time changing y value on each one.
    count = 0
    print "Starting Plot"
    for exechost, group in df:
        print exechost, group.reqnum.unique()[0]
        count = count+1
        #print exechost
        for attnum in group.attnum.unique():
            p.segment( x0=group[group.attnum==attnum].start_time, y0=len(group) * [count+(0.1*(attnum-1))], x1=group[group.attnum==attnum].end_time, y1=len(group) * [count+(0.1*(attnum-1))], color=Colors[attnum], line_width=3, legend="Attempt Num: " + str(attnum))

### Line Method ###
### If you want to use it, add hovertool to the list of tools and comment out segment ###    
#        for attnum in group.attnum.unique():
#            for i, row in group[group.attnum==attnum].iterrows():
#                p.line(x=[row['start_time'],row['end_time']], y=[count+(0.1*(attnum-1)),count+(0.1*(attnum-1))], source=create_data_source(group), color=Colors[attnum], line_width=3)
#                print row['start_time'] 

    return p

def create_tab(df, band, hover, tag, tab_name):
    def create_all_data_source(df):
       return ColumnDataSource(data=dict(tilename=df['tilename'],dmedian=df['dmedian']))

    p = figure(height=1000, width=1000, x_axis_label='RA (Deg)', y_axis_label='DEC (Deg)', tools=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool(),hover], title=str(tag)+' Coadd Map')
    p.patches(xs=df['x'], ys=df['y'], source=create_all_data_source(df), name=tab_name, fill_color='blue', fill_alpha=df['alphas'], line_color='black')
    p.xaxis[0].ticker=FixedTicker(ticks=[360])
    tab = Panel(child=p, title=tab_name)

    return tab

def plot_coadd(all_df, processed_df, band_df, tag):
    def create_processed_data_source(df):
        return ColumnDataSource(data=dict(status=df['status'],tilename=df['tilename'],id=df['id'], reqnum=df['reqnum'],attnum=df['attnum'],dmedian=df['dmedian']))
    def create_all_data_source(df):
        return ColumnDataSource(data=dict(tilename=df['tilename'],dmedian=df['dmedian'])) 

    Colors=['green','blue','blue','blue','blue','blue','blue','blue','blue','blue']

    ### All_df data prep ###
    new_all_df = pd.DataFrame()
    xlist, ylist, tilelist =[],[],[]
    for i, row in all_df.iterrows():
        # Fixes wrapping issue
        if (row['rac3']-row['rac4']) > 100:
            row['rac4']=360 + row['rac4']
            row['rac1']=360 + row['rac1']
        # Shifts image so that tank is visable
        if row['rac3'] < 180:
            row['rac1'] = row['rac1']+360
            row['rac2'] = row['rac2']+360
            row['rac3'] = row['rac3']+360
            row['rac4'] = row['rac4']+360
        xlist.append([row['rac1'], row['rac2'], row['rac3'], row['rac4']])
        ylist.append([row['decc1'], row['decc2'], row['decc3'], row['decc4']])
        tilelist.append(row['tilename'])

    new_all_df['x']=xlist
    new_all_df['y']=ylist
    new_all_df['tilename']=tilelist 

    ### Avg_df data prep ###
    avg_band_df = band_df.groupby(by = ['tilename'])

    new_avg_band_df = pd.DataFrame()
    maxdepth=20
    tilelist, depthlist, alphalist = [],[],[]
    for tile,group in avg_band_df:
        tilelist.append(tile)
        depth,count = 0,0
        for i,row in group.iterrows():
            count += 1
            depth += row['dmedian']
        depth = depth/count
        alphalist.append(depth/maxdepth)
        depthlist.append(depth)

    new_avg_band_df['tilename'] = tilelist
    new_avg_band_df['dmedian'] = depthlist
    new_avg_band_df['alphas'] = alphalist
    
    ### Individual_df data prep ###
    maxdepth=20
    alphalist = []
    for i,row in band_df.iterrows():
        alphalist.append(row['dmedian']/maxdepth)

    band_df['alphas'] = alphalist

    ### Merge all dfs ###
    individual_df = pd.merge(new_all_df, band_df, how='outer', on=['tilename'])
    individual_df.fillna(0, inplace=True)
    avg_df = pd.merge(new_all_df, new_avg_band_df, how='outer', on=['tilename'])
    avg_df.fillna(0, inplace=True)
    fn_df = pd.merge(avg_df, processed_df, how='inner', on=['tilename'])
    fn_df.fillna('None', inplace=True)
    fn_df = fn_df.groupby(by = ['tilename'])

    tablist = []

    ### Avg band plot ###
    all_hover = HoverTool(names=['all'])
    processed_hover = HoverTool(names=['processed'])
    TOOLS = [BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool(),all_hover,processed_hover]

    p = figure(height=1000, width=1000, x_axis_label='RA (Deg)', y_axis_label='DEC (Deg)', tools=TOOLS, title=str(tag)+' Coadd Map')
    p.xaxis[0].ticker=FixedTicker(ticks=[360])
    p.patches(xs=avg_df['x'], ys=avg_df['y'], source=create_all_data_source(avg_df[~avg_df.tilename.isin(processed_df.tilename.values)]), name='all', fill_color='blue', fill_alpha=avg_df['alphas'], line_color='black')

    for i,group in fn_df:
        p.patches(xs=group[group.attnum==max(group['attnum'])].x, ys=group[group.attnum==max(group['attnum'])].y, source=create_processed_data_source(group[group.attnum==max(group['attnum'])]), name='processed', fill_color=Colors[int(group[group.attnum==max(group['attnum'])].status)], fill_alpha=0.95, line_color='black')

    all_hover.point_policy = "follow_mouse"
    processed_hover.point_policy = "follow_mouse"
    processed_hover.tooltips = [("Tilename", "@tilename"),("Pfw_attempt_id","@id"),("Reqnum","@reqnum"),("Attnum","@attnum"),("Status","@status"),("Depth","@dmedian")]
    all_hover.tooltips = [("Tilename", "@tilename"),("Depth","@dmedian")]

    avg = Panel(child=p, title='DET')
    tablist.append(avg)
    
    ### Individual band plots ###
    bands, names = ['g','r','i','z','Y','u'], ['G','R','I','Z','Y','U']
    for i in range(0,6):
        hover = HoverTool(names=[names[i]])

        tab = create_tab(individual_df[individual_df['band']==bands[i]], bands[i], hover, tag, names[i])
        tablist.append(tab)

        hover.point_policy = "follow_mouse"
        hover.tooltips = [("Tilename", "@tilename"),("Depth","@dmedian")]

    tabs = Tabs(tabs=tablist)

    return tabs

def data_usage_plot(df):
    def create_data_source(df):
        return ColumnDataSource(data=dict(filesystem=df['filesystem'],total_size=df['total_size'],used=df['used'],available=df['available'],use_percent=df['use_percent'],mounted=['mounted'],submittime=[x.strftime("%Y-%m-%d") for x in df['submittime']]))

    hover = HoverTool(names=['points'])
    TOOLS = [BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool(),hover]
    Colors = ['red','navy','olive','firebrick','lightskyblue','yellowgreen','lightcoral','yellow', 'green','blue','gold']

    # Change percents to format bokeh can use
    fp_list = []
    for row in df['use_percent']:
        fp_list.append(float(row.replace('%','')))
    df['f_percent'] = fp_list

    # Begin Plotting
    x_max = datetime.now()
    p = figure(height=500, width=1000, x_axis_type="datetime", x_range=(x_max,(x_max-timedelta(14))) y_axis_label='Percent Full', tools=TOOLS, title='Data Usage by Filesystem')

    # Points with hover info
    p.scatter(x=df['submittime'], y=df['f_percent'], name='points', source=create_data_source(df), color='black', size=6)
    df = df.groupby(by = ['filesystem'])

    # Connecting lines
    count = 0
    for filesystem, group in df:
        count += 1
        p.line(x=group['submittime'] ,y=group['f_percent'], color=Colors[count], legend=str(filesystem), line_width=3)

    # Formating
    p.legend.orientation = "top_left"
    hover.point_policy = "follow_mouse"
    hover.tooltips = [("Filesystem", "@filesystem"),("Size","@total_size"),("Available","@available"),("Percent Used","@use_percent"),("Time","@submittime")]

    return p
