import os
from bokeh.resources import CDN
from bokeh.embed import components,file_html
from bokeh.plotting import figure,ColumnDataSource
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool,
                          glyphs,Legend)
from bokeh.palettes import Spectral6,PuOr9
from bokeh.charts import Scatter,Bar,Histogram,color
from bokeh.io import vplot,hplot,gridplot
from app import celery

@celery.task()
def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Histogram(dataframe,'total time',bins=24, color = 'green',
        title="Processing Times")
    figscript,figdiv = components(p)

    #return (figscript,figdiv)
    return p

@celery.task()
def plot_accepted(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, label='program',values='program', agg='count', group='assessment',
        title="Accepted by Program", legend='top_right')

    figscript,figdiv = components(p)
    #return (figscript,figdiv)
    return p

@celery.task()
def plot_exec_time(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, values='total time', label='exec_host', agg='mean', color='blue',
        title='Mean Processing Time for Each Exec Host')
    figscript,figdiv = components(p)
    #return (figscript,figdiv)
    return p

@celery.task()
def plot_status_per_host(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    mycolors = ['green','red','blue','orange','yellow','purple','black']
    p = Bar(dataframe, label='exec_host',values='exec_host', agg='count', group='status',
        title="Status per Exec Host", legend='top_right',color=color('status', palette=mycolors))

    figscript,figdiv = components(p)
    #return (figscript,figdiv)
    return p

@celery.task()
def plot_t_eff(dataframe):
    """
    bands = ['u','g','r','i','z','Y','VR']
    dfs = [dataframe[dataframe['band']==n] for n in bands]
    dfs = [d for d in dfs if len(d)>0]
    def create_data_source(df):
        return ColumnDataSource(data=dict(t_eff=df['t_eff'],expnum=df['expnum'],program=df['program'],accepted=df['assessment']))
    plots = []
    for i in range(0,len(dfs)):
        df_false = dfs[i][dfs[i]['assessment']=='False']
        df_true = dfs[i][dfs[i]['assessment']=='True']
        p = figure(tools = [HoverTool(tooltips = [('expnum','@expnum'),('program', '@program')]),BoxZoomTool(),ResetTool(),WheelZoomTool()], x_axis_label = "t_eff", y_axis_label = "expnum", title = dfs[i]['band'].iloc[0],width=500,height=500)
        p.scatter('t_eff','expnum',source=create_data_source(df_false),fill_color='red',line_color='white',alpha=0.5)
        p.scatter('t_eff','expnum',source=create_data_source(df_true),fill_color='green',line_color='white',alpha=0.5)
        h =  Histogram(dfs[i]['t_eff'], bins= 50, color='skyblue', width=500, height=500,title=dfs[i]['band'].iloc[0])
        plots.append([hplot(p,h)])
    gs = gridplot(*plots)

    """
    gs =  Histogram(dataframe,'t_eff', bins= 25, color='skyblue')

    #html = file_html(gs,CDN,"t_eff")
    #with open('/Users/mjohns44/GIT_DESDM/desdm-dash-mjohns44/desdm-dash/app/templates/t_eff.html','w') as myfile:
        # /work/devel/mjohns44/git/desdm-dash/app/templates/t_eff.html','w') as myfile:
     #   myfile.write(html)
    figscript,figdiv = components(gs)
    return (figscript,figdiv)
