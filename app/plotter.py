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

def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Histogram(dataframe,'total time',bins=24, color = 'green',
        title="Processing Times",height=500,widdth=1000)
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
    p.scatter('t_eff','expnum',source=create_data_source(df_false),fill_color='red',line_color='white',alpha=0.5)
    p.scatter('t_eff','expnum',source=create_data_source(df_true),fill_color='green',line_color='white',alpha=0.5)
    h =  Histogram(dataframe['t_eff'], bins= 50, color='skyblue', width=1000, height=500)
    plots.append(p)
    plots.append(h)
    return plots

if __name__=='__main__':
    import get_data
    df,columns,reqnum,mean_times,updated = get_data.processing_detail(db,reqnum)
    df2 = df.dropna()
    df_pass = df[df.status==0].dropna()
    df_teff = df_pass
    df_teff.t_eff.replace(0,'None')
    plots = []
    try:
        times = plotter.plot_times(df_pass)
        plots.append(times)
    except:
        pass
    try:
        assess = plotter.plot_accepted(df_pass)
        plots.append(assess)
    except:
        pass
    try:
        exechost = plotter.plot_exec_time(df_pass)
        plots.append(exechost)
    except:
        pass
    try:
        fails = plotter.plot_status_per_host(df2)
        plots.append(fails)
    except:
        pass
    try:
        teff =plotter.plot_t_eff(df_teff[(df_teff.t_eff !='None')])
        for p in teff: plots.append(p)
    except:
        pass
    html = file_html(vplot(*plots),CDN,'plots')
    path = '/work/devel/mjohns44/git/desdm-dash/app/templates/reports'
    filename = 'plots_{reqnum}'.format(reqnum=reqnum)
    filepath = os.path.join(path,filename)
    with open(filepath,'w') as h:
        h.write('<center>\n')
        h.write(html)
        h.write('</center>\n')
