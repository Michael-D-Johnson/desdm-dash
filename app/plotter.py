from bokeh.embed import components,file_html
from bokeh.plotting import figure,ColumnDataSource
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool,
                          glyphs,Legend)
from bokeh.palettes import Spectral6,PuOr9
from bokeh.charts import Scatter,Bar,Histogram,color
from bokeh.io import vplot

def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Histogram(dataframe,'total time',bins=24, color = 'green',
        title="Processing Times")
    figscript,figdiv = components(p)

    return (figscript,figdiv)

def plot_accepted(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, label='program',values='program', agg='count', group='assessment',
        title="Accepted by Program", legend='top_right')

    figscript,figdiv = components(p)
    return (figscript,figdiv)

def plot_exec_time(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, values='total time', label='exec_host', agg='mean', color='blue',
        title='Mean Processing Time for Each Exec Host')
    figscript,figdiv = components(p)
    return (figscript,figdiv)

def plot_status_per_host(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    mycolors = ['green','red','blue','orange','yellow','purple','black']
    p = Bar(dataframe, label='exec_host',values='exec_host', agg='count', group='status',
        title="Status per Exec Host", legend='top_right',color=color('status', palette=mycolors))

    figscript,figdiv = components(p)
    return (figscript,figdiv)

def plot_t_eff(dataframe):
    bands = ['u','g','r','i','z','Y','VR']
    dfs = [dataframe[dataframe['band']==n] for n in bands]
    dfs = [d for d in dfs if len(d)>0]
    def create_data_source(df):
        return ColumnDataSource(data=dict(t_eff=df['t_eff'],expnum=df['expnum'],program=df['program'],accepted=df['accepted']))
    ps = []
    for i in range(0,len(dfs)):
        df_false = dfs[i][dfs[i]['accepted']=='False']
        df_true = dfs[i][dfs[i]['accepted']=='True']
        p = figure(tools = [HoverTool(tooltips = [('program', '@program'),('accepted','@accepted')]),BoxZoomTool(),ResetTool(),WheelZoomTool()], x_axis_label = "t_eff", y_axis_label = "expnum", title = dfs[i]['band'].iloc[0],width=500,height=500)
        p.scatter('t_eff','expnum',source=create_data_source(df_false),radius=0.01,fill_color='red',line_color='white',alpha=0.5)
        p.scatter('t_eff','expnum',source=create_data_source(df_true),radius=0.01,fill_color='green',line_color='white',alpha=0.5)
        ps.append(p)
    ph = []
    for i in range(0,len(dfs)):
        p =  Histogram(dfs[i]['t_eff'], color='skyblue', bins=50, width=500, height=500)
        ph.append(p)
    gs = []
    for i in range(0,len(dfs)):
        gs.append(hplot(*[ps[i],ph[i]]))
        gs = vplot(*gs)
    figscript,figdiv = components(gs)
    return (figscript,figdiv)

