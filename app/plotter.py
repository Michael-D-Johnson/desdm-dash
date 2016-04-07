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
    from collections import OrderedDict
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    mycolors = ['green','red','blue','orange','yellow','purple','black']
    p = Bar(dataframe, label='exec_host',values='exec_host', agg='count', group='status',
        title="Status per Exec Host", legend='top_right',color=color('status', palette=mycolors))

    figscript,figdiv = components(p)
    return (figscript,figdiv)
