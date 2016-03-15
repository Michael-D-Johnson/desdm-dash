from bokeh.embed import components,file_html
from bokeh.plotting import figure,ColumnDataSource
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool,
                          glyphs,Legend)
from bokeh.palettes import Spectral6
from bokeh.charts import Scatter,Bar,Histogram
from bokeh.io import vplot


def plot_times_per_host(dataframe):
    """Starting point to create scatter plot for total run times per exec host"""
    dataframe = dataframe.fillna('NA')
    dataframe = dataframe[dataframe.exec_host != 'NA']
    def create_data_source(data_frame):
        return ColumnDataSource(
            data=dict(
            expnum=data_frame['total time'],
            exec_host=data_frame['exec_host'],
            ))
    source = create_data_source(dataframe)
    TOOLS=[HoverTool(tooltips = [('total time', '@total_time'),('exec_host','@exec_host')]),BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = figure(title="Processing time per exec_host", x_axis_label='exec_host',
               y_axis_label='total time',tools=TOOLS)

    p.scatter('exec_host','total time', source = source)
    figscript,figdiv = components(p)
    return (figscript,figdiv)

def plot_times(dataframe):
    dataframe = dataframe.fillna(-1)
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Histogram(dataframe,'total time',bins=24,
        title="Processing Times")
    figscript,figdiv = components(p)

    return (figscript,figdiv)

def plot_accepted(dataframe):
    TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
    p = Bar(dataframe, label='program',values='program', agg='count', group='assessment',
        title="Accepted by Program", legend='top_right')

    figscript,figdiv = components(p)
    return (figscript,figdiv)
