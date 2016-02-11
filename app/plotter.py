from bokeh.embed import components,file_html
from bokeh.plotting import figure,ColumnDataSource
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool,
                          glyphs,Legend)
from bokeh.palettes import Spectral6
from bokeh.charts import Bar,Histogram
from bokeh.io import vplot

import get_data

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
