from bokeh.embed import components,file_html
from bokeh.plotting import figure,ColumnDataSource,output_file
from bokeh.models import (HoverTool, BoxSelectTool, BoxZoomTool,
                          PanTool, ResetTool,WheelZoomTool,
                          glyphs,Legend,NumeralTickFormatter,SingleIntervalTicker,LinearAxis)
from bokeh.palettes import Spectral6,PuOr9
from bokeh.charts import Scatter,Bar,Histogram,color,show
from bokeh.io import vplot,hplot,gridplot
import pandas
from bokeh.io import output_notebook
from bokeh.resources import CDN

#output_file('plots.html')

dataframe = pandas.read_csv('allbands.csv')

#output_notebook()

plots = []
TOOLS=[BoxZoomTool(),PanTool(),ResetTool(),WheelZoomTool()]
p1 = Histogram(dataframe,'total time',bins=24, color = 'green',
        title="Processing Times",height=500,width=1000)
plots.append(p1)

p2 = Bar(dataframe, label='program',values='program', agg='count', group='assessment',
        title="Accepted by Program", legend='top_right',height=500,width=1000)
plots.append(p2)

p3 = Bar(dataframe, values='total time', label='exec_host', agg='mean', color='blue',
        title='Mean Processing Time for Each Exec Host',height=500,width=1000)
plots.append(p3)

mycolors = ['green','red','blue','orange','yellow','purple','black']
p4 = Bar(dataframe, label='exec_host',values='exec_host', agg='count', group='status',
        title="Status per Exec Host", legend='top_right',color=color('status', palette=mycolors),
        height=500,width=1000)
plots.append(p4)

#html1 = file_html(vplot(*plots),CDN,'plots')
#with open('plots.html','w') as p:
#    p.write(html1)

def create_data_source(df):
    return ColumnDataSource(data=dict(t_eff=df['t_eff'],expnum=df['expnum'],program=df['program'],accepted=df['assessment'],attnum=df['attnum']))
df_false = dataframe[dataframe['assessment']=='False']
df_true = dataframe[dataframe['assessment']=='True']
p = figure(tools = [HoverTool(tooltips = [('expnum','@expnum'),('program', '@program'),('teff','@t_eff'),('attempt','@attnum')]),BoxZoomTool(),ResetTool(),WheelZoomTool()], x_axis_label = "expnum", y_axis_label = "t_eff", title = 't_eff',width=1000,height=500)
p.scatter('expnum','t_eff',source=create_data_source(df_false),fill_color='red',line_color='white',alpha=0.5)
p.scatter('expnum','t_eff',source=create_data_source(df_true),fill_color='green',line_color='white',alpha=0.5)
p.xaxis[0].formatter = NumeralTickFormatter(format="000000")
h =  Histogram(dataframe['t_eff'], bins= 50, color='skyblue', width=1000, height=500)
plots.append(p)
plots.append(h)

html = file_html(vplot(*plots),CDN,'teff')

with open('plots.html','w') as h:
    h.write('<center>\n')
    h.write(html)
    h.write('</center>\n')
#show(gs)
