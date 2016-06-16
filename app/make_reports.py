#! /usr/bin/env python
import os
import pandas
from datetime import datetime

from bokeh.resources import CDN,INLINE
from bokeh.embed import file_html
from bokeh.io import vplot

import plotter
import get_data
import query

from app import app

# Creating command-line arguments
from configargparse import ArgParser

def create_args():
    parser = ArgParser()
    parser.add('--db_section')
    parser.add('--reqnums')
    parser.add('--csv')
    args = parser.parse_args()
    return args

def make_reports(db=None,reqnums=None):
    if db is None and reqnums is None: 
        #1. get reqnums from last four days
        #2. create dataframe for all reqnums in both databases
        test_reqnums = [str(r) for r in query.get_reqnums(query.cursor('db-destest'))]
        oper_reqnums = [str(r) for r in query.get_reqnums(query.cursor('db-desoper'))]
    else:
        if db=='db-destest':
            test_reqnums = reqnums
            oper_reqnums = None
        elif db=='db-desoper':
            oper_reqnums = reqnums
            test_reqnums = None
    if test_reqnums:
        df_test = pandas.DataFrame(
                query.processing_summary(query.cursor('db-destest'),','.join(test_reqnums)),
                columns = ['created_date','project','campaign','unitname','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host'])
        df_test_status = pandas.DataFrame(
                query.get_status(query.cursor('db-destest'),','.join(test_reqnums)),
                columns = ['unitname','reqnum','attnum','status'])
        df_test_nites = pandas.DataFrame(
                query.get_nites(query.cursor('db-destest'),','.join(test_reqnums)),
                columns = ['unitname','reqnum','attnum','nite'])
        try:
            df_test_expnum = pandas.DataFrame(
                query.get_expnum_info(query.cursor('db-destest'),','.join(test_reqnums)),
                columns = ['unitname','reqnum','attnum','expnum','band'])
        except: 
            df_test_expnum = pandas.DataFrame()
            df_test_expnum.insert(len(df_test_expnum.columns),'unitname',None)
            df_test_expnum.insert(len(df_test_expnum.columns),'reqnum',None)
            df_test_expnum.insert(len(df_test_expnum.columns),'attnum',None)
        for df in [df_test_status,df_test_expnum,df_test_nites]:
            df_test = pandas.merge(df_test,df,on=['unitname','reqnum','attnum'],how='left',suffixes=('_orig',''))
        df_test['db'] = 'db-destest'
    else:
        df_test = pandas.DataFrame()
    if oper_reqnums:
        df_oper = pandas.DataFrame(
                query.processing_summary(query.cursor('db-desoper'),','.join(oper_reqnums)),
                columns = ['created_date','project','campaign','unitname','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host'])
        df_oper_status = pandas.DataFrame(
                query.get_status(query.cursor('db-desoper'),','.join(oper_reqnums)),
                columns = ['unitname','reqnum','attnum','status'])
        df_oper_nites = pandas.DataFrame(
                query.get_nites(query.cursor('db-desoper'),','.join(oper_reqnums)),
                columns = ['unitname','reqnum','attnum','nite'])
        df_oper_expnum = pandas.DataFrame(
                query.get_expnum_info(query.cursor('db-desoper'),','.join(oper_reqnums)),
                columns = ['unitname','reqnum','attnum','expnum','band'])
        for df in [df_oper_status,df_oper_expnum,df_oper_nites]:
            df_oper = pandas.merge(df_oper,df,on=['unitname','reqnum','attnum'],how='left',suffixes=('_orig',''))
        df_oper['db'] ='db-desoper'
    else:
        df_oper = pandas.DataFrame()
    dfs = [df_oper,df_test]
    df_master = pandas.concat(dfs)
    updated = "#{0}".format(datetime.now())
    with open(csv_path,'w') as csv:
        csv.write('%s\n' % updated)
    df_master.to_csv(csv_path,index=False,mode='a')

    # Make plots html
    for reqnum,group in df_master.groupby(by=['reqnum']):
        df,columns,reqnum,updated = get_data.processing_detail(group.db.unique()[0],reqnum,group,updated=updated)
        df2 = df.dropna()
        df_pass = df[df.status==0]

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
            fails = plotter.plot_status_per_host(df)
            plots.append(fails)
        except:
            pass
        try:
            if len(df_teff.t_eff):
                teff =plotter.plot_t_eff(df_teff[(df_teff.t_eff !='None')])
                for p in teff: plots.append(p)
        except:
            pass
        
        # Creating output path
        path = '/work/devel/mjohns44/git/desdm-dash/app/templates/reports/{reqnum}'.format(reqnum=reqnum)
        if not os.path.isdir(path): os.makedirs(path)

        # Writing CSV
        req_csv = '{reqnum}.csv'.format(reqnum=reqnum)
        df.to_csv(os.path.join(path,req_csv))

        # Writing plots to HTML
        html = file_html(vplot(*plots),INLINE,'plots')
        filename = 'plots_{reqnum}.html'.format(reqnum=reqnum)
        includepath = 'reports/{reqnum}/{file}'.format(reqnum=reqnum,file=filename)
        filepath = os.path.join(path,filename)
        with open(filepath,'w') as h:
            h.write('<center>\n')
            h.write(html)
            h.write('</center>\n')

        # Writing reports to HTML
        template = app.jinja_env.get_template('processing_detail_plots.html')
        output_from_parsed_template = template.render(
                        columns=columns,df=df,reqnum=reqnum,
                        db = group.db.unique(), updated=updated,filename=includepath)
        reportfile = "report_{reqnum}.html".format(reqnum=reqnum)
        reportpath = os.path.join(path,reportfile)
        with open(reportpath, "wb") as fh:
            fh.write(output_from_parsed_template)

if __name__ =='__main__':
    args = create_args()
    if args.reqnums:
        reqnums = [str(r) for r in args.reqnums.split(',')]
    else:
        reqnums = None
    if args.db_section:
        db = args.db_section
    else:
        db = None
    if args.csv:
        csv_path = args.csv
    else:
        csv_path = '/work/devel/mjohns44/git/desdm-dash/app/static/processing.csv'

    make_reports(db=db,reqnums=reqnums)
