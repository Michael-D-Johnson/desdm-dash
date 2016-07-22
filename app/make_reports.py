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
        test_reqnums = [str(r) for r in query.get_reqnums(query.connect_to_db('db-destest')[1])]
        oper_reqnums = [str(r) for r in query.get_reqnums(query.connect_to_db('db-desoper')[1])]
    else:
        if db=='db-destest':
            test_reqnums = reqnums
            oper_reqnums = None
        elif db=='db-desoper':
            oper_reqnums = reqnums
            test_reqnums = None
    if test_reqnums:
        df_test = pandas.DataFrame(
                query.processing_summary(query.connect_to_db('db-destest')[1],','.join(test_reqnums)),
                columns = ['created_date','project','campaign','unitname','reqnum','attnum','pfw_attempt_id','status',
                           'data_state','operator','pipeline','start_time','end_time','target_site','submit_site',
                           'exec_host'])
        df_test_status = pandas.DataFrame(
                query.get_status(query.connect_to_db('db-destest')[1],','.join(test_reqnums)),
                columns = ['unitname','reqnum','attnum','pfw_attempt_id','status'])
        df_test_nites = pandas.DataFrame(
                query.get_nites(query.connect_to_db('db-destest')[1],','.join(test_reqnums)),
                columns = ['unitname','reqnum','attnum','pfw_attempt_id','nite'])
        try:
            df_test_expnum = pandas.DataFrame(
                query.get_expnum_info(query.connect_to_db('db-destest')[1],','.join(test_reqnums)),
                columns = ['unitname','reqnum','attnum','expnum','band'])
        except: 
            df_test_expnum = pandas.DataFrame()
            df_test_expnum.insert(len(df_test_expnum.columns),'unitname',None)
            df_test_expnum.insert(len(df_test_expnum.columns),'reqnum',None)
            df_test_expnum.insert(len(df_test_expnum.columns),'attnum',None)
            df_test_expnum.insert(len(df_test_expnum.columns),'pfw_attempt_id',None)
        for df in [df_test_status,df_test_expnum,df_test_nites]:
            df_test = pandas.merge(df_test,df,on=['unitname','reqnum','attnum','pfw_attempt_id'],how='left',
                                   suffixes=('_orig',''))
        df_test['db'] = 'db-destest'
    else:
        df_test = pandas.DataFrame()
    if oper_reqnums:
        df_oper = pandas.DataFrame(
                query.processing_summary(query.connect_to_db('db-desoper')[1],','.join(oper_reqnums)),
                columns = ['created_date','project','campaign','unitname','reqnum','attnum','pfw_attempt_id',
                           'status','data_state','operator','pipeline','start_time','end_time','target_site',
                           'submit_site','exec_host'])
        df_oper_status = pandas.DataFrame(
                query.get_status(query.connect_to_db('db-desoper')[1],','.join(oper_reqnums)),
                columns = ['unitname','reqnum','attnum','pfw_attempt_id','status'])
        df_oper_nites = pandas.DataFrame(
                query.get_nites(query.connect_to_db('db-desoper')[1],','.join(oper_reqnums)),
                columns = ['unitname','reqnum','attnum','pfw_attempt_id','nite'])
        df_oper_expnum = pandas.DataFrame(
                query.get_expnum_info(query.connect_to_db('db-desoper')[1],','.join(oper_reqnums)),
                columns = ['unitname','reqnum','attnum','pfw_attempt_id','expnum','band'])
        for df in [df_oper_status,df_oper_expnum,df_oper_nites]:
            df_oper = pandas.merge(df_oper,df,on=['unitname','reqnum','attnum','pfw_attempt_id'],how='left',
                                   suffixes=('_orig',''))
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
    for name,group in df_master.groupby(by=['reqnum','db']):
        reqnum = name[0]
        db = name[1]
        df,columns,reqnum,updated = get_data.processing_detail(group.db.unique()[0],reqnum,group,updated=updated)
        df2 = df.dropna()
        df_pass = df[df.status==0]

        df_teff = df_pass[df_pass.t_eff != -1].dropna()
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
            if not df_pass.empty:
                exec_time = plotter.plot_exec_wall_time(df_pass)
                plots.append(exec_time)
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
        path = os.path.join(app.config["STATIC_PATH"],"reports/{reqnum}".format(reqnum=reqnum))
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

def make_system_plots(sys_df, res, des_df):
    
    plots = []
    
    try:
         p_du = plotter.data_usage_plot(des_df)
         plots.append(p_du)
    except:
        print "data_usage failed"
        pass
    try:
        p_ttfts = plotter.plot_tape_tar_file_transfer_status(sys_df['run_time'],sys_df['number_transferred'],sys_df['number_not_transferred'])
        plots.append(p_ttfts)
    except:
        print "tape_tar_file failed"
        pass
    try:
        p_bs = plotter.plot_backup_size(sys_df['run_time'],sys_df['size_transferred'],sys_df['size_to_be_transferred'])
        plots.append(p_bs)
    except:
        print "bakcup_size failed"
        pass
    try:
        p_prp = plotter.plot_pipeline_run_progress(sys_df['run_time'],sys_df['pipe_processed'],sys_df['pipe_to_be_processed'])
        plots.append(p_prp)
    except:
        print "pipeline_run failed"
        pass
    try:
        p_dtss = plotter.plot_dts_status(sys_df['run_time'],sys_df['raw_processed'],sys_df['raw_to_be_processed'])
        plots.append(p_dtss)
    except:
        print "dts_status failed"
        pass
    try:
        p_ts = plotter.plot_system_transfer_rates(res['tdate'],res['tsize'],res['tav'])
        plots.append(p_ts)
    except:
        print "system_transfer failed"
        pass

    html_vplots = vplot(*plots)

    return html_vplots

def make_coadd_html():
    try:
        all_df, processed_df, band_df = get_data.create_coadd_map('db-destest',"ME_TEST")
        p = plotter.plot_coadd(all_df, processed_df, band_df, "ME_TEST")
    except:
        pass    

    # Creating output path
    path = os.path.join(app.config["STATIC_PATH"],"reports/coadd/")
    if not os.path.isdir(path): os.makedirs(path)

    # Writing plots to HTML    
    html = file_html(vplot(p),INLINE,'coadd')
    filename = 'coadd_map_save.html'
    includepath = 'reports/coadd/coadd_map_save.html'
    filepath = os.path.join(path,filename)
    with open(filepath,'w') as h:
        h.write('<h5> Last updated on: %s </h5>' % "{0}".format(datetime.now()))
        h.write('<center>\n')
        h.write(html)
        h.write('</center>\n')

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
        csv_path = os.path.join(app.config["STATIC_PATH"],"processing.csv")

    make_reports(db=db,reqnums=reqnums)
    make_coadd_html() 
