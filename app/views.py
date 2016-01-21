#! /usr/bin/env python
from flask import render_template,url_for,send_file
import matplotlib
from opstoolkit import jiracmd
from despydb import DesDbi
import os
import pandas
import datetime
from app import app

matplotlib.use('Agg')

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/processing_summary')
def processing_summary():
    pandas.options.mode.chained_assignment = None
    dbh = DesDbi(os.getenv("DES_SERVICES"),'db-desoper')
    cur = dbh.cursor()
    query = "select distinct r.project,r.campaign,a.unitname,v.val,a.reqnum,a.attnum,t.status,a.data_state,a.operator,r.pipeline from pfw_job j,pfw_attempt a,pfw_attempt_val v,task t,pfw_request r where a.created_date >= sysdate-4 and t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=j.reqnum and a.unitname=j.unitname and a.attnum=j.attnum and a.reqnum=v.reqnum and a.unitname=v.unitname and a.attnum=v.attnum and key in ('nite','range')"
    cur.execute(query)
    df = pandas.DataFrame(cur.fetchall(),columns=['project','campaign','unitname','nite','reqnum','attnum','status','data_state','operator','pipeline'])
    df = df.sort(columns=['reqnum','unitname','attnum'],ascending=False)
    df = df.drop_duplicates(subset=['unitname','reqnum','attnum'])
    df = df.fillna('NULL')
    columns = ['operator','project','campaign','pipeline','reqnum','nite','total expnums','passed','failed','unknown','remaining']
    df_op = df.groupby(by=['operator'])
    all_dict = []
    for name,group in df_op:
        for req in sorted(group['reqnum'].unique(),reverse = True):
            nitelist = sorted(df[df.reqnum==req].loc[group.index,('nite')].dropna().unique())
            nitelist = ', '.join(nitelist)
            if df[df.reqnum==req]['pipeline'].unique()[0] == 'sne':
                expnum_query = "select count(*) from (select distinct field,band from exposure where nite in (%s) and field like 'SN%%' group by field,band)" % nitelist
            elif df[df.reqnum==req]['pipeline'].unique()[0] == 'finalcut':
                key = 'DESOPS-%s' % req
                summary = jiracmd.Jira('jira-desdm').get_issue(key).fields.summary
                expnum_query = "select count(*) from exposuretag where tag in ('%s')" % summary
            else:
                expnum_query = "select count(distinct expnum) from exposure where obstype='object' and object not like '%%pointing%%' and object not like '%%focus%%' and object not like '%%donut%%' and object not like '%%test%%' and object not like '%%junk%%' and nite in (%s)" % nitelist
            cur.execute(expnum_query)
            total_expnums = cur.fetchone()[0]
            #passed =  df['status'][(df.operator == name) & (df.status==0) & (df.reqnum == req)].count()
            passed_df =  df[(df.operator == name) & (df.status==0) & (df.reqnum == req)].drop_duplicates(['unitname'])
            passed = passed_df['status'][(df.operator == name) & (df.status==0) & (df.reqnum == req)].count()
            #failed = df['status'][(df.operator == name) & (df.status.isin([1,5,8,9,127,247])) & (df.reqnum == req)].count()
            failed_df = df[(df.operator == name) & (df.status.isin([1,5,8,9,127,247])) & (df.reqnum == req)].drop_duplicates(['unitname'])
            failed = failed_df['status'][(df.operator == name) & (df.status.isin([1,5,8,9,127,247])) & (df.reqnum == req)].count()
            try: unknown = df['status'][(df.operator == name) & (df.status =='NULL') & (df.reqnum==req)].count()
            except: unknown = 0
            remaining = int(total_expnums)-int(passed)-int(failed)
            if remaining < 0:
                remaining = 0
            if len(nitelist) == 1:
                nitelist = nitelist[0]
            req_dict = {'remaining':remaining,'operator':name,'total expnums':total_expnums,'reqnum':req,'passed':passed,'failed':failed,'unknown':unknown,
                        'nite':nitelist,
                        'campaign':df[df.reqnum==req].loc[group.index,('campaign')].dropna().unique()[0],
                        'project':df[df.reqnum==req].loc[group.index,('project')].dropna().unique()[0],
                        'pipeline':df[df.reqnum==req].loc[group.index,('pipeline')].dropna().unique()[0]}
            all_dict.append(req_dict)
    return render_template('processing_summary.html',columns=columns,dict_list=all_dict,df = df)

@app.route('/processing_detail/<reqnum>')
def processing_detail(reqnum):
    pandas.options.mode.chained_assignment = None
    dbh = DesDbi(os.getenv("DES_SERVICES"),'db-desoper')
    cur = dbh.cursor()

    query = "select distinct r.project,r.campaign,a.unitname,v.val,a.reqnum,a.attnum,t.status,a.data_state,a.operator,r.pipeline,j.condor_start_time,condor_end_time from pfw_job j,pfw_attempt a,pfw_attempt_val v,task t,pfw_request r where a.created_date >= sysdate-4 and t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=j.reqnum and a.unitname=j.unitname and a.attnum=j.attnum and a.reqnum=v.reqnum and a.unitname=v.unitname and a.attnum=v.attnum and key in ('nite','range') and a.reqnum=%s" % reqnum
    cur.execute(query)

    df = pandas.DataFrame(cur.fetchall(),columns=['project','campaign','unitname','nite','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time'])
    df.insert(len(df.columns),'assessment', None)
    df.insert(len(df.columns),'program', None)
    df.insert(len(df.columns),'t_eff', None)
    df.insert(12,'total time', None)

    df = df.sort(columns=['reqnum','unitname','attnum','start_time'],ascending=False)
    df_attempt = df.groupby(by=['unitname','reqnum','attnum'])
    for name,group in df_attempt:
        df = df.drop_duplicates(subset=['unitname','reqnum','attnum'])
        columns = df.columns
        index = df[(df['unitname']==name[0]) & (df['reqnum']==name[1]) & (df['attnum']==name[2])].index[0]
        df.loc[index,('start_time','end_time')] = group['start_time'].min(),group['end_time'].max()
        total_time = (group['end_time'].max() - group['start_time'].min())/pandas.Timedelta(hours=1)
        try:
            df.loc[index,('total time')] = round(total_time,3)
        except:
            pass
        if 'SN' in name[0]:
            camsym,field,band,seq = name[0].split('_')
            comment = field.strip('SN-') + band + ' ' + df.loc[index,('nite')]
            assess_q = "select distinct accepted,t_eff,program from firstcut_eval where analyst_comment='%s'" % (comment)
        else:
            if df.loc[index,('pipeline')] == 'finalcut':
                assess_q = "select distinct accepted,t_eff,program from finalcut_eval where unitname='%s' and reqnum='%s' and attnum='%s'" % (name[0],name[1],name[2])
            else:
                assess_q = "select distinct accepted,t_eff,program from firstcut_eval where unitname='%s' and reqnum='%s' and attnum='%s'" % (name[0],name[1],name[2])

        cur.execute(assess_q)
        results = cur.fetchall()
        try:
            assess,t_eff,program = results[0][0],results[0][1],results[0][2]
        except:
            assess = 'None'
            b_eff,c_eff,f_eff,t_eff = 'None','None','None','None'
            program = 'None'
        df.loc[index,('assessment')] = assess
        df.loc[index,('t_eff')] = t_eff
        df.loc[index,('program')] = program

    accepted_group = df.groupby(by=['program'])
    ax = df['total time'].fillna(0).hist(bins=24)
    mean_times =round(df['total time'].mean(skipna=True),3)
    fig = ax.get_figure()
    ax.set_title('Processing Times')
    ax.set_xlabel('Hours')
    ax.set_ylabel('Count')
    fig_name = 'figure_%s.png' % reqnum
    static_fig_name = 'completed_requests/r%s/%s' % (reqnum,fig_name)
    fig_path = '/Users/mjohns44/Code/GIT/desdm-dash/app/static/completed_requests/r%s' % (reqnum)
    if not os.path.isdir(fig_path): os.makedirs(fig_path)
    full_fig_path = os.path.join(fig_path,fig_name) 
    fig.savefig(full_fig_path)
    fig.clf()
    return render_template('processing_detail.html',columns=columns,df = df,reqnum=reqnum,plot=static_fig_name,mean_times=mean_times,accepted_group=accepted_group)

@app.route('/dts')
def dts():
    return render_template('dts_monitor.html')

@app.route('/backups')
def backups():
    return render_template('back_ups.html')
