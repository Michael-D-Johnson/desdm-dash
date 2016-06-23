#! /usr/bin/env python
import os
import sys
import pandas
import time
from datetime import datetime
import query

csv_path = '/work/devel/mjohns44/git/desdm-dash/app/static/processing.csv'
templates = '/work/devel/mjohns44/git/desdm-dash/app/templates/reports'

def create_coadd_map(section, tag):
    all_df = pandas.DataFrame(query.query_all_tiles(query.connect_to_db(section)[1]), columns = ['tilename','decc1','rac1','decc2','rac2','decc3','rac3','decc4','rac4'])
    processed_df = pandas.DataFrame(query.query_processed_tiles(query.connect_to_db(section)[0], query.connect_to_db(section)[1],','.join([tag])), columns = ['reqnum','tilename','attnum','id','status'])
    band_df = pandas.DataFrame(query.query_band_info(query.connect_to_db('db-desoper')[1]), columns = ['tilename','dmedian'])
    return all_df, processed_df, band_df

def create_des_usage(section):
    df = pandas.DataFrame(query.query_desdf(connect_to_db('destest')[1]), columns = ['filesystem','total_size','used','available','use_percent','mounted','submittime'])
    return df

def processing_archive():
    reqnums = os.listdir(templates)
    return reqnums

def processing_summary(db,project,df=None):
    if df is None:
        try: 
            df = pandas.read_csv(csv_path,skiprows=1)
            with open(csv_path,'r') as csvfile:
                updated = csvfile.readlines()[0]
        except (ValueError,IOError):
            updated = '#{time}'.format(time=datetime.now())
            df = pandas.DataFrame(columns=['created_date','project','campaign','unitname','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host','db']) 
    else:
        updated = '#{time}'.format(time=datetime.now()) 

    df = df.sort(columns=['reqnum','unitname','attnum'],ascending=False)
    df = df.fillna(-99)
    if project =='TEST':
        df_oper = df[(df.project != 'OPS') & (df.db=='db-desoper')] 
        df_test = df[(df.db=='db-destest')]
        df = pandas.concat([df_test,df_oper])
    else:
        df = df[(df.project == 'OPS') & (df.db =='db-desoper')]
    columns = ['operator','project','campaign','pipeline','submit_site','target_site','reqnum','nite','batch size','passed','failed','unknown','remaining']
    df_op = df.groupby(by=['reqnum'])
    current_dict,rest_dict = [],[]
    for req,group in sorted(df_op,reverse=True):
        req = int(float(req))
        orig_nitelist = sorted(group['nite'].unique())
        pipeline = group['pipeline'].unique()[0]
        if len(orig_nitelist) > 1:
            orig_nitelist_cp = list(orig_nitelist)
            if -99 in orig_nitelist_cp:
                orig_nitelist_cp.remove(-99)
                if orig_nitelist_cp:
                    if len(orig_nitelist_cp) >1:
                        nitelist = str('NA' + ', ' + str(int(orig_nitelist_cp[0])) + ' - ' + int(orig_nitelist_cp[-1]))
                    else:
                        nitelist = str('NA' + ', ' + str(int(orig_nitelist_cp[-1])))
            else:
                nitelist = str(int(orig_nitelist[0])) + ' - ' + str(int(orig_nitelist[-1]))
        else:
            nitelist = int(orig_nitelist[0])
        if pipeline =='supercal':
            nites = orig_nitelist[0].split('t')
            nite1,nite2 = nites[0],nites[0][:4]+nites[1]
            nitelist = str(int(nite1)) + ' - ' + str(int(nite2))
        if project =='TEST':
            total_batch_size = query.basic_batch_query(query.connect_to_db(db)[1],int(req))
        else:
            total_batch_size = query.batch_size_query(query.connect_to_db(db)[1],nitelist,int(req),pipeline)
        passed_df =  group[group.status==0].drop_duplicates(['unitname'])
        passed = passed_df['status'].count()
        failed_df = group[~group.status.isin([0,-99])].drop_duplicates(['unitname'])
        failed = failed_df['status'].count()
        try: unknown = group['status'][group.status == -99].count()
        except: unknown = 0
        try:
            target_site = ', '.join(group[group.status.isin([-99])].sort(columns=['created_date'])['target_site'].unique())
            if not target_site:
                target_site =group.sort(columns=['created_date'])['target_site'].unique()[-1]
        except:
            target_site =group.sort(columns=['created_date'])['target_site'].unique()[-1]
        try:
            submit_site = ', '.join([site.split('.')[0] for site in group[group.status.isin([-99])].sort(columns=['created_date'])['submit_site'].unique()])
            if not submit_site:
                submit_site = [site.split('.')[0] for site in group.sort(columns=['created_date'])['submit_site'].unique()][-1]
        except:
            submit_site = [site.split('.')[0] for site in group.sort(columns=['created_date'])['submit_site'].unique()][-1]

        remaining = int(total_batch_size)-int(passed) 
        if remaining < 0:
            remaining = 0
        if len(orig_nitelist) == 1:
            nitelist = int(orig_nitelist[0])
            if nitelist == -99: nitelist = 'NA'
        req_dict = {'remaining':remaining,'operator':group.operator.unique()[0],
                    'batch size':total_batch_size,
                    'reqnum':req,'passed':passed,'failed':failed,'unknown':unknown,
                    'nite':nitelist,
                    'submit_site':submit_site,
                    'target_site':target_site,
                    'campaign':group.campaign.unique()[0],
                    'project':group.project.unique()[0],
                    'pipeline':pipeline}
        if unknown: current_dict.append(req_dict)
        else: rest_dict.append(req_dict)
    try: current_dict
    except: current_dict = []
    try: rest_dict
    except: rest_dict = []
    try: columns
    except: columns = []
    return (current_dict,rest_dict,columns,updated)

def processing_detail(db,reqnum,df=None,updated=None):
    if df is None:
        try: 
            df = pandas.read_csv(csv_path,skiprows=1)
            with open(csv_path,'r') as csvfile:
                updated = csvfile.readlines()[0]
        except IOError: 
            sys.exit()
    df = df[df.reqnum==int(reqnum)]
    if not len(df):
        df = pandas.DataFrame(columns=['project','campaign','pipeline','reqnum','unitname','attnum','status','data_state','operator','target_site','submit_site','exec_host','start_time','end_time','total time','assessment','t_eff'])
    else:
        columns = ['project','campaign','pipeline','reqnum','unitname','attnum','status','data_state','operator','target_site','submit_site','exec_host','start_time','end_time','total time','assessment','t_eff']

        df.insert(len(df.columns),'assessment', None)
        df.insert(len(df.columns),'program', None)
        try:
            df.insert(len(df.columns),'t_eff', None)
        except: pass
        df.insert(13,'total time', None)
        def rename(row):
            row['submit_site'] = row['submit_site'].split('.')[0]
            return row['submit_site']

        df['submit_site'] = df.apply(rename,axis=1)
        df = df.sort(columns=['reqnum','unitname','attnum','start_time'],ascending=False)
        df_attempt = df.groupby(by=['unitname','reqnum','attnum'])
        for name,group in df_attempt:
            df = df.drop_duplicates(subset=['unitname','reqnum','attnum'])
            index = df[(df['unitname']==name[0]) & (df['reqnum']==name[1]) & (df['attnum']==name[2])].index[0]
            df.loc[index,('start_time','end_time')] = group['start_time'].min(),group['end_time'].max()
            try:
                start = datetime.strptime(str(group['start_time'].min())[:19],'%Y-%m-%d %H:%M:%S')
                end = datetime.strptime(str(group['end_time'].max())[:19],'%Y-%m-%d %H:%M:%S')

                total_time = (end - start)/pandas.Timedelta(hours=1)
                df.loc[index,('total time')] = round(total_time,3)
            except:
                pass
            pipeline = group.pipeline.unique()
            assess_query_results = query.assess_query(query.connect_to_db(db)[1],df,index,name,pipeline)

            try:
                assess,t_eff,program = assess_query_results[0][0],assess_query_results[0][1],assess_query_results[0][2]
            except:
                assess,t_eff,program = 'None','None','None'

            df.loc[index,('assessment')] = assess
            try: df.loc[index,('t_eff')] = round(float(t_eff),4)
            except: df.loc[index,('t_eff')] = t_eff
            df.loc[index,('program')] = program

        return (df,columns,reqnum,updated)
