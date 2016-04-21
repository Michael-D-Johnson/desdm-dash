#! /usr/bin/env python
import os
import sys
import pandas
from datetime import datetime
import query
csv_path = '/work/devel/mjohns44/git/desdm-dash/app/static/processing.csv'

def processing_summary(db,project,df=None):
    if not df:
        try: 
            df = pandas.read_csv(csv_path,skiprows=1)
            with open(csv_path,'r') as csvfile:
                updated = csvfile.readlines()[0]
        except IOError: sys.exit() 

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
            nitelist = str(int(orig_nitelist[0])) + ' - ' + str(int(orig_nitelist[-1]))
        else:
            nitelist = int(orig_nitelist[0])
        if pipeline =='supercal':
            nites = orig_nitelist[0].split('t')
            nite1,nite2 = nites[0],nites[0][:4]+nites[1]
            nitelist = str(int(nite1)) + ' - ' + str(int(nite2))
        if project =='TEST':
            total_batch_size = query.basic_batch_query(query.cursor(db),int(req))
        else:
            total_batch_size = query.batch_size_query(query.cursor(db),nitelist,int(req),pipeline)
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

def processing_detail(db,reqnum):
    try: 
        df = pandas.read_csv(csv_path,skiprows=1)
        with open(csv_path,'r') as csvfile:
            updated = csvfile.readlines()[0]
    except IOError: 
        sys.exit()
    df = df[df.reqnum==int(reqnum)]
    if not len(df):
        results = query.processing_basic(query.cursor(db),reqnum)
        df = pandas.DataFrame(results,columns=['created_date','project','campaign','unitname','reqnum','attnum','status','data_state','operator','pipeline','target_site','submit_site','exec_host'])
        df = df.sort(columns=['unitname','attnum'],ascending=False)
        columns = df.columns
        return (df,columns,reqnum,None)
    else:
        columns = ['project','campaign','pipeline','reqnum','unitname','attnum','status','data_state','operator','target_site','submit_site','exec_host','start_time','end_time','total time','assessment','t_eff']

        df.insert(len(df.columns),'assessment', None)
        df.insert(len(df.columns),'program', None)
        df.insert(len(df.columns),'t_eff', None)
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
                start = datetime.strptime(str(group['start_time'].min()[:19]),'%Y-%m-%d %H:%M:%S')
                end = datetime.strptime(str(group['end_time'].max()[:19]),'%Y-%m-%d %H:%M:%S')

                total_time = (end - start)/pandas.Timedelta(hours=1)
                df.loc[index,('total time')] = round(total_time,3)
            except:
                pass
            pipeline = group.pipeline.unique()
            assess_query_results = query.assess_query(query.cursor(db),df,index,name,pipeline)

            try:
                assess,t_eff,program = assess_query_results[0][0],assess_query_results[0][1],assess_query_results[0][2]
            except:
                assess,t_eff,program = 'None','None','None'

            df.loc[index,('assessment')] = assess
            df.loc[index,('t_eff')] = t_eff
            df.loc[index,('program')] = program

        mean_times =round(df[df.status==0]['total time'].mean(skipna=True),3)
        return (df,columns,reqnum,mean_times,updated)

if __name__ =='__main__':
    #1. get reqnums from last four days
    #2. create dataframe for all reqnums in both databases
    import datetime
    date1 = datetime.datetime.now()
    test_reqnums = [str(r) for r in query.get_reqnums(query.cursor('db-destest'))]
    oper_reqnums = [str(r) for r in query.get_reqnums(query.cursor('db-desoper'))]
    df_test = pandas.DataFrame(
                query.processing_summary(query.cursor('db-destest'),','.join(test_reqnums)),
                columns = ['created_date','project','campaign','unitname','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host'])
    df_test['db'] = 'db-destest'
    df_oper = pandas.DataFrame(
                query.processing_summary(query.cursor('db-desoper'),','.join(oper_reqnums)),
                columns = ['created_date','project','campaign','unitname','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host'])

    df_oper['db'] ='db-desoper'
    dfs = [df_oper,df_test]
    df_master = pandas.concat(dfs)
    """
    df_master.insert(len(df_master.columns),'expnum',None)
    df_master.insert(len(df_master.columns),'band',None)
    def update_row(row):
        print row['db'],row['reqnum'],row['unitname'],row['attnum']
        status = query.get_status(query.cursor(row['db']),row['reqnum'],row['unitname'],
                                                      row['attnum'])
        expnum,band = query.get_expnum_info(query.cursor(row['db']),row['reqnum'],row['unitname'],
                                                      row['attnum'])
        nites = query.get_nites(query.cursor(row['db']),row['reqnum'],row['unitname'],row['attnum'])
        print status,expnum,band
        if status: row['status'] = status[0]
        else: pass
        if expnum: row['expnum'],row['band'] = expnum,band
        else: pass
        if nites: row['nite'] = nites
        else: pass
        return row.loc['expnum','band','status']
        
    df_master.loc['expnum','band','status'].apply(update_row,axis=1)
    """
    with open(csv_path,'w') as csv:
        csv.write('#%s\n' % datetime.datetime.now())
    df_master.to_csv(csv_path,index=False,mode='a')
    date2 = datetime.datetime.now()
    total_time = date2-date1
    print total_time
 
