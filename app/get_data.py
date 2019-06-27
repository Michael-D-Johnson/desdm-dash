#! /usr/bin/env python
import os
import sys
import pandas as pd
from collections import deque
from datetime import datetime, timedelta
import query

csv_path = os.path.join(os.getenv("STATIC_PATH"),'reports/processing.csv')
templates = os.path.join(os.getenv("STATIC_PATH"),'reports')

def processing_archive():
    reqnums = [r.strip() for r in os.listdir(templates)]
    reqnums.remove('processing.csv')
    return reqnums

def processing_summary(db,project,df=None):
    if df is None:
        try: 
            df = pd.read_csv(csv_path,skiprows=1)
            with open(csv_path,'r') as csvfile:
                updated = csvfile.readlines()[0]
        except (ValueError,IOError):
            updated = '#{time}'.format(time=datetime.now())
            df = pd.DataFrame(columns=['created_date','project','campaign','pfw_attempt_id','reqnum','unitname','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host','db']) 
    else:
        updated = '#{time}'.format(time=datetime.now()) 
    df = df.sort_values(by=['reqnum','unitname','attnum'],ascending=False)
    df = df.fillna(-99)
    if project =='TEST':
        df_oper = df[(df.project != 'OPS') & (df.db=='desoper')] 
        df_test = df[(df.db=='destest')]
        df = pd.concat([df_test,df_oper])
    else:
        df = df[(df.project == 'OPS') & (df.db =='desoper')]
    columns = ['operator','project','campaign','pipeline','submit_site','target_site','reqnum','nite','batch size','passed','failed','unknown','remaining']
    df_op = df.groupby(by=['reqnum','db'])
    current_dict,rest_dict = [],[]
    for name,group in sorted(df_op,reverse=True):
        req = name[0]
        db = name[1]
        req = int(float(req))
        orig_nitelist = sorted(group['nite'].unique())
        pipeline = group['pipeline'].unique()[0]
        orig_nitelist_cp = list(orig_nitelist)
        if len(orig_nitelist) > 1:
            orig_nitelist_cp = list(orig_nitelist)
            if -99 in orig_nitelist_cp:
                orig_nitelist_cp.remove(-99)
                if orig_nitelist_cp:
                    if len(orig_nitelist_cp) >1:
                        nitelist = str('NA' + ', ' + str(int(orig_nitelist_cp[0])) + ' - ' + str(int(orig_nitelist_cp[-1])))
                    else:
                        nitelist = str('NA' + ', ' + str(orig_nitelist_cp[-1]))
            else:
                try:
                    nitelist = str(int(orig_nitelist[0])) + ' - ' + str(int(orig_nitelist[-1]))
                except:
                    nitelist = str(orig_nitelist[0]) + ' - ' + str(orig_nitelist[-1])
        else:
            try:
                nitelist = int(orig_nitelist[0])
            except:
                nitelist = str(orig_nitelist[0])
        if pipeline =='supercal':
            try:
                nites = str(orig_nitelist[0]).split('t')
            except:
                nites = str(orig_nitelist_cp[0]).split('t')
            if project =='TEST': 
                nites = str(orig_nitelist_cp[0]).split('t')
            try:
                nite1,nite2 = nites[0],nites[0][:4]+nites[1]
            except:
                nite1,nite2 = nites[0],nites[0][:4]+nites[0]
            nitelist = str(int(nite1)) + ' - ' + str(int(nite2))
        if project =='TEST':
            total_batch_size = query.basic_batch_query(query.connect_to_db(db),int(req))
        else:
            total_batch_size = query.batch_size_query(query.connect_to_db(db),nitelist,int(req),pipeline)
        passed_df =  group[group.status==0].drop_duplicates(['unitname'])
        passed = passed_df['status'].count()
        failed_df = group[~group.status.isin([0,-99])].drop_duplicates(['unitname'])
        failed = failed_df['status'].count()
        try: unknown = len(group[group.status == -99].pfw_attempt_id.unique())
        except: unknown = 0
        try:
            target_site = ', '.join(group[group.status.isin([-99])].sort_values(by=['created_date'])['target_site'].unique())
            if not target_site:
                target_site =group.sort_values(by=['created_date'])['target_site'].unique()[-1]
        except:
            target_site =group.sort_values(by=['created_date'])['target_site'].unique()[-1]
        try:
            submit_site = ', '.join([site.split('.')[0] for site in group[group.status.isin([-99])].sort_values(by=['created_date'])['submit_site'].unique()])
            if not submit_site:
                submit_site = [site.split('.')[0] for site in group.sort_values(by=['created_date'])['submit_site'].unique()][-1]
        except:
            submit_site = [site.split('.')[0] for site in group.sort_values(by=['created_date'])['submit_site'].unique()][-1]

        remaining = int(total_batch_size)-int(passed) 
        if remaining < 0:
            remaining = 0
        if len(orig_nitelist) == 1:
            try:
                nitelist = int(orig_nitelist[0])
            except:
                nitelist = str(orig_nitelist[0])
            if nitelist == -99: nitelist = 'NA'
        max_delta = timedelta(hours=0) 
        req_dict = {'remaining':remaining,'operator':', '.join(group.operator.unique()),
                    'batch size':total_batch_size,
                    'reqnum':req,'passed':passed,'failed':failed,'unknown':unknown,
                    'nite':nitelist,
                    'submit_site':submit_site,
                    'target_site':target_site,
                    'campaign':group.campaign.unique()[0],
                    'project':group.project.unique()[0],
                    'pipeline':pipeline,
                    'db':db}
        if unknown: current_dict.append(req_dict)
        else: rest_dict.append(req_dict)
    try: current_dict
    except: current_dict = []
    try: rest_dict
    except: rest_dict = []
    try: columns
    except: columns = []
    return (current_dict,rest_dict,columns,updated,pd.DataFrame(current_dict),pd.DataFrame(rest_dict))

def processing_detail(db,reqnum,df=None,updated=None):
    if df is None:
        try: 
            df = pd.read_csv(csv_path,skiprows=1)
            with open(csv_path,'r') as csvfile:
                updated = csvfile.readlines()[0]
        except IOError: 
            sys.exit()
    df = df[df.reqnum==int(reqnum)]
    if not len(df):
        df = pd.DataFrame(columns=['project','campaign','pipeline','pfw_attempt_id','reqnum','unitname','attnum','status','data_state','operator','target_site','submit_site','exec_host','start_time','end_time','total time','assessment','t_eff','b_eff','c_eff','f_eff','program'])
    else:
        columns = ['project','campaign','pipeline','pfw_attempt_id','reqnum','unitname','attnum','status','data_state','operator','target_site','submit_site','exec_host','start_time','end_time','total time','assessment','t_eff','b_eff','c_eff','f_eff','program']

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
        df = df.sort_values(by=['reqnum','unitname','attnum','start_time'],ascending=False)
        df_attempt = df.groupby(by=['unitname','reqnum','attnum'])
        for name,group in df_attempt:
            df = df.drop_duplicates(subset=['unitname','reqnum','attnum'])
            index = df[(df['unitname']==name[0]) & (df['reqnum']==name[1]) & (df['attnum']==name[2])].index[0]
            df.loc[index,('start_time','end_time')] = group['start_time'].min(),group['end_time'].max()
            try:
                start = datetime.strptime(str(group['start_time'].min())[:19],'%Y-%m-%d %H:%M:%S')
                end = datetime.strptime(str(group['end_time'].max())[:19],'%Y-%m-%d %H:%M:%S')

                total_time = (end - start)/pd.Timedelta(hours=1)
                df.loc[index,('total time')] = round(total_time,3)
            except:
                pass

            pipeline = group.pipeline.unique()
            assess_query_results = query.assess_query(query.connect_to_db(db),df,index,name,
                                                      group.pfw_attempt_id.unique()[0],pipeline)

            try:
                assess,t_eff,b_eff,c_eff,f_eff, program = assess_query_results[0][0],assess_query_results[0][1],\
                                                          assess_query_results[0][2],assess_query_results[0][3],\
                                                          assess_query_results[0][4],assess_query_results[0][5]
            except:
                assess,t_eff,b_eff,c_eff,f_eff, program = 'None','None','None','None','None','None'

            df.loc[index,('assessment')] = assess
            try:
                df.loc[index,('t_eff')] = round(float(t_eff),4)
                df.loc[index,('b_eff')] = round(float(b_eff),4)
                df.loc[index,('c_eff')] = round(float(c_eff),4)
                df.loc[index,('f_eff')] = round(float(f_eff),4)
            except:
                df.loc[index,('t_eff')] = t_eff
                df.loc[index,('b_eff')] = b_eff
                df.loc[index,('c_eff')] = c_eff
                df.loc[index,('f_eff')] = f_eff
            df.loc[index,('program')] = program

        return (df,columns,reqnum,updated)

def get_exec_job_data(data_df):
    ''' Change start and end time data into count of jobs running at any point '''
    pd.to_datetime(data_df['start_time'])
    pd.to_datetime(data_df['end_time'])

    data_df = data_df.sort_values(by=['start_time'])
    data_df = data_df[~(data_df.exec_host.isnull())]

    start_queue = deque([row for row in data_df.iterrows()])
    end_times = []
    cur_hostname_count = dict((hostname, 0) for hostname in set(data_df['exec_host']))
    final_hostname_count = dict((hostname, []) for hostname in set(data_df['exec_host']))
    cur_time = start_queue[0][1][0]
    start_time = start_queue[0][1][0]
    time_segment = timedelta(minutes=10)

    # Just for in case somebody needs to edit this in the future.
    # Data format is   [0]       [1][0]     [1][1]   [1][2] 
    #               (df_num, (start_time, end_time, exec_host))
    while len(start_queue) != 0 or len(end_times) != 0:
        # Check start_times
        while len(start_queue) != 0 and start_queue[0][1][0] < cur_time:
            cur_hostname_count[start_queue[0][1][2]] += 1
            end_times.append(start_queue.popleft())
        # Log counts
        for host in cur_hostname_count.keys():
            final_hostname_count[host].append(cur_hostname_count[host])
        # Check end_times
        if len(end_times) != 0:
            new_end_times = []
            for time in end_times:
                if time[1][1] < cur_time:
                    cur_hostname_count[time[1][2]] -= 1
                else:
                    new_end_times.append(time)
            end_times = new_end_times
        cur_time += time_segment

    return final_hostname_count, start_time
