#! /usr/bin/env python
import subprocess
import re
import os
import sys
import pandas as pd
import time
import pytz
from collections import deque
from datetime import datetime, timedelta, date
from dateutil.relativedelta import *
import math
import query
from difflib import SequenceMatcher

from app import app
csv_path = os.path.join(app.config["STATIC_PATH"],'processing.csv')

templates = os.path.join(app.config["STATIC_PATH"],'reports')
#csv_path = '/work/devel/mjohns44/git/desdm-dash/app/static/processing.csv'
#templates = '/work/devel/mjohns44/git/desdm-dash/app/static/reports'

def create_coadd_map(section, tag):
    all_df = pd.DataFrame(query.query_all_tiles(query.connect_to_db(section)[1]), columns = ['tilename','decc1','rac1','decc2','rac2','decc3','rac3','decc4','rac4'])
    processed_df = pd.DataFrame(query.query_processed_tiles(query.connect_to_db(section)[0], query.connect_to_db(section)[1],','.join([tag])), columns = ['reqnum','tilename','attnum','id','status'])
    band_df = pd.DataFrame(query.query_band_info(query.connect_to_db('db-desoper')[1]), columns = ['tilename','band','dmedian'])
    return all_df, processed_df, band_df

### Gets data for all plots on system plots page ###
def create_system_data(section, section2):
    ### Length of graph (Defult 14 days) ###
    start = datetime.now() - timedelta(days=int(14))
    res, df = query.get_system_info(start, query.connect_to_db(section)[1])

    ### Change from GB to TB ### 
    df['size_transferred'] /= math.pow(1024,4)
    df['size_to_be_transferred'] /= math.pow(1024,4)
    desdf_df = pd.DataFrame(query.query_desdf(query.connect_to_db(section2)[1]), columns = ['filesystem','total_size','used','available','use_percent','mounted','submittime'])
    return df, res, desdf_df

### Live Render version of get_ingest_time for dts plot ###
def get_ingest_time():

    times, filenames = [],[]
    curdate = datetime.now()

    ### Desar3 ###
    permlocation = "/local/dts_desdm/logs/dts_file_handler_logs/%s/%s/%s-handle_file_from_dts.log" % (curdate.year, curdate.strftime('%m'), str(curdate.year)+str(curdate.strftime('%m'))+str(curdate.strftime('%d')))
    try:
        with open(testlocation) as fh:
            for line in fh:
                if "move_file_to_archive" in line:
                    contents = line.split(' ')
                    times.append(datetime.strptime(contents[0]+" "+contents[1], '%Y/%m/%d %H:%M:%S'))
                    filenames.append(os.path.basename(contents[5].rstrip(':')))
    except:
        print "No ingest log for: " + str(curdate)
        pass

    df = pd.DataFrame()
    df['ingest_time'] = times
    df['filename'] = filenames

    return df

### Live Render version of get_accept_time for dts plot ###
def get_accept_time():

    times, filenames = [],[]
    curdate = datetime.now()

    ### Desar3 ###
    permlocation = "/local/dts_desdm/logs/accept_dts_delivery_logs/%s/%s/%s_dts_accept.log" % (curdate.year, curdate.strftime('%m'), str(curdate.year)+str(curdate.strftime('%m'))+str(curdate.strftime('%d')))
    try:
        with open(testlocation) as fh:
            for line in fh:
                if "accept file" in line:
                    contents = line.split(' ')
                    if os.path.basename(contents[6].rstrip('\n')) not in filenames:
                        times.append(datetime.strptime(contents[0]+" "+contents[1], '%Y/%m/%d %H:%M:%S'))
                        filenames.append(os.path.basename(contents[6].rstrip('\n')))
    except:
        print "No accept log for: " + str(curdate)
        pass

    df = pd.DataFrame()
    df['accept_time'] = times
    df['filename'] = filenames

    return df

def convert_timezones(df):

    localtz = pytz.timezone('US/Central')
    noaodiff, ncsadiff, totaldiff, xtime = [],[],[],[]
                                                        # Sort is depreciated as of 0.17.0
    df.sort(['sispi_time'], inplace=True)               # 0.16.0 and earilier
    #df.sort_values(['sispi_time'], inplace=True)       # 0.17.0

    for i,line in df.iterrows():
        accept_time = localtz.localize(line['accept_time'])
        ingest_time = localtz.localize(line['ingest_time'])
        try:
            totaldiff.append((ingest_time.astimezone (pytz.utc) - line['sispi_time'].astimezone (pytz.utc)).total_seconds()/60)
            if ((accept_time.astimezone (pytz.utc) - line['sispi_time'].astimezone (pytz.utc)).total_seconds()/60) < 0:
                print "##### WARNING: NEGITIVE NOAO TIME #####"
                print line['filename']
                print "Accept: ", accept_time
                print "Ingest: ", ingest_time
                print "Sispi:  ", line['sispi_time']
            noaodiff.append((accept_time.astimezone (pytz.utc) - line['sispi_time'].astimezone (pytz.utc)).total_seconds()/60)
            if ((ingest_time.astimezone (pytz.utc) - accept_time.astimezone (pytz.utc)).total_seconds()/60) < 0:
                print "##### WARNING: NEGITIVE NCSA TIME #####"
                print line['filename']
                print "Accept: ", accept_time
                print "Ingest: ", ingest_time
                print "Sispi:  ", line['sispi_time']
            ncsadiff.append((ingest_time.astimezone (pytz.utc) - accept_time.astimezone (pytz.utc)).total_seconds()/60)
            xtime.append(line['sispi_time'])
        except:
            print "##### WARNING: FILE FAILED TO BE ADDED TO PLOT #####"
            print line['filename']
            print "Accept: ", accept_time
            print "Ingest: ", ingest_time
            print "Sispi:  ", line['sispi_time']
            pass

    fn_df = pd.DataFrame()
    fn_df['total_time'] = totaldiff
    fn_df['ncsa_time'] = ncsadiff
    fn_df['noao_time'] = noaodiff
    fn_df['xtime'] = xtime

    return fn_df

def smooth_dts(df):
                                                   # Sort is depreciated as of 0.17.0
    df.sort(['xtime'], inplace=True)               # 0.16.0 and earilier
    #df.sort_values(['xtime'], inplace=True)       # 0.17.0

    current_time = df['xtime'].iloc[0]
    current_total,current_ncsa,current_noao,current_i = 0,0,0,0
    noaotime, ncsatime, totaltime, xtime = [],[],[],[]
    for i,x in df.iterrows():
        if (x['xtime'] - current_time).total_seconds() < 300:
            current_total += x['total_time']
            current_ncsa += x['ncsa_time']
            current_noao += x['noao_time']
        else:
            totaltime.append(current_total/(i - current_i))
            ncsatime.append(current_ncsa/(i - current_i))
            noaotime.append(current_noao/(i - current_i))
            xtime.append(current_time)

            if (x['xtime'] - current_time).total_seconds() > 900:
                totaltime.append(0)
                ncsatime.append(0)
                noaotime.append(0)
                xtime.append(current_time + timedelta(0,300))

                totaltime.append(0)
                ncsatime.append(0)
                noaotime.append(0)
                xtime.append(x['xtime'] - timedelta(0,300))

            current_time = x['xtime']
            current_total = x['total_time']
            current_ncsa = x['ncsa_time']
            current_noao = x['noao_time']
            current_i = i


    fn_df = pd.DataFrame()
    fn_df['total_time'] = totaltime
    fn_df['ncsa_time'] = ncsatime
    fn_df['noao_time'] = noaotime
    fn_df['xtime'] = xtime

    return fn_df

def average_dts(df, stime, days):
                                                   # Sort is depreciated as of 0.17.0
    df.sort(['xtime'], inplace=True)               # 0.16.0 and earilier
    #df.sort_values(['xtime'], inplace=True)       # 0.17.0

    graphtime = stime
    current_time = df['xtime'].iloc[0]
    current_total,current_ncsa,current_noao,current_i,current_extrema = 0,0,0,0,0
    noaotime, ncsatime, totaltime, xtime, extrema = [],[],[],[],[]
    for i,x in df.iterrows():
        if (graphtime - x['xtime']).total_seconds() > 0:
            if x['total_time'] < 1440*days:
                current_total += x['total_time']
                current_ncsa += x['ncsa_time']
                current_noao += x['noao_time']
            else:
                current_extrema = current_extrema + 1
        else:
            totaltime.append(current_total/(i - current_i))
            ncsatime.append(current_ncsa/(i - current_i))
            noaotime.append(current_noao/(i - current_i))
            xtime.append(current_time)
            extrema.append(current_extrema)
            graphtime = graphtime + relativedelta(months=+1)

            if x['total_time'] < 1440*days:
                current_time = x['xtime']
                current_total = x['total_time']
                current_ncsa = x['ncsa_time']
                current_noao = x['noao_time']
                current_i = i
                current_extrema = 0
            else:
                current_time = x['xtime']
                current_total = 0
                current_ncsa = 0
                current_noao = 0
                current_i = i
                current_extrema = 0

    fn_df = pd.DataFrame()
    fn_df['total_time'] = totaltime
    fn_df['ncsa_time'] = ncsatime
    fn_df['noao_time'] = noaotime
    fn_df['xtime'] = xtime
    fn_df['extrema'] = extrema
    
    return fn_df

def processing_archive():
    reqnums = os.listdir(templates)
    return reqnums

def decade_overview(db,df=None):
    cur = query.connect_to_db('db-decade')[1]
    if df is None:
        try: 
            df = pd.read_csv(csv_path,skiprows=1)
            with open(csv_path,'r') as csvfile:
                updated = csvfile.readlines()[0]
        except (ValueError,IOError):
            updated = '#{time}'.format(time=datetime.now())
            df = pd.DataFrame(columns=['created_date','project','propid','campaign','pfw_attempt_id','reqnum','unitname','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host','db']) 
    else:
        updated = '#{time}'.format(time=datetime.now()) 
    df = df.sort(columns=['reqnum','unitname','attnum'],ascending=False)
    df = df.fillna(-99)
    df = df[(df.db=='db-decade') & (df.propid !=-99)]
    columns = ['operator','project','campaign','pipeline','submit_site','target_site','propid','batch size','passed','failed','unknown','remaining']
    df_op = df.groupby(by=['propid'])
    current_dict,rest_dict = [],[]
    for name,group in sorted(df_op,reverse=True):
        propid = group['propid'].unique()[0]
        pipeline = group['pipeline'].unique()[0]
        total_batch_size = query.basic_propid_size(cur,propid)
        passed_df =  group[group.status==0].drop_duplicates(['unitname'])
        passed = query.get_passed_status(cur,propid)[0]
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
        req_dict = {'remaining':remaining,'operator':', '.join(group.operator.unique()),
                    'batch size':total_batch_size,
                    'propid':propid,'passed':passed,'failed':failed,'unknown':unknown,
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

def processing_summary(db,project,df=None):
    if df is None:
        try: 
            df = pd.read_csv(csv_path,skiprows=1)
            with open(csv_path,'r') as csvfile:
                updated = csvfile.readlines()[0]
        except (ValueError,IOError):
            updated = '#{time}'.format(time=datetime.now())
            df = pd.DataFrame(columns=['created_date','project','propid','campaign','pfw_attempt_id','reqnum','unitname','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_site','exec_host','db']) 
    else:
        updated = '#{time}'.format(time=datetime.now()) 
    df = df.sort(columns=['reqnum','unitname','attnum'],ascending=False)
    df = df.fillna(-99)
    if project =='TEST':
        df_oper = df[(df.project != 'OPS') & (df.db=='db-desoper')] 
        df_test = df[(df.db=='db-destest')]
        df = pd.concat([df_test,df_oper])
    elif project =='DECADE':
        df = df[(df.db=='db-decade')]
    else:
        df = df[(df.project == 'OPS') & (df.db =='db-desoper')]
    columns = ['operator','project','campaign','pipeline','submit_site','target_site','reqnum','nite','batch size','passed','failed','unknown','remaining']
    df_op = df.groupby(by=['reqnum','db'])
    current_dict,rest_dict = [],[]
    for name,group in sorted(df_op,reverse=True):
        req = name[0]
        db = name[1]
        req = int(float(req))
        try:
            propid = group['propid'].unique()[0]
        except:
            propid = None
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
                    'db':db,
                    'propid':propid}
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
        df = pd.DataFrame(columns=['project','campaign','pipeline','pfw_attempt_id','reqnum','unitname','attnum','status','data_state','operator','target_site','submit_site','exec_host','start_time','end_time','total time','assessment','t_eff',
                                       'b_eff','c_eff','f_eff','program','job_timing'])
    else:
        columns = ['project','campaign','pipeline','pfw_attempt_id','reqnum','unitname','attnum','status','data_state','operator','target_site','submit_site','exec_host','start_time','end_time','total time','assessment','t_eff','b_eff',
                   'c_eff','f_eff','program','job_timing']

        df.insert(len(df.columns),'assessment', None)
        df.insert(len(df.columns),'program', None)
        try:
            df.insert(len(df.columns),'t_eff', None)
        except: pass
        df.insert(13,'total time', None)
        df.insert(len(df.columns),'job_time',None)
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

                total_time = (end - start)/pd.Timedelta(hours=1)
                job_time = sum(group.job_timing.unique())/pd.Timedelta(hours=1)
                df.loc[index,('total time')] = round(total_time,3)
                df.loc[index,('job_time')] = job_time
            except:
                pass
    
            pipeline = group.pipeline.unique()
            assess_query_results = query.assess_query(query.connect_to_db(db)[1],df,index,name,
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

######################
# NOT USED CURRENTLY #
######################
# Was for errors plot
#def similar(a, b):
#    matcher = SequenceMatcher(None, a, b)
#    ratio = matcher.ratio()
#    longest_match = matcher.find_longest_match(0,len(a),0,len(b))
#    matching_blocks = matcher.get_matching_blocks()
#    string = []
#    for i,match in enumerate(matching_blocks):
#        m = a[match[0]:match[0]+match[2]]
#        string.append(m)
#        if i == len(match)-1:
#            continue
#        else:
#            string.append(' <---> ')
#    return ''.join(string)

def find_errors(message_dict):
    ''' Finds patterns dynamically by the : delimiter for the error summary plot'''
    error_info = {}
    prog = re.compile('error', flags=re.IGNORECASE)

    for message in message_dict['message']:
        result = prog.search(message)
        if result is not None:
            contents = message.split()
            for i in contents:
                result = prog.search(i)
                if result is not None:
                    if i not in error_info:
                        error_info[i] = ( message, 1 )
                    else:
                        error_info[i] = ( error_info[i][0], error_info[i][1] + 1)
        else:
            if message not in error_info:
                error_info[message] = ( message , 1 )
            else:
                error_info[message] = ( error_info[message][0], error_info[message][1] + 1)
    return error_info

def get_exec_job_data(data_df):
    ''' Change start and end time data into count of jobs running at any point '''
    pd.to_datetime(data_df['start_time'])
    pd.to_datetime(data_df['end_time'])
    data_df = data_df.sort('start_time')

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

####################
# added by ycchen
###################

def generate_stat_df():
    columns = [i[0] for i in query.get_stat_columns(query.connect_to_db("db-destest")[1])]
    df = pd.DataFrame(query.get_stat_data(query.connect_to_db("db-destest")[1]), columns = columns)
    updated = '#{time}'.format(time=datetime.now())
    return (df,updated)

def select_useful_data(df):
    df = df[df.TOTAL_TIME > 0]
    df = df[df.STATUS == 0]
    df = df[ (df.TARGET_SITE != "iforge-dev") & (df.TARGET_SITE != "fermigrid-sl6") & (df.TARGET_SITE != "fermigrid-ce") & (df.TARGET_SITE != "CampusClusterPrecal") ]
    df = df[df.PROJECT == "OPS"]
    df = df[ (df.PIPELINE == "firstcut") | (df.PIPELINE == "multiepoch") | (df.PIPELINE == "sne")]
    return df
