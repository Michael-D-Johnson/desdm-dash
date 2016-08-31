#! /usr/bin/env python
import subprocess
import os
import sys
import pytz
import pandas
import time
from datetime import datetime, timedelta
import math
import query

### Submits DTS_delay info to DB ###
### Run once a day by dts_submit.sh ###
def submit_dts_logs(connection, df):

    cur = connection[1]
    dbh = connection[0]

    for i, row in df.iterrows():
        print row     

        submit = "insert into abode.dts_delay (SISPI_TIME, ACCEPT_TIME, INGEST_TIME, DELIVERED, FILENAME) values "
        submit += "(TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), \'%s\', \'%s\')" % (row['exptime'].strftime('%Y-%m-%d %H:%M:%S'), row['accept_time'].strftime('%Y-%m-%d %H:%M:%S'), row['ingest_time'].strftime('%Y-%m-%d %H:%M:%S'), row['delivered'], row['filename'])
       # submit += "(TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), \'%s\', \'%s\')" % (dbh.get_named_bind_string('exptime'), 
#                                                           dbh.get_named_bind_string('accept_time'), 
#                                                           dbh.get_named_bind_string('ingest_time'), 
#                                                           dbh.get_named_bind_string('delivered'), 
#                                                           dbh.get_named_bind_string('filename'))
        
        #print "(TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), TO_DATE(\'%s\',\'YYYY-MM-DD HH24:MI:SS\'), \'%s\', \'%s\')" % (row['exptime'].strftime('%Y-%m-%d %H:%M:%S'), row['accept_time'].strftime('%Y-%m-%d %H:%M:%S'), row['ingest_time'].strftime('%Y-%m-%d %H:%M:%S'), row['delivered'], row['filename'])
        #print "(\'%s\', \'%s\', \'%s\', \'%s\', \'%s\')" % (row['exptime'], row['accept_time'], row['ingest_time'], row['delivered'], row['filename'])

#        params = {'exptime': row['exptime'].strftime('%Y-%m-%d %H:%M:%S'),
#                  'accept_time': row['accept_time'].strftime('%Y-%m-%d %H:%M:%S'),
#                  'ingest_time': row['ingest_time'].strftime('%Y-%m-%d %H:%M:%S'),
#                  'delivered': row['delivered'],
#                  'filename': row['filename']}

        #print submit
        
        cur.execute(submit)#, params)

    cur.execute("commit")
    return 0

### Gets last days accept times for submission ###
### Run once a day by dts_submit.sh ###
def get_cron_accept_time():

    times, filenames = [],[]
    curdate = datetime.now() - timedelta(days=int(1))

    ### Desar3 ###
    permlocation = "/local/dts_desdm/logs/accept_dts_delivery_logs/%s/%s/%s_dts_accept.log" % (curdate.year, curdate.strftime('%m'), str(curdate.year)+str(curdate.strftime('%m'))+str(curdate.strftime('%d')))
    ### Testing on deslogin ###
    templocation = "/work/devel/abode/logs/dts_logs/accept_dts_delivery_logs/%s/%s/%s_dts_accept.log" % (curdate.year, curdate.strftime('%m'), str(curdate.year)+str(curdate.strftime('%m'))+str(curdate.strftime('%d')))

    try:
        with open(permlocation) as fh:
            for line in fh:
                if "accept file" in line:
                    contents = line.split(' ')
                    if os.path.basename(contents[6].rstrip('\n')) not in filenames:
                        times.append(datetime.strptime(contents[0]+" "+contents[1], '%Y/%m/%d %H:%M:%S'))
                        filenames.append(os.path.basename(contents[6].rstrip('\n')))
    except:
        print "No accept log for: " + str(curdate)
        pass

    df = pandas.DataFrame()
    df['accept_time'] = times
    df['filename'] = filenames

    return df

### Gets last days ingest times for submission ###
### Run once a day by dts_submit.sh ###
def get_cron_ingest_time():

    times, filenames = [],[]
    curdate = datetime.now() - timedelta(days=int(1))

    ### Desar3 ###
    permlocation = "/local/dts_desdm/logs/dts_file_handler_logs/%s/%s/%s-handle_file_from_dts.log" % (curdate.year, curdate.strftime('%m'), str(curdate.year)+str(curdate.strftime('%m'))+str(curdate.strftime('%d')))
    ### Testing on Deslogin ###
    templocation = "/work/devel/abode/logs/dts_logs/dts_file_handler_logs/%s/%s/%s-handle_file_from_dts.log" % (curdate.year, curdate.strftime('%m'), str(curdate.year)+str(curdate.strftime('%m'))+str(curdate.strftime('%d')))

    try:
        with open(permlocation) as fh:
            for line in fh:
                if "move_file_to_archive" in line:
                    contents = line.split(' ')
                    times.append(datetime.strptime(contents[0]+" "+contents[1], '%Y/%m/%d %H:%M:%S'))
                    filenames.append(os.path.basename(contents[5].rstrip(':')))
    except:
        print "No ingest log for: " + str(curdate)
        pass

    df = pandas.DataFrame()
    df['ingest_time'] = times
    df['filename'] = filenames

    return df

### Calculates delivered column for dts_delay table ###
### Run once a day by dts_submit.sh ###
def create_delivered(df):
  
    delivered_list = []
    for i,line in df.iterrows():
        if line['ingest_time'] is not None:
            delivered_list.append("T")
        else:
            delivered_list.append("F")

    return delivered_list

### Gets data from the cmd line desdf ###
### Run evey 12 hours at noon and midnight ###
def get_desdf():
    df = pandas.DataFrame()
    for column in range(1,7):
        p1 = subprocess.Popen( "/usr/local/bin/desdf | awk {\'print $%i\'}" % column, stdout=subprocess.PIPE, shell=True)
        output = p1.stdout.read().split('\n')
        df[output[0]] = [output[1], output[2], output[3], output[4], output[5], output[6], output[7], output[8]]
    st = 'sysdate'
    df['submittime'] = [st,st,st,st,st,st,st,st]

    df.columns = ['FILESYSTEM','TOTAL_SIZE','USED','AVALIABLE','USE_PERCENT','MOUNTED','SUBMITTIME']
    return df

### Submits desdf data to destest db ###
### Run every 12 hours at noon and midnight ###
def submit_desdf(cur,df):
    ### List of mounts for insert ###
    mounts = ["/home /home2 /usr/apps","/work","OPS inputs and ACT","OPS multi-epoch","OPS single-epoch","DTS archive","Scratch and db_backup"," "]
    for i, row in df.iterrows():
        submit = "insert into abode.data_usage (FILESYSTEM, TOTAL_SIZE, USED, AVAILABLE, USE_PERCENT, MOUNTED, SUBMITTIME) values "
        submit += "(\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', %s)" % (row[0], row[1], row[2], row[3], row[4], mounts[i], row[6])
        cur.execute(submit)

    cur.execute("commit")
    return 0

