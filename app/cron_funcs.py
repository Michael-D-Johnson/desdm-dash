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
def submit_dts_logs(cur,df):

    for i, row in df.iterrows():
        submit = "insert into abode.dts_delay (SISPI_TIME, ACCEPT_TIME, INGEST_TIME, DELIVERED, FILENAME) values "
        submit += "(\'%s\', \'%s\', \'%s\', \'%s\', \'%s\')" % (row[0], row[1], row[2], row[3], row[4])
        cur.execute(submit)

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

### Things to move into cron_funcs ###
# get_desdf() from get_data.py
# submit_desdf() from query.py
