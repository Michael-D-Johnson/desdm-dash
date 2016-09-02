#!/usr/bin/env python

import os
import pandas as pd
import pytz
from datetime import datetime, timedelta, date

import query
import cron_funcs as cf

def main():

    ### Set time frame of midnight yesterday to midnight of today ###
    etime = datetime.combine(date.today(), datetime.min.time())
    stime = etime - timedelta(days=int(1))

    ### Fetch Data ###
    try:
        send_df = pd.DataFrame(query.query_exptime(query.connect_to_db('db-sispi')[1], stime, etime), columns = ['exptime', 'filename'])
    except:
        send_df = pd.DataFrame(columns = ['exptime', 'filename'])
        pass
    accept_df = cf.get_cron_accept_time()
    ingest_df = cf.get_cron_ingest_time()

    ### Standardize file names for merge ###
    trimed_fn = []
    for i, line in send_df.iterrows():
        trimed_fn.append(os.path.basename(line['filename'].split(':')[1]))
    send_df['filename']=trimed_fn

    ### Merge Dataframes ###
    # will need to be changed to outer and put nan handling in.
    tmp_df = pd.merge(ingest_df, accept_df, how='inner', on=['filename'])
    merge_df = pd.merge(tmp_df, send_df, how='left', on=['filename'])

    ### Standardize timezones to UTC and drop manifests ###
    fn_df = pd.DataFrame()
    adjust_at, adjust_it, adjust_et, adjust_fn = [],[],[],[]
    localtz = pytz.timezone('US/Central')
    for i, line in merge_df.iterrows():
        if ".fits.fz" in line['filename']:
            adjust_at.append(localtz.localize(line['accept_time']).astimezone (pytz.utc))
            adjust_it.append(localtz.localize(line['ingest_time']).astimezone (pytz.utc))
            adjust_et.append(line['exptime'])
            adjust_fn.append(line['filename'])
    fn_df['accept_time'] = adjust_at
    fn_df['ingest_time'] = adjust_it
    fn_df['exptime'] = adjust_et
    fn_df['filename'] = adjust_fn

    ### Add Delivered column ###
    fn_df['delivered'] = cf.create_delivered(fn_df)

    ### Insert to db ###
    # issues with insert statement currently.
    cf.submit_dts_logs(query.connect_to_db('db-destest'), fn_df)

if __name__=='__main__':
    main()

