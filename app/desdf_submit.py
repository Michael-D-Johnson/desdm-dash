#!/usr/bin/env python

import query
import cron_funcs as cf

import os
import pandas as pd
import despydb.desdbi as desdbi
from despydb import DesDbi

def main():

    info_df = cf.get_desdf()

    # Direct pandas method if Oracle gets implemented.
#    info_df.to_sql(con=connect_to_db('db-destest')[0], name='data_usage', schema='abode', if_exists='append', flavor='Oracle')
    cf.submit_desdf(query.connect_to_db('db-destest')[1], info_df)

if __name__=='__main__':
    main()

