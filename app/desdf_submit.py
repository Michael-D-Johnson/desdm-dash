#!/usr/bin/env python

import query
import get_data

import os
import pandas as pd
import despydb.desdbi as desdbi
from despydb import DesDbi

def main():

    info_df = get_data.get_desdf()

    # Direct pandas method if Oracle gets implemented.
#    info_df.to_sql(con=connect_to_db('db-destest')[0], name='data_usage', schema='abode', if_exists='append', flavor='Oracle')
    query.submit_desdf(query.connect_to_db('db-destest')[1], info_df)

if __name__=='__main__':
    main()

