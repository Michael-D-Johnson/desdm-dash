#! /usr/bin/env python
import os
import pandas
import query

pandas.options.mode.chained_assignment = None

def processing_summary():
    reqnums = [str(r) for r in query.get_reqnums()]
    reqnum_list = ','.join(reqnums)

    results = query.processing_summary_brief(reqnum_list)
    df = pandas.DataFrame(results,columns=['project','campaign','unitname','nite','reqnum','attnum','status','data_state','operator','pipeline'])
    df = df.sort(columns=['reqnum','unitname','attnum'],ascending=False)
    df = df.drop_duplicates(subset=['unitname','reqnum','attnum'])
    df = df.fillna(-99)
    columns = ['operator','project','campaign','pipeline','reqnum','nite','total expnums','passed','failed','unknown','remaining']
    df_op = df.groupby(by=['operator'])
    all_dict = []
    for name,group in df_op:
        for req in sorted(group['reqnum'].unique(),reverse = True):
            nitelist = sorted(df[df.reqnum==req].loc[group.index,('nite')].dropna().unique())
            nitelist = ', '.join(nitelist)
            pipeline = df[df.reqnum==req]['pipeline'].unique()[0]
            if pipeline =='supercal':
                nites = nitelist.split('t')
                nite1,nite2 = nites[0],nites[0][:4]+nites[1]
                nitelist = nite1 + ', ' + nite2
            total_expnums = query.expnum_query(nitelist,req,pipeline)
            passed_df =  df[(df.operator == name) & (df.status==0) & (df.reqnum == req)].drop_duplicates(['unitname'])
            passed = passed_df['status'][(df.operator == name) & (df.status==0) & (df.reqnum == req)].count()
            failed_df = df[(df.operator == name) & (~df.status.isin([0,-99])) & (df.reqnum == req)].drop_duplicates(['unitname'])
            failed = failed_df['status'][(df.operator == name) & (~df.status.isin([0,-99])) & (df.reqnum == req)].count()
            try: unknown = df['status'][(df.operator == name) & (df.status == -99) & (df.reqnum==req)].count()
            except: unknown = 0
            remaining = int(total_expnums)-int(passed) #-int(failed)
            if remaining < 0:
                remaining = 0
            if len(nitelist) == 1:
                nitelist = nitelist[0]
            req_dict = {'remaining':remaining,'operator':name,'total expnums':total_expnums,
                        'reqnum':req,'passed':passed,'failed':failed,'unknown':unknown,
                        'nite':nitelist,
                        'campaign':df[df.reqnum==req].loc[group.index,('campaign')].dropna().unique()[0],
                        'project':df[df.reqnum==req].loc[group.index,('project')].dropna().unique()[0],
                        'pipeline':df[df.reqnum==req].loc[group.index,('pipeline')].dropna().unique()[0]}
            all_dict.append(req_dict)
    operator_list =  list(set( dic['operator'] for dic in all_dict ))
    return (all_dict,operator_list,columns)

def processing_detail(reqnum):
    results = query.processing_detail(reqnum) 

    df = pandas.DataFrame(results,columns=['project','campaign','unitname','nite','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time'])
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
        pipeline = group.pipeline.unique()
        assess_query_results = query.assess_query(name,pipeline)

        try:
            assess,t_eff,program = assess_query_results[0][0],assess_query_results[0][1],assess_query_results[0][2]
        except:
            assess,t_eff,program = 'None','None','None'

        df.loc[index,('assessment')] = assess
        df.loc[index,('t_eff')] = t_eff
        df.loc[index,('program')] = program

    mean_times =round(df['total time'].mean(skipna=True),3)
    return (df,columns,reqnum,mean_times)
