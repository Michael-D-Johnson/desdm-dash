#! /usr/bin/env python
import os
import pandas
import query

#pandas.options.mode.chained_assignment = None

def processing_summary(db,project):
    reqnums = [str(r) for r in query.get_reqnums(query.cursor(db))]
    reqnum_list = ','.join(reqnums)
    if reqnums:
        results = query.processing_summary_brief(query.cursor(db),reqnum_list)
        df = pandas.DataFrame(results,columns=['created date','project','campaign','unitname','nite','reqnum','attnum','status','data_state','operator','pipeline','target_site','submit_site'])
        df = df.sort(columns=['reqnum','unitname','attnum'],ascending=False)
        df = df.drop_duplicates(subset=['unitname','reqnum','attnum'])
        df = df.fillna(-99)
        if project =='TEST':
            df = df[df.project != 'OPS']
        else:
            df = df[df.project == project]
        columns = ['operator','project','campaign','pipeline','submit_site','target_site','reqnum','nite','batch size','passed','failed','unknown','remaining']
        df_op = df.groupby(by=['operator'])
        all_dict = []
        for name,group in df_op:
            for req in sorted(group['reqnum'].unique(),reverse = True):
                nitelist = sorted(df[df.reqnum==req].loc[group.index,('nite')].dropna().unique())
                #nitelist = ', '.join(nitelist)
                pipeline = df[df.reqnum==req]['pipeline'].unique()[0]
                if len(nitelist) > 1:
                    nitelist = nitelist[0] + ' - ' + nitelist[-1]
                if pipeline =='supercal':
                    nites = nitelist.split('t')
                    nite1,nite2 = nites[0],nites[0][:4]+nites[1]
                    nitelist = nite1 + ' - ' + nite2
                if project =='TEST':
                    total_batch_size = query.test_query(query.cursor(db),req)
                else:
                    total_batch_size = query.batch_size_query(query.cursor(db),nitelist,req,pipeline)
                passed_df =  df[(df.operator == name) & (df.status==0) & (df.reqnum == req)].drop_duplicates(['unitname'])
                passed = passed_df['status'][(df.operator == name) & (df.status==0) & (df.reqnum == req)].count()
                failed_df = df[(df.operator == name) & (~df.status.isin([0,-99])) & (df.reqnum == req)].drop_duplicates(['unitname'])
                failed = failed_df['status'][(df.operator == name) & (~df.status.isin([0,-99])) & (df.reqnum == req)].count()
                try: unknown = df['status'][(df.operator == name) & (df.status == -99) & (df.reqnum==req)].count()
                except: unknown = 0
                #target_site = ', '.join(df[df.reqnum==req].sort(columns=['created date'])['target_site'].unique())
                target_site =df[df.reqnum==req].sort(columns=['created date'])['target_site'].unique()[-1]

                #submit_site = ', '.join([site.split('.')[0] for site in df[df.reqnum==req].sort(columns=['created date'])['submit_site'].unique()])
                submit_site = [site.split('.')[0] for site in df[df.reqnum==req].sort(columns=['created date'])['submit_site'].unique()][-1]

                remaining = int(total_batch_size)-int(passed) #-int(failed)
                if remaining < 0:
                    remaining = 0
                if len(nitelist) == 1:
                    nitelist = nitelist[0]
                req_dict = {'remaining':remaining,'operator':name,'batch size':total_batch_size,
                            'reqnum':req,'passed':passed,'failed':failed,'unknown':unknown,
                            'nite':nitelist,
                            'submit_site':submit_site,
                            'target_site':target_site,
                            'campaign':df[df.reqnum==req].loc[group.index,('campaign')].dropna().unique()[0],
                            'project':df[df.reqnum==req].loc[group.index,('project')].dropna().unique()[0],
                            'pipeline':df[df.reqnum==req].loc[group.index,('pipeline')].dropna().unique()[0]}
                all_dict.append(req_dict)
    try: operator_list =  list(set( dic['operator'] for dic in all_dict ))
    except: 
        operator_list = ''
        all_dict = {}
        columns = '' 
    return (all_dict,operator_list,columns)

def processing_detail(db,operator,reqnum):
    results = query.processing_detail(query.cursor(db),reqnum) 
    if not results:
        results = query.processing_basic(query.cursor(db),reqnum)
        df = pandas.DataFrame(results,columns=['project','campaign','unitname','reqnum','attnum','status','data_state','operator','pipeline','target_site','submit_host','exec_host'])
        df = df.sort(columns=['unitname','attnum'],ascending=False)
        df = df[df.operator==operator]
        columns = df.columns
        return (df,columns,reqnum,None)
    else:
        df = pandas.DataFrame(results,columns=['project','campaign','unitname','nite','reqnum','attnum','status','data_state','operator','pipeline','start_time','end_time','target_site','submit_host','exec_host'])
        df.insert(len(df.columns),'assessment', None)
        df.insert(len(df.columns),'program', None)
        df.insert(len(df.columns),'t_eff', None)
        df.insert(12,'total time', None)
        def rename(row):
            row['submit_host'] = row['submit_host'].split('.')[0]
            return row['submit_host']

        df['submit_host'] = df.apply(rename,axis=1)
        df = df.sort(columns=['reqnum','unitname','attnum','start_time'],ascending=False)
        df = df[df.operator==operator]
        df_attempt = df.groupby(by=['unitname','reqnum','attnum'])
        for name,group in df_attempt:
            df = df.drop_duplicates(subset=['unitname','reqnum','attnum'])
            columns = df.columns
            index = df[(df['unitname']==name[0]) & (df['reqnum']==name[1]) & (df['attnum']==name[2])].index[0]
            df.loc[index,('start_time','end_time')] = group['start_time'].min(),group['end_time'].max()
            try:
                total_time = (group['end_time'].max() - group['start_time'].min())/pandas.Timedelta(hours=1)

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

        mean_times =round(df['total time'].mean(skipna=True),3)
        return (df,columns,reqnum,mean_times)
