#! /usr/bin/env python
import os
from opstoolkit import jiracmd
from despydb import DesDbi
import pandas

def connect_to_db(section):
    dbh = DesDbi(os.getenv("DES_SERVICES"),section)
    cur = dbh.cursor()
    return (dbh,cur)

def processing_detail(cur,reqnums):
    query = "SELECT distinct a.created_date,r.project,r.campaign,a.reqnum,a.unitname,a.attnum,a.id,v.val,t1.status,\
            a.data_state,a.operator,r.pipeline,t2.start_time,t2.end_time,b.target_site,t1.exec_host,t2.exec_host \
            FROM pfw_job j,pfw_attempt a, pfw_attempt_val v,task t1, task t2,pfw_request r,pfw_block b \
            WHERE a.reqnum=r.reqnum and a.id=j.pfw_attempt_id and a.id=v.pfw_attempt_id and key in ('nite','range') \
            and a.reqnum in ({reqnums}) and b.unitname=a.unitname and b.pfw_attempt_id = a.id and \
            j.task_id = t2.id and a.task_id=t1.id".format(reqnums=reqnums)
    cur.execute(query)
    return cur.fetchall()

def processing_basic(cur,reqnum):
    query = "SELECT distinct r.project,r.campaign,a.reqnum,a.id,t1.status,a.data_state,a.operator,r.pipeline,\
            b.target_site,t1.exec_host,t2.exec_host \
            FROM pfw_attempt a, pfw_job j, task t1,task t2, pfw_request r,pfw_block b \
            WHERE a.created_date >= sysdate-4 and t1.id =a.task_id and a.reqnum=r.reqnum and a.reqnum={reqnum}\
            and a.id=b.pfw_attempt_id and j.task_id=t2.id and j.pfw_attempt_id=a.id".format(reqnum=reqnum)
    cur.execute(query)
    return cur.fetchall()

def processing_summary(cur,reqnums):
    query = "SELECT distinct a.created_date,r.project,r.campaign,a.unitname, a.reqnum,a.attnum,a.id,t1.status,\
            a.data_state,a.operator, r.pipeline,t2.start_time,t2.end_time,b.target_site,t1.exec_host,t2.exec_host \
            FROM pfw_job j,pfw_attempt a,task t1, task t2,pfw_request r,pfw_block b \
            WHERE a.reqnum=r.reqnum and a.id=j.pfw_attempt_id and b.pfw_attempt_id=a.id and j.task_id = t2.id \
            and a.task_id=t1.id and a.reqnum in ({reqnums}) ".format(reqnums=reqnums)

    cur.execute(query)
    return cur.fetchall()

def get_reqnums(cur):
    query = "SELECT distinct a.reqnum \
            FROM pfw_attempt a,task t,pfw_request r,pfw_job j \
            WHERE a.created_date >= sysdate-4 and t.id =a.task_id and a.reqnum=r.reqnum and a.id=j.pfw_attempt_id"
    cur.execute(query)
    return [req[0] for req in cur.fetchall()]

def basic_batch_query(cur,reqnum):
    query = "select count(distinct unitname) from pfw_attempt where reqnum=%s" % reqnum
    cur.execute(query)

    return cur.fetchone()[0]

def batch_size_query(cur,nitelist,reqnum,pipeline):
    if pipeline=='sne':
        batch_size_query = "SELECT count(*) \
                            FROM (SELECT distinct field,band from exposure where nite in ({nitelist}) \
                            and field like 'SN%%' group by field,band)".format(nitelist=nitelist)
    elif pipeline =='finalcut':
        key = 'DESOPS-%s' % reqnum
        summary = jiracmd.Jira('jira-desdm').get_issue(key).fields.summary
        batch_size_query = "select count(*) from exposuretag where tag in ('{summary}')".format(summary=summary)
    elif pipeline=='supercal' or pipeline=='precal':
        batch_size_query = "SELECT count(distinct unitname) \
                            FROM pfw_attempt where reqnum={reqnum}".format(reqnum=reqnum)
    else:
        batch_size_query = "select count(distinct expnum) from exposure where obstype in ('standard','object') \
                            and object not like '%%pointing%%' and object not like '%%focus%%' \
                            and object not like '%%donut%%' and object not like '%%test%%' \
                            and object not like '%%junk%%' and nite in ({nitelist})".format(nitelist=nitelist)
    cur.execute(batch_size_query)
    results = cur.fetchone()[0]
    if not results:
        batch_size_query = "SELECT count(distinct unitname) \
                            FROM pfw_attempt where reqnum={reqnum}".format(reqnum=reqnum)
        cur.execute(batch_size_query)
        results = cur.fetchone()[0]
    return results

def assess_query(cur,df,index,triplet,pfw_attempt_id,pipeline):
    if 'DES' in triplet[0].split('_')[0]:
        pipeline = 'multiepoch'
    if 'D00' in triplet[0].split('_')[0]:
        pipeline = 'firstcut'
    if pipeline=='sne':
        camsym,field,band,seq = triplet[0].split('_')
        comment = field.strip('SN-') + band + ' ' + str(df.loc[index,('nite')])
        assess_q = "select distinct accepted,t_eff,b_eff,c_eff,f_eff,program from firstcut_eval where analyst='SNQUALITY' \
                    and analyst_comment='{comment}'".format(comment=comment)
    elif pipeline =='finalcut':
        assess_q = "SELECT distinct accepted,t_eff,b_eff,c_eff,f_eff,program from finalcut_eval \
                    WHERE pfw_attempt_id={pfwid}".format(pfwid=pfw_attempt_id)
    elif pipeline =='firstcut':
        assess_q = "SELECT distinct accepted,t_eff,b_eff,c_eff,f_eff,program \
                    FROM firstcut_eval where pfw_attempt_id={pfwid}".format(pfwid=pfw_attempt_id)
    else:
        assess_q = "SELECT distinct accepted,t_eff,b_eff,c_eff,f_eff,program \
                    FROM firstcut_eval where pfw_attempt_id={pfwid}".format(pfwid=pfw_attempt_id)
    cur.execute(assess_q)
    return cur.fetchall()

def get_status(cur,reqnums):
    query = "select unitname,reqnum,attnum,a.id, status from pfw_attempt a,task t where t.id=a.task_id \
            and reqnum in ({reqnums})".format(reqnums=reqnums)
    cur.execute(query)
    return cur.fetchall()

def get_tilename_info(cur,reqnums):
    query = "SELECT distinct a.unitname,a.reqnum,a.attnum,a.id from pfw_request r, \
            pfw_attempt a\
            WHERE a.reqnum in ({reqnums}) and r.reqnum=a.reqnum \
            and r.pipeline in ('multiepoch','coadd')".format(reqnums=reqnums)
    try:
        cur.execute(query)
        return cur.fetchall()
    except:
        return None

def get_expnum_info(cur,reqnums):
    query = "SELECT distinct a.unitname,a.reqnum,a.attnum,a.id, e.expnum,e.band from pfw_request r,exposure e, \
            pfw_attempt a\
            WHERE a.reqnum in ({reqnums}) and e.expnum= substr(a.unitname,4) and r.reqnum=a.reqnum \
            and r.pipeline in ('firstcut','finalcut') and a.unitname not like 'DES%%'".format(reqnums=reqnums)
    try: 
        cur.execute(query)
        return cur.fetchall()
    except:
        return None

def get_nites(cur,reqnums):
    query = "SELECT distinct a.unitname,a.reqnum,a.attnum,a.id,v.val from pfw_attempt a,pfw_attempt_val v \
             WHERE key in ('nite','range') and a.id=v.pfw_attempt_id and a.reqnum in ({reqnums})".format(reqnums=reqnums)
    cur.execute(query)
    return cur.fetchall()

def get_teffs(cur,reqnums):
    query = "SELECT distinct a.reqnum,a.id, e.expnum, e.t_eff from pfw_attempt a,finalcut_eval e \
            WHERE and a.id=e.pfw_attempt_id and a.reqnum in ({reqnums})".format(reqnums=reqnums)
    cur.execute(query)
    return cur.fetchall()

def query_all_tiles(cur):
    query = "select distinct tilename, decc1, rac1, decc2, rac2, decc3, rac3, decc4, rac4 from mjohns44.destiles"
    cur.execute(query)
    return cur.fetchall()

def query_processed_tiles(dbh,cur,tag):
    query = "SELECT a.reqnum, a.unitname, a.attnum, a.id, t.status \
            FROM prod.task t, prod.pfw_attempt a, mjohns44.destiles c, prod.proctag p \
            WHERE t.id=a.task_id and a.unitname=c.tilename and a.id=p.pfw_attempt_id \
            and p.tag={tag}".format(tag= dbh.get_named_bind_string('tag'))
    params = {'tag': tag}
    cur.execute(query, params)
    return cur.fetchall()

def query_band_info(cur):
    query = "select tilename, band, dmedian from mjohns44.Y3A1depth"
    cur.execute(query)
    return cur.fetchall()

def query_desdf(cur):
    query = "select filesystem, total_size, used ,available, use_percent, mounted, submittime from abode.data_usage \
             order by submittime"
    cur.execute(query)
    return cur.fetchall()

def get_system_info(start, cur, cur2):
    query = "select number_transferred,number_not_transferred,size_transferred,size_to_be_transferred,number_deprecated,size_deprecated,pipe_processed,pipe_to_be_processed,raw_processed,raw_to_be_processed,run_time from friedel.backup_monitor where run_time >= TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS') order by run_time desc" % (start.strftime('%Y-%m-%d %H:%M:%S'))
    cur2.execute(query)

    query2 = "select transfer_date,(tar_size/(1024*1024*1024)),(tar_size/(transfer_time*1024*1024*1024)) from prod.backup_tape where transfer_date is not null and transfer_date >= TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS') order by transfer_date desc" % (start.strftime('%Y-%m-%d %H:%M:%S'))
    cur.execute(query2)
    return cur.fetchall(), cur2.fetchall()

def query_exptime(cur, stime, etime):
    format = "%Y-%m-%d %H:%M:%S"
    query = "select date, file_uri from exposure where delivered=True and date between \'%s\' and \'%s\' order by date" % (stime.strftime(format), etime.strftime(format))
    cur.execute(query)
    return cur.fetchall()

def query_dts_delay(cur, stime, etime):
    format = "%Y-%m-%d %H:%M:%S"
    #query = "select interval_to_seconds(sispi_time), accept_time, ingest_time, delivered from abode.dts_delay where sispi_time between \'%s\' and \'%s\' order by sispi_time" % (stime.strftime(format), etime.strftime(format))
    query = "select intervalToSeconds(ingest_time - sispi_time)/60 as total_time, intervalToSeconds(ingest_time - accept_time)/60 as ncsa_time, intervalToSeconds(accept_time - sispi_time)/60 as noao_time, sispi_time as xtime from abode.dts_delay where sispi_time between TO_DATE(\'%s\', \'YYYY-MM-DD HH24:MI:SS\') and TO_DATE(\'%s\', \'YYYY-MM-DD HH24:MI:SS\') order by sispi_time" % (stime.strftime(format), etime.strftime(format))
    #print query
    cur.execute(query)
    return cur.fetchall()

def update_column_df(con,df,name):
#    data = pandas.read_csv('/home/ycchen/'+file,skiprows=1)
    dbh = con[0]
    cur = con[1]
    data = df
    data.reset_index(inplace=True,drop=True)
#    pandas.DatetimeIndex(data['start_time']).to_native_types()
#    pandas.DatetimeIndex(data['created_date']).to_native_types()
#    pandas.DatetimeIndex(data['end_time']).to_native_types()
    data['start_time']=data['start_time'].values.astype('<M8[s]').astype(str)
    data['created_date']=data['created_date'].values.astype('<M8[s]').astype(str)
    data['end_time']=data['end_time'].values.astype('<M8[s]').astype(str)
    data['status']=data['status'].fillna(-99.0).astype(int)
    data['status_orig']=data['status_orig'].fillna(-99.0).astype(int)
    data.rename(columns={"total time": "total_time"},inplace=True)
    data['total_time']=data['total_time'].astype(float)
    data['t_eff']=data['t_eff'].astype(str)
    data['b_eff']=data['b_eff'].astype(str)
    data['c_eff']=data['c_eff'].astype(str)
    data['f_eff']=data['f_eff'].astype(str)

    for column in data.columns:
    
        if 'int' in data[column].dtypes.name or 'float' in data[column].dtypes.name:
            data[column] = data[column].fillna(-99)
        else:
            data[column] = data[column].fillna('None')
#    data = data.reindex(range(0,len(data)),method='ffill')
#    for i in range(0,len(data)):
#        data=data.rename(index={data.index.values[i]:i})
# ignore total_time,assessment,t_eff,b_eff,c_eff,f_eff,program

    list_check = ['pfw_attempt_id','status','data_state','end_time','total_time','assessment','t_eff','b_eff','c_eff','f_eff']
    query_pfw = "select "
    for column in list_check:
        if column == 'pfw_attempt_id':
            query_pfw = query_pfw +  column
        else:
            query_pfw = query_pfw + "," + column
    query_pfw = query_pfw + " from " + name
    cur.execute(query_pfw)
    result = cur.fetchall()
    if result == []:
        data_db = pandas.DataFrame(columns=list_check)
    else: data_db = pandas.DataFrame(result,columns=list_check)
    data['expnum']=data['expnum'].astype(str)
    num = len(data)
    num_db = len(data_db)
    i = 0
#    print data_db.info()
#    print data.info()
    while (i<num):
        j = 0
        found = 0
        while (j<num_db):
            if (data_db.loc[j,'pfw_attempt_id']==data.loc[i,'pfw_attempt_id']):
                up_dict = {}
                for column in list_check:
                    if 'float' in data[column].dtype.name:
                        if (round(data_db.loc[j,column],4) != round(data.loc[i,column],4)):
                            up_dict.update({column:round(data.loc[i,column],3)})
                    else:
                        if (data_db.loc[j,column] != data.loc[i,column]):
                            up_dict.update({column:data.loc[i,column]})
                found = 1
                if up_dict != {}:
                    dbh.basic_update_row(name,up_dict,{'pfw_attempt_id' : data.loc[i,'pfw_attempt_id']})
                    dbh.commit()
                    print "Updating for " + str(data.loc[i,'pfw_attempt_id'])+" :"
                    print up_dict
                else: print "Already up to date for ID:" + str(data.loc[i,'pfw_attempt_id'])
            j=j+1
        if (found == 0):
            print " Append pfw_attempt_id: "+str(data.loc[i,'pfw_attempt_id'])
            row = dict(zip(list(data.columns),map(list,data.values)[i]))
#            print row
            dbh.basic_insert_row (name, row)
            dbh.commit()
#        else: print str(data.loc[i,'pfw_attempt_id'])+" Exist !! "
        i=i+1
    return 0


