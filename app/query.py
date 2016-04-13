#! /usr/bin/env python
import os
from opstoolkit import jiracmd
from despydb import DesDbi

def cursor(section):
    dbh = DesDbi(os.getenv("DES_SERVICES"),section)
    cur = dbh.cursor()
    return cur

def prod_processing_summary_brief(cur,reqnums):
    query = "select distinct a.created_date,r.project,r.campaign,a.unitname,v.val,a.reqnum,a.attnum,t.status,a.data_state,a.operator,r.pipeline,b.target_site,t.exec_host from pfw_attempt a,pfw_attempt_val v,task t,pfw_request r, pfw_block b where t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=v.reqnum and a.unitname=v.unitname and a.attnum=v.attnum and key in ('nite','range') and a.reqnum in (%s) and b.unitname=a.unitname and b.reqnum=a.reqnum and b.attnum=a.attnum and r.project='OPS'" % reqnums
    cur.execute(query)
    return cur.fetchall()

def prod_testing_summary_brief(cur,reqnums):
    query = "select distinct a.created_date,r.project,r.campaign,a.unitname,v.val,a.reqnum,a.attnum,t.status,a.data_state,a.operator,r.pipeline,b.target_site,t.exec_host from pfw_attempt a,pfw_attempt_val v,task t,pfw_request r, pfw_block b where t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=v.reqnum and a.unitname=v.unitname and a.attnum=v.attnum and key in ('nite','range') and a.reqnum in (%s) and b.unitname=a.unitname and b.reqnum=a.reqnum and b.attnum=a.attnum and r.project not in ('OPS')" % reqnums
    cur.execute(query)
    return cur.fetchall()

def hostname_summary_brief(cur,reqnums):
    query = "select distinct a.created_date,r.project,r.campaign,a.unitname,a.reqnum,a.attnum,t.status,a.data_state,a.operator,r.pipeline,b.target_site,t.exec_host from pfw_attempt a,pfw_attempt_val v,task t,pfw_request r, pfw_block b where t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum in (%s) and b.unitname=a.unitname and b.reqnum=a.reqnum and b.attnum=a.attnum" % reqnums
    cur.execute(query)
    return cur.fetchall()

def processing_summary_brief(cur,reqnums):
    query = "select distinct a.created_date,r.project,r.campaign,a.unitname,v.val,a.reqnum,a.attnum,t.status,a.data_state,a.operator,r.pipeline,b.target_site,t.exec_host from pfw_attempt a,pfw_attempt_val v,task t,pfw_request r, pfw_block b where t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=v.reqnum and a.unitname=v.unitname and a.attnum=v.attnum and key in ('nite','range') and a.reqnum in (%s) and b.unitname=a.unitname and b.reqnum=a.reqnum and b.attnum=a.attnum" % reqnums
    cur.execute(query)
    return cur.fetchall()

def get_test_reqnums(cur):
    query = "select distinct a.reqnum from pfw_attempt a,task t,pfw_request r,pfw_job j where a.created_date >= sysdate-4 and t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=j.reqnum and r.project not in ('OPS')"
    cur.execute(query)
    return [req[0] for req in cur.fetchall()]

def get_prod_reqnums(cur):
    query = "select distinct a.reqnum from pfw_attempt a,task t,pfw_request r,pfw_job j where a.created_date >= sysdate-4 and t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=j.reqnum and r.project='OPS'"
    cur.execute(query)
    return [req[0] for req in cur.fetchall()]

def get_reqnums(cur):
    query = "select distinct a.reqnum from pfw_attempt a,task t,pfw_request r,pfw_job j where a.created_date >= sysdate-4 and t.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=j.reqnum"
    cur.execute(query)
    return [req[0] for req in cur.fetchall()]

def test_query(cur,reqnum):
    query = "select count(distinct unitname) from pfw_attempt where reqnum=%s" % reqnum
    cur.execute(query)

    return cur.fetchone()[0]

def batch_size_query(cur,nitelist,reqnum,pipeline):
    if pipeline=='sne':
        batch_size_query = "select count(*) from (select distinct field,band from exposure where nite in (%s) and field like 'SN%%' group by field,band)" % nitelist
    elif pipeline =='finalcut':
        key = 'DESOPS-%s' % reqnum
        summary = jiracmd.Jira('jira-desdm').get_issue(key).fields.summary
        batch_size_query = "select count(*) from exposuretag where tag in ('%s')" % summary
    elif pipeline=='supercal' or pipeline=='precal':
        batch_size_query = "select count(distinct unitname) from pfw_attempt where reqnum=%s" % reqnum
    else:
        batch_size_query = "select count(distinct expnum) from exposure where obstype='object' and object not like '%%pointing%%' and object not like '%%focus%%' and object not like '%%donut%%' and object not like '%%test%%' and object not like '%%junk%%' and nite in (%s)" % nitelist
    cur.execute(batch_size_query)
    results = cur.fetchone()[0]
    if not results:
        batch_size_query = "select count(distinct unitname) from pfw_attempt where reqnum=%s" % reqnum
        cur.execute(batch_size_query)
        results = cur.fetchone()[0]
    return results

def processing_detail(cur,reqnum):
    query = "select distinct r.project,r.campaign,a.unitname,v.val,a.reqnum,a.attnum,t1.status,a.data_state,a.operator,r.pipeline,t2.start_time,t2.end_time,b.target_site,t1.exec_host,t2.exec_host from pfw_job j,pfw_attempt a,pfw_attempt_val v,task t1, task t2,pfw_request r,pfw_block b where a.created_date >= sysdate-4 and  a.reqnum=r.reqnum and a.reqnum=j.reqnum and a.unitname=j.unitname and a.attnum=j.attnum and a.reqnum=v.reqnum and a.unitname=v.unitname and a.attnum=v.attnum and key in ('nite','range') and a.reqnum=%s and b.unitname=a.unitname and b.reqnum=a.reqnum and b.attnum=a.attnum and j.task_id = t2.id and a.task_id=t1.id" % reqnum
    cur.execute(query)
    return cur.fetchall()

def processing_basic(cur,reqnum):
    query = "select distinct r.project,r.campaign,a.unitname,a.reqnum,a.attnum,t1.status,a.data_state,a.operator,r.pipeline,b.target_site,t1.exec_host,t2.exec_host from pfw_attempt a, pfw_job j, task t1,task t2, pfw_request r,pfw_block b where a.created_date >= sysdate-4 and t1.id =a.task_id and a.reqnum=r.reqnum and a.reqnum=%s and b.unitname=a.unitname and a.reqnum=b.reqnum and a.attnum=b.attnum and j.task_id=t2.id and j.unitname=a.unitname and j.attnum=a.attnum and j.reqnum=a.reqnum" % reqnum
    cur.execute(query)
    return cur.fetchall()

def assess_query(cur,df,index,triplet,pipeline):
    if pipeline=='sne':
        camsym,field,band,seq = triplet[0].split('_')
        comment = field.strip('SN-') + band + ' ' + df.loc[index,('nite')]
        assess_q = "select distinct accepted,t_eff,program from firstcut_eval where analyst='SNQUALITY' and analyst_comment='%s'" % (comment)
    elif pipeline =='finalcut':
        assess_q = "select distinct accepted,t_eff,program from finalcut_eval where unitname='%s' and reqnum='%s' and attnum='%s'" % (triplet[0],triplet[1],triplet[2])
    elif pipeline =='firstcut':
        assess_q = "select distinct accepted,t_eff,program from firstcut_eval where unitname='%s' and reqnum='%s' and attnum='%s'" % (triplet[0],triplet[1],triplet[2])
    else:
        assess_q = "select distinct accepted,t_eff,program from firstcut_eval where unitname='%s' and reqnum='%s' and attnum='%s'" % (triplet[0],triplet[1],triplet[2])
    cur.execute(assess_q)
    return cur.fetchall()
