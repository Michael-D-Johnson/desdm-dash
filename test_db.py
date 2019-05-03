import easyaccess as ea
def connect_to_db(section):
    dbh = ea.connect(section)
    cur = dbh.cursor()
    return (cur)
def processing_summary(cur,reqnums):
    query = "SELECT distinct a.created_date,r.project,r.campaign,a.unitname, a.reqnum,a.attnum,a.id,t1.status,\
            a.data_state,a.operator, r.pipeline,t1.start_time,t2.end_time,b.target_site,t1.exec_host,t2.exec_host \
            FROM pfw_job j,pfw_attempt a,task t1, task t2,pfw_request r,pfw_block b \
            WHERE a.reqnum=r.reqnum and a.id=j.pfw_attempt_id and b.pfw_attempt_id=a.id and j.task_id = t2.id \
            and a.task_id=t1.id and a.reqnum in ({reqnums}) ".format(reqnums=reqnums)

    cur.execute(query)
    return cur.fetchall()
if __name__=="__main__":
    cur = connect_to_db('desoper')
    results = processing_summary(cur,4276)
    print(results)
