#! /usr/bin/env python
import tornado.web
import get_data
import plotter

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("home.html")

class ProcessingSummaryHandler(tornado.web.RequestHandler):
    def get(self):
        all_dict,operator_list,columns = get_data.processing_summary('db-desoper','OPS')
        self.render('processing_summary.html',columns=columns,dict_list=all_dict,
                                              operator_list=operator_list)

class TestingSummaryHandler(tornado.web.RequestHandler):
    def get(self):
        all_dict,operator_list,columns = get_data.processing_summary('db-desoper','TEST')
        tall_dict,toperator_list,tcolumns = get_data.processing_summary('db-destest','TEST')
        self.render('testing_summary.html',columns=columns,dict_list=all_dict,
                                           operator_list=operator_list,tdict_list=tall_dict,
                                           toperator_list=toperator_list,tcolumns=tcolumns)

class ProcessingDetailHandler(tornado.web.RequestHandler):
    def get(self,db,operator,reqnum):
        df,columns,reqnum,mean_times = get_data.processing_detail(db,operator,reqnum)
        try:
            tfigscript,tfigdiv=plotter.plot_times(df)
            figscript,figdiv=plotter.plot_accepted(df)
        except:
            tfigscript,tfigdiv=None,None
            figscript,figdiv=None,None
    
        self.render('processing_detail.html',columns=columns,df = df,reqnum=reqnum,figdiv=figdiv,
                                             figscript=figscript,mean_times=mean_times,
                                             tfigscript=tfigscript,tfigdiv=tfigdiv,db=db,
                                             operator=operator)

class DTSHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('dts_monitor.html')

class BackupsHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('back_ups.html')

class SupernovaSummaryHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('supernova_summary.html')
