import os
import tornado.web
from views import *

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", IndexHandler),
            (r"/processing_summary",ProcessingSummaryHandler),
            (r"/testing_summary",TestingSummaryHandler),
            (r"/processing_detail/<db>/<operator>/<reqnum>",ProcessingDetailHandler),
            (r"/dts",DTSHandler),
            (r"/backups",BackupsHandler),
            (r"/supernova_summary",SupernovaSummaryHandler),
        ]

        settings = {
            "template_path":'/Users/mjohns44/GIT_DESDM/desdm-dash/app/templates',
            "static_path":'/Users/mjohns44/GIT_DESDM/desdm-dash/app/static',
            "debug":True,
        }
        tornado.web.Application.__init__(self, handlers, **settings)
    
def main():
    app = Application()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start() 

if __name__=='__main__':
    main()    
