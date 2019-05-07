from app import app
from gevent.pywsgi import WSGIServer
if __name__=="__main__":
    http_server = WSGIServer(('',5000),app,log=app.logger)
    http_server.serve_forever()
    
