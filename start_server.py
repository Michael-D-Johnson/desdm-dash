from app import app
from gevent.pywsgi import WSGIServer
if __name__=="__main__":
    #app.run(host='141.142.161.36',debug=True)
    #app.run(debug=True)
    http_server = WSGIServer(('',5000),app)
    http_server.serve_forever()
    
