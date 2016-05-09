from app import app
from subprocess import call
import shlex
if __name__=="__main__":
    cmd = shlex.split('celery -A app.celery worker --loglevel=info')
    command = call(cmd, shell = False)
