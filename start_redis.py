from app import app
from subprocess import call
import shlex
if __name__=="__main__":
    cmd = shlex.split('redis-server')
    command = call(cmd, shell = False)
