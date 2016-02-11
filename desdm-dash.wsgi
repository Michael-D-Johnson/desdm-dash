# For Virtual Environments
#activate_this = '/path/to/env/bin/activate_this.py'
#execfile(activate_this, dict(__file__=activate_this))

import sys
sys.path.insert(0, '/work/devel/mjohns44/git/desdm-dash')

from app import app as application
