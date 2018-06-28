#!/bin/bash
# This code runs the server
# Crontab to create data/csv: 0,15 * * * * /work/devel/mjohns44/git/desdm-dash/app/get_data.py
source $HOME/.desdm_dash_settings.sh
source $EUPS_SETUP
setup -r .
setup pandas 0.15.2+5
setup jinja2 2.6+10
setup flask 0.10.1+0
setup configargparse 0.10.0+0
setup -r /work/devel/mjohns44/svn/opstoolkit/trunk
setup despymisc 1.0.4+0
setup bokeh 0.11.1+3

python start_server.py
