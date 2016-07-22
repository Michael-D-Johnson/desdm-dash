#!/bin/bash
# This code runs the server
# Crontab to create data/csv: 0,15 * * * * /work/devel/mjohns44/git/desdm-dash/app/get_data.py
source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh
setup -r .
setup pandas 0.15.2+1
setup jinja2 2.6+8
setup opstoolkit 0.1.0+0
setup flask 0.10.1+0
setup configargparse 0.10.0+0
setup bokeh 0.11.0+1
python start_server.py
