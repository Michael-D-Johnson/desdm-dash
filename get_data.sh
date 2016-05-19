#!/bin/bash
# This code runs the server
# Crontab to create data/csv: 0,15 * * * * /work/devel/mjohns44/git/desdm-dash/app/get_data.py
source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh

setup -r /work/devel/mjohns44/git/desdm-dash --nolocks
setup pandas 0.15.2+1 --nolocks
setup jinja2 2.6+8 --nolocks
setup opstoolkit 0.1.0+0 --nolocks
setup bokeh 0.10.0+0 --nolocks
setup flask 0.10.1+0 --nolocks

/work/devel/mjohns44/git/desdm-dash/app/get_data.py
/work/devel/mjohns44/git/desdm-dash/app/plotter.py
