#!/bin/bash
# This code submits the desdf info to db
# Crontab to submit data 0,0,1 * * * /work/devel/abode/projects/desdm-dash/desdf_submit.sh
source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh

setup -r /work/devel/abode/projects/desdm-dash --nolocks
setup pandas 0.15.2+1 --nolocks
setup jinja2 2.6+8 --nolocks
setup opstoolkit 0.1.0+0 --nolocks
setup bokeh 0.10.0+0 --nolocks
setup flask 0.10.1+0 --nolocks

/work/devel/abode/projects/desdm-dash/app/desdf_submit.py
