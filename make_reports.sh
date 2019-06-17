#!/bin/bash
# This code runs the server
# Crontab to create data/csv: 0,15 * * * * /work/devel/mjohns44/git/desdm-dash/app/get_data.sh

# Checking if script is already running
l=$(ps aux | grep -e '/work/devel/mjohns44/git/desdm-dash/desdm-dash/make_reports.sh' | wc -l)
echo "Number of processes: $l"
if [ "$l" -lt 2 ]
then
date
echo "No script currently running. Continuing"
else
date
echo "Already running.  Aborting"
exit 1
fi

source $HOME/.desdm_dash_settings.sh
source $EUPS_SETUP
setup -r $DESDM_DASH_PATH --nolocks
setup pandas 0.15.2+5 --nolocks
setup jinja2 2.6+8 --nolocks
setup bokeh 0.10.0+0 --nolocks
setup flask 0.10.1+0 --nolocks
setup -r /work/devel/mjohns44/svn/opstoolkit/trunk --nolocks
setup despymisc 1.0.4+0 --nolocks

python $DESDM_DASH_PATH/app/make_reports.py 
