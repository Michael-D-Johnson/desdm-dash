#!/bin/bash
# This code runs the server
# Crontab to create data/csv: 0,15 * * * * /work/devel/mjohns44/git/desdm-dash/app/get_data.py
setup -r .
python start_server.py
