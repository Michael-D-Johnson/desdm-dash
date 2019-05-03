#!/bin/bash
nohup python /desdm-dash/app/make_reports.py 2>&1 > /desdm-dash/app/static/reports/make_reports.out &
python /desdm-dash/start_server.py 
