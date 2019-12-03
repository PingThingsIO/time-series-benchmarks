SHELL := /bin/bash
install:
	python3 -m venv env
	source ./env/bin/activate
	python -m pip install -r requirements.txt --user; \
	python ./plot-time-series.py --file=report-influx-pretty.json --file=report-predictivegrid-pretty.json --file=report-timescale-pretty.json; \
