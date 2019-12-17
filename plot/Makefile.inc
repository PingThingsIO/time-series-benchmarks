SHELL := /bin/bash

install:
	python ./plot-time-series.py --file=report-influx-pretty.json --file=report-predictivegrid-pretty.json --file=report-timescale-pretty.json
