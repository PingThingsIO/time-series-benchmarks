# Main make file

include timescale/Makefile.inc
include influxdb/Makefile.inc
include testrunner/Makefile.inc
include predictivegrid/Makefile.inc
include cluster.inc

clean:
	rm -rf build

uninstall: timescale_uninstall influx_uninstall testrunner_uninstall clean


.PONY: clean uninstall
