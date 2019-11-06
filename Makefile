# Main make file

include timescale/Makefile.inc
include influxdb/Makefile.inc

clean:
	rm -rf build

uninstall: timescale_uninstall influx_uninstall clean


.PONY: clean uninstall
