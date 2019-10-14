# Main make file

include timescale/Makefile.inc


clean:
	rm -rf build

uninstall: timescale_uninstall clean


.PONY: clean uninstall
