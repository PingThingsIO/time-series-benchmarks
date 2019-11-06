# Influx make file

BASE_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
BENCHMARK_CONFIG=config.yaml
build/influx/%.yaml: ${BASE_DIR}/templates/%.yaml
	mkdir -p build/influx
	gomplate -d config=${BENCHMARK_CONFIG} -f $< -o $@

influx_templates: build/influx/influx-namespace.yaml build/influx/influx-system.yaml

influx_system: influx_templates
	kubectl apply -f build/influx/influx-namespace.yaml
	kubectl apply -f build/influx/influx-system.yaml

influx_uninstall: build/influx/influx-namespace.yaml
	kubectl delete --ignore-not-found=true -f build/influx/influx-namespace.yaml

.PHONY: influx_system	influx_uninstall
