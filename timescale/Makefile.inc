# Timescale make file

BASE_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
BENCHMARK_CONFIG=config.yaml
build/timescale/%.yaml: ${BASE_DIR}/templates/%.yaml
	mkdir -p build/timescale
	gomplate -d config=${BENCHMARK_CONFIG} -f $< -o $@

timescale_templates: build/timescale/timescale-namespace.yaml build/timescale/timescale-system.yaml

timescale_system: timescale_templates
	kubectl apply -f build/timescale/timescale-namespace.yaml
	kubectl apply -f build/timescale/timescale-system.yaml

timescale_uninstall: build/timescale/timescale-namespace.yaml
	kubectl delete --ignore-not-found=true -f build/timescale/timescale-namespace.yaml

timescale_benchmark: timescale_system testrunner
	go test pkg/benchmarks -tags timescale -c -o build/timescale/timescale-benchmark

.PHONY: timescale_system	timescale_uninstall
