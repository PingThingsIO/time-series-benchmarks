# Timescale make file

BASE_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
BENCHMARK_CONFIG=config.yaml
build/timescale/%.yaml: ${BASE_DIR}/templates/%.yaml
	mkdir -p build/timescale
	gomplate -d config=${BENCHMARK_CONFIG} -f $< -o $@

timescale_templates: build/timescale/timescale-namespace.yaml build/timescale/timescale-system.yaml

timescale_system: timescale_templates testrunner_system
	kubectl apply -f build/timescale/timescale-namespace.yaml
	kubectl apply -f build/timescale/timescale-system.yaml

timescale_uninstall: build/timescale/timescale-namespace.yaml
	kubectl delete --ignore-not-found=true -f build/timescale/timescale-namespace.yaml

timescale_benchmark: testrunner_system timescale_system fingerprint
	go test ./pkg/benchmarks/ --tags timescale -c -o build/benchmark_timescale
	kubectl -n ${NSPREFIX} cp build/benchmark_timescale testrunner:/
	kubectl -n ${NSPREFIX} cp config.yaml testrunner:/
	kubectl -n ${NSPREFIX} cp fingerprint.ini testrunner:/
	kubectl -n ${NSPREFIX} exec -it testrunner /benchmark_timescale
	kubectl -n ${NSPREFIX} cp testrunner:/report.json ./report-timescale.json

.PHONY: timescale_system	timescale_uninstall
