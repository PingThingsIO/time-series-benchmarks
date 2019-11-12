# Influx make file

BASE_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
BENCHMARK_CONFIG=config.yaml
#NSPREFIX := $$(cat ${BENCHMARK_CONFIG} | grep NamespacePrefix | cut -d':' -f2)testrunner
NSPREFIX := $$(gomplate -d ${BENCHMARK_CONFIG} -i '{{ (datasource "config").Deployment.NamespacePrefix }}testrunner')

build/influx/%.yaml: ${BASE_DIR}/templates/%.yaml
	mkdir -p build/influx
	gomplate -d config=${BENCHMARK_CONFIG} -f $< -o $@

influx_templates: build/influx/influx-namespace.yaml build/influx/influx-system.yaml

influx_system: influx_templates testrunner_system
	kubectl apply -f build/influx/influx-namespace.yaml
	kubectl apply -f build/influx/influx-system.yaml

influx_uninstall: build/influx/influx-namespace.yaml
	kubectl delete --ignore-not-found=true -f build/influx/influx-namespace.yaml

influx_benchmark: testrunner_system influx_system fingerprint
	go test ./pkg/benchmarks/ --tags influx -c -o benchmark_influx
	kubectl -n ${NSPREFIX} cp benchmark_influx testrunner:/
	kubectl -n ${NSPREFIX} cp config.yaml testrunner:/
	kubectl -n ${NSPREFIX} cp fingerprint.ini testrunner:/
	kubectl -n ${NSPREFIX} exec -it testrunner /benchmark_influx --test.timeout=5h -v
	kubectl -n ${NSPREFIX} cp testrunner:/report.json ./report-influx.json
.PHONY: influx_system	influx_uninstall
