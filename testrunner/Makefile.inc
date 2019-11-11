# Test runner makefile

BASE_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
BENCHMARK_CONFIG=config.yaml
build/testrunner/%.yaml: ${BASE_DIR}/templates/%.yaml
	mkdir -p build/testrunner
	gomplate -d config=${BENCHMARK_CONFIG} -f $< -o $@

testrunner_templates: build/testrunner/testrunner-namespace.yaml build/testrunner/testrunner-system.yaml

testrunner_system: testrunner_templates
	kubectl apply -f build/testrunner/testrunner-namespace.yaml
	kubectl apply -f build/testrunner/testrunner-system.yaml

fingerprint:
	gen_fingerprint.sh > fingerprint.ini

testrunner_uninstall: build/testrunner/testrunner-namespace.yaml
	kubectl delete --ignore-not-found=true -f build/testrunner/testrunner-namespace.yaml

.PHONY: testrunner_system	testrunner_uninstall
