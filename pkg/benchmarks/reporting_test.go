package benchmarks

import (
	"encoding/json"
	"fmt"
	"strings"
)

type FinalReport struct {
	Benchmarks map[string][]TestResults `json:"benchmarks"`
}

type TestResults struct {
	Parameters map[string]string  `json:"parameters"`
	Results    map[string]float64 `json:"results"`
}

func writeReport() {

	report := FinalReport{}
	for test, pars := range reportData {
		subtests := make([]TestResults, 0)
		for parstring, mp := range pars {
			ptestresults := TestResults{}
			ptestresults.Parameters = make(map[string]string)
			params := strings.Split(parstring, ",")
			for _, p := range params {
				parts := strings.Split(p, "=")
				ptestresults.Parameters[parts[0]] = parts[1]
			}
			for k, v := range mp {
				ptestresults.Results[k] = v
			}
			subtests = append(subtests, ptestresults)
		}
		report.Benchmarks[test] = subtests
	}

	jsonb, err := json.Marshal(&report)
	if err != nil {
		panic(err)
	}

	fmt.Printf("json: %s\n", string(jsonb))
	//TODO write report
}

//reportData is TestName -> ParameterString -> Name -> Value
var reportData map[string]map[string]map[string]float64

func init() {
	reportData = make(map[string]map[string]map[string]float64)
}

func ReportValue(ctx *TestContext, name string, value float64) {
	parts := strings.SplitN(ctx.T.Name(), "/", 2)
	testname := parts[0]
	parstring := parts[1]
	_, ok := reportData[testname]
	if !ok {
		reportData[testname] = make(map[string]map[string]float64)
	}
	_, ok := reportData[testname][parstring]
	if !ok {
		reportData[testname][parstring] = make(map[string]float64)
	}
	reportData[testname][parstring][name] = value
	fmt.Printf(">>> %s :: %s = %f\n", ctx.T.Name(), name, value)
}
