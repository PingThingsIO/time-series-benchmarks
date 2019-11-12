package benchmarks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

//FinalReport is the structure of the final json file that is written
type FinalReport struct {
	Fingerprint map[string]string        `json:"fingerprint"`
	Benchmarks  map[string][]TestResults `json:"benchmarks"`
}

//TestResults stores the results of a given test, across multiple
//groups of parameters
type TestResults struct {
	Parameters map[string]string  `json:"parameters"`
	Results    map[string]float64 `json:"results"`
}

//writeReport will take all values saved from ReportValue and write them
//to the given json file
func writeReport(name string) {

	report := FinalReport{
		Benchmarks: make(map[string][]TestResults),
	}

	fingerprint := make(map[string]string)
	fingerprintdata, err := ioutil.ReadFile("fingerprint.ini")
	if err != nil {
		fmt.Printf("error reading fingerprint (skipping): %v\n", err)
		fingerprint["unofficial"] = "true"
	} else {
		rdr := bytes.NewBuffer(fingerprintdata)
		for {
			l, err := rdr.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			l = strings.TrimSpace(l)
			parts := strings.SplitN(l, "=", 2)
			fingerprint[parts[0]] = parts[1]
		}
	}
	fingerprint["time"] = time.Now().Format(time.RFC3339)

	report.Fingerprint = fingerprint

	for test, pars := range reportData {
		subtests := make([]TestResults, 0)
		for parstring, mp := range pars {
			ptestresults := TestResults{
				Results:    make(map[string]float64),
				Parameters: make(map[string]string),
			}
			if len(parstring) > 0 {
				params := strings.Split(parstring, "/")
				for _, p := range params {
					parts := strings.Split(p, "=")
					ptestresults.Parameters[parts[0]] = parts[1]
				}
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

	err = ioutil.WriteFile(name, jsonb, 0666)
	if err != nil {
		panic(err)
	}
}

//reportData is TestName -> ParameterString -> Name -> Value
var reportData map[string]map[string]map[string]float64

func init() {
	reportData = make(map[string]map[string]map[string]float64)
}

//ReportValue will record a metric in the final performance report
func ReportValue(ctx *TestContext, name string, value float64) {
	parts := strings.SplitN(ctx.T.Name(), "/", 2)
	testname := parts[0]
	var parstring string
	if len(parts) > 1 {
		parstring = parts[1]
	}
	_, ok := reportData[testname]
	if !ok {
		reportData[testname] = make(map[string]map[string]float64)
	}
	_, ok = reportData[testname][parstring]
	if !ok {
		reportData[testname][parstring] = make(map[string]float64)
	}
	reportData[testname][parstring][name] = value
	fmt.Printf(">>> %s :: %s = %f\n", ctx.T.Name(), name, value)
}
