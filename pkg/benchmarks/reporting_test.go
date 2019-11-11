package benchmarks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/davecgh/go-spew/spew"
)

type FinalReport struct {
	Fingerprint map[string]string        `json:"fingerprint"`
	Benchmarks  map[string][]TestResults `json:"benchmarks"`
}

type TestResults struct {
	Parameters map[string]string  `json:"parameters"`
	Results    map[string]float64 `json:"results"`
}

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
	//fmt.Printf("json: %s\n", string(jsonb))
	//TODO write report
}

//reportData is TestName -> ParameterString -> Name -> Value
var reportData map[string]map[string]map[string]float64

func init() {
	reportData = make(map[string]map[string]map[string]float64)
}

func ReportValue(ctx *TestContext, name string, value float64) {
	fmt.Printf("parstring is %s\n", ctx.T.Name())
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
	spew.Dump(reportData)
	fmt.Printf(">>> %s :: %s = %f\n", ctx.T.Name(), name, value)
}
