package benchmarks

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

type benchmarkContextKey string

var ContextTestName benchmarkContextKey = "testname"
var ContextReport benchmarkContextKey = "report"

type tInsertionData struct {
	Params *iface.MaterializePMUParams
	UUIDs  []uuid.UUID
	Source iface.DataSource
}

type BenchmarkContext struct {
	Config  map[string]interface{}
	Reports map[string]*Report
	//	Tests  map[string]*ReportTest
}

type Report struct {
}
type TestContext struct {
	context.Context
	cancel context.CancelFunc
	T      *testing.T
	//	name   string
	BC *BenchmarkContext
}

func (tc *TestContext) Param(name string) interface{} {
	//First look for parameter in db specific
	bsection, ok := tc.BC.Config["Benchmarking"]
	if !ok {
		tc.T.Fatalf("config is missing Benchmarking section")
	}
	csection, ok := bsection.(map[interface{}]interface{})["Common"]
	if !ok {
		tc.T.Fatalf("config is missing Benchmarking.Common section")
	}
	tsection, ok := bsection.(map[interface{}]interface{})[Target]
	if !ok {
		tc.T.Fatalf("config is missing Benchmarking.%s section", Target)
	}
	val, ok := tsection.(map[interface{}]interface{})[name]
	if !ok {
		//Then look in common
		val, ok = csection.(map[interface{}]interface{})[name]
		if !ok {
			tc.T.Fatalf("test requires parameter %q, not found in config", name)
		}
	}
	return val
}

func (tc *TestContext) ParamBool(name string) bool {
	val, ok := tc.Param(name).(bool)
	if !ok {
		tc.T.Fatalf("expected parameter %q to be a boolean", name)
	}
	return val
}
func (tc *TestContext) ParamInt(name string) int {
	val, ok := tc.Param(name).(int)
	if !ok {
		tc.T.Fatalf("expected parameter %q to be an integer", name)
	}
	return val
}
func (tc *TestContext) ParamIntSlice(name string) []int {
	val, ok := tc.Param(name).([]interface{})
	if !ok {
		//Perhaps it is just a single integer
		val, ok := tc.Param(name).(int)
		if ok {
			return []int{val}
		}
		tc.T.Fatalf("expected parameter %q to be an integer array", name)
	}
	rv := make([]int, len(val))
	for i, v := range val {
		rv[i] = v.(int)
	}
	return rv
}
func (tc *TestContext) ParamBoolSlice(name string) []bool {
	val, ok := tc.Param(name).([]interface{})
	if !ok {
		//Perhaps it is just a single bool
		val, ok := tc.Param(name).(bool)
		if ok {
			return []bool{val}
		}
		tc.T.Fatalf("expected parameter %q to be an integer array", name)
	}
	rv := make([]bool, len(val))
	for i, v := range val {
		rv[i] = v.(bool)
	}
	return rv
}

func (tc *TestContext) child() *TestContext {
	sub, cancel := context.WithCancel(tc.Context)
	return &TestContext{
		Context: sub,
		cancel:  cancel,
		T:       nil, //will be set by caller
		BC:      tc.BC,
	}
}

func (tc *TestContext) DB() iface.AbstractDatabase {
	if tc.T == nil {
		panic("why is T nil?")
	}
	rv, err := Factory.Initialize(tc.BC.Config)
	if err != nil {
		tc.T.Fatalf("target failed to initialize: %v", err)
	}
	return rv
}

var setup *BenchmarkContext

var _ require.TestingT = &TestContext{}

func Context(t *testing.T) *TestContext {
	// v := reflect.ValueOf(*t)
	// fmt.Printf("Context called\n")
	// name := v.FieldByName("name").String()
	ctx, cancel := context.WithCancel(context.Background())
	return &TestContext{
		Context: ctx,
		cancel:  cancel,
		T:       t,
		BC:      setup,
	}
}

func (tc *TestContext) Errorf(format string, args ...interface{}) {
	tc.T.Errorf(format, args...)
}

func (tc *TestContext) FailNow() {
	tc.T.Fail()
	tc.cancel()
}

func TestMain(m *testing.M) {
	fmt.Printf("testmain ran")
	cfg := "config.yaml"
	if os.Getenv("CONFIG") != "" {
		cfg = os.Getenv("CONFIG")
	}
	cfgfile, err := ioutil.ReadFile(cfg)
	if err != nil {
		fmt.Printf("could not read config file %q: %v\n", cfg, err)
		os.Exit(1)
	}
	cfgmap := make(map[string]interface{})
	err = yaml.Unmarshal(cfgfile, cfgmap)
	if err != nil {
		fmt.Printf("could not parse config file %q: %v\n", cfg, err)
		os.Exit(1)
	}

	//TODO parse config
	setup = &BenchmarkContext{
		Config: cfgmap,
	}
	m.Run()

	writeReport()
}

func ParametricSweepInt(ctx *TestContext, name string, f func(ctx *TestContext, v int)) {
	values := ctx.ParamIntSlice(name)
	for _, v := range values {
		subctx := ctx.child()
		ctx.T.Run(fmt.Sprintf("%s=%v", name, v), func(t *testing.T) {
			subctx.T = t
			f(subctx, v)
		})
	}
}

func ParametricSweepBool(ctx *TestContext, name string, f func(ctx *TestContext, v bool)) {
	values := ctx.ParamBoolSlice(name)
	for _, v := range values {
		subctx := ctx.child()
		ctx.T.Run(fmt.Sprintf("%s=%v", name, v), func(t *testing.T) {
			subctx.T = t
			f(subctx, v)
		})
	}
}

func (ctx *TestContext) Run(name string, f func(ctx *TestContext)) {
	subctx := ctx.child()
	ctx.T.Run(name, func(t *testing.T) {
		subctx.T = t
		f(subctx)
	})
}

func Repeat(ctx *TestContext, name string, f func(ctx *TestContext, iteration int)) {
	count := ctx.ParamInt(name)
	for i := 0; i < count; i++ {
		subctx := ctx.child()
		ctx.T.Run(fmt.Sprintf("iter=%d", i), func(t *testing.T) {
			subctx.T = t
			f(subctx, i)
		})
	}
}
func ParametricSweepFloat(name string, values []string, f func(ctx context.Context, v float64)) {
	panic("ni")
}
