package benchmarks

import "fmt"

func ReportValue(ctx *TestContext, name string, value float64) {
	fmt.Printf(">>> %s :: %s = %f\n", ctx.T.Name(), name, value)
}
