package benchmarks

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

//TestSmallAggregation does a scan over the entire dataset, requesting windows
//of size SmallAggregationWindowSize.
func TestSmallAggregation(t *testing.T) {
	//Check the dataset exists
	ctx := Context(t)
	checkInsertion(ctx)

	db := ctx.DB()

	//Get the size of each window (in nanoseconds)
	aggSize := ctx.ParamInt("SmallAggregationWindowSize")

	//Use the first stream to test
	uu := insertionInfo.UUIDs[0]
	stream, err := db.ObtainStream(uu)
	if !assert.NoError(ctx, err) {
		ctx.FailNow()
		return
	}

	start := time.Now()
	totalwindows := 0
	totalpoints := 0

	//Query the aggregates and count how many points they represent
	chanwnd, chanerr := stream.QueryAggregates(insertionInfo.Source.StartTime(), insertionInfo.Source.EndTime(), uint64(aggSize))
	for arr := range chanwnd {
		totalwindows += len(arr)
		for _, w := range arr {
			totalpoints += int(w.Count)
		}
	}

	//Measure how long that took
	delta := time.Since(start)
	deltams := float64(delta/1000) / 1000

	//Ensure we got no errors during the query
	err = <-chanerr
	if !assert.NoError(ctx, err) {
		ctx.FailNow()
		return
	}

	//TODO report this properly
	fmt.Printf("read %d windows covering %d points in %.3fms (%.3f mpps)\n", totalwindows, totalpoints, deltams, float64(totalpoints)/deltams/1000)
}
