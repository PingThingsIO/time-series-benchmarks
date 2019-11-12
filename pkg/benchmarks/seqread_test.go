package benchmarks

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
)

func TestSequentialReadSingleStream(t *testing.T) {
	ctx := Context(t)
	if ctx.T == nil {
		panic("here")
	}
	checkInsertion(ctx)

	db := ctx.DB()
	uu := insertionInfo.UUIDs[0]
	stream, err := db.ObtainStream(uu)
	spew.Dump(err)
	if !assert.NoError(ctx, err) {
		ctx.FailNow()
		return
	}

	start := time.Now()
	totalpts := 0
	chanpts, chanerr := stream.QueryValues(insertionInfo.Source.StartTime(), insertionInfo.Source.EndTime())
	for arr := range chanpts {
		totalpts += len(arr)
	}
	fmt.Printf("query channel ended\n")
	delta := time.Since(start)
	deltams := float64(delta/1000) / 1000
	err = <-chanerr
	if !assert.NoError(ctx, err) {
		ctx.FailNow()
		return
	}
	ReportValue(ctx, "SingleStreamSequentialRead.Points", float64(totalpts))
	ReportValue(ctx, "SingleStreamSequentialRead.Duration", deltams)
	ReportValue(ctx, "SingleStreamSequentialRead.MPPS", float64(totalpts)/deltams/1000)
}

func TestSequentialReadParallelStream(t *testing.T) {
	ctx := Context(t)
	checkInsertion(ctx)

	db := ctx.DB()

	ParametricSweepInt(ctx, "SequentialReadParallelNum", func(ctx *TestContext, parallelism int) {
		wgGetStreams := sync.WaitGroup{}
		wgGetStreams.Add(parallelism)
		wgComplete := sync.WaitGroup{}
		wgComplete.Add(parallelism)

		firstDone := sync.Once{}
		var totalpoints uint64
		for idx := 0; idx < parallelism; idx++ {
			go func(idx int) {
				uu := insertionInfo.UUIDs[idx]
				stream, err := db.ObtainStream(uu)
				if !assert.NoError(ctx, err) {
					ctx.FailNow()
					return
				}
				wgGetStreams.Done()
				wgGetStreams.Wait()

				start := time.Now()

				chanpts, chanerr := stream.QueryValues(insertionInfo.Source.StartTime(), insertionInfo.Source.EndTime())
				for arr := range chanpts {
					atomic.AddUint64(&totalpoints, uint64(len(arr)))
				}
				err = <-chanerr
				if !assert.NoError(ctx, err) {
					ctx.FailNow()
					return
				}
				firstDone.Do(func() {
					delta := time.Since(start)
					deltams := float64(delta/1000) / 1000
					totalpts := atomic.LoadUint64(&totalpoints)
					ReportValue(ctx, "FullParallelSequentialRead.Points", float64(totalpts))
					ReportValue(ctx, "FullParallelSequentialRead.Duration", deltams)
					ReportValue(ctx, "FullParallelSequentialRead.MPPS", float64(totalpts)/deltams/1000)
				})
				wgComplete.Done()
			}(idx)
		}
		wgComplete.Wait()
	})

	//TODO partial parallel

	//fmt.Printf("read %d points in %.3fms (%.3f mpps)\n", totalpts, deltams, float64(totalpts)/deltams/1000)
}
