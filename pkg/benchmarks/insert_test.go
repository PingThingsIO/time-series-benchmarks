package benchmarks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dummyds "github.com/PingThingsIO/time-series-benchmarks/pkg/dummyDS"
	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

//dataInsertion is used to ensure that the dataset is inserted only once
//even if multiple tests require it to be inserted
var dataInsertion = sync.Once{}

//insertionInfo records information about the streams that the dataset was
//inserted into, for use by subsequent tests
var insertionInfo tinsertionInfo

//doinsert actually performs the insert. Pulled out to avoid duplication
func doinsert(ctx *TestContext) {

	db := ctx.DB()

	//TODO this should be parameterized?
	ds := dummyds.NewDummyDataSource()

	//Store the data source so that tests can query it for information
	insertionInfo.Source = ds

	//Store the generation parameters for use by other tests
	insertionInfo.Params = &iface.MaterializePMUParams{
		NumStreams:    ctx.ParamInt("InsertNumStreams"),
		Timespan:      uint64(ctx.ParamInt("InsertSpanSeconds") * 1000000000),
		SubSample:     ctx.ParamInt("InsertSubsample"),
		TSJitter:      ctx.ParamBool("InsertTSJitter"),
		TruncateValue: ctx.ParamBool("Insert32Bit"),
		BatchSize:     ctx.ParamInt("InsertBatchSize"),
	}
	insertionInfo.UUIDs = make([]uuid.UUID, insertionInfo.Params.NumStreams)

	sources := ds.MaterializePMU(insertionInfo.Params)

	//Barrier to ensure that all the stream creation is done before
	//inserts (to make metrics cleaner)
	creationWG := sync.WaitGroup{}
	//Barrier for completion
	completeWG := sync.WaitGroup{}

	creationWG.Add(len(sources))
	completeWG.Add(len(sources))

	//Only one worker needs to report the time it took to create streams
	//TODO have this done by a distinct goroutine instead of a race by
	//workers
	createReport := sync.Once{}

	//We want to measure both the total time to insert, as well as the
	//"peak" bandwidth that occurs while all parallel streams are still
	//inserting. It's natural for the bandwidth to drop as you are finishing
	//stragglers, and while that is interesting, we want it to be a distinct
	//metric
	firstCompleteDone := sync.Once{}

	var insertedpoints uint64

	start := time.Now()

	for idx, src := range sources {
		go func(idx int, src chan []iface.Point) {
			uu := uuid.NewRandom()
			insertionInfo.UUIDs[idx] = uu
			s, err := db.CreateStream(uu)
			if !assert.NoError(ctx, err) {
				creationWG.Done()
				completeWG.Done()
				ctx.FailNow()
				return
			}
			creationWG.Done()
			creationWG.Wait()
			createReport.Do(func() {
				nw := time.Now()
				delta := nw.Sub(start)
				deltams := float64(delta/1000) / 1000
				fmt.Printf("Stream creation took %.3f ms\n", deltams)
			})
			startInsert := time.Now()

			progressctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() {
				for {
					time.Sleep(3 * time.Second)
					if progressctx.Err() != nil {
						return
					}
					totalpts := atomic.LoadUint64(&insertedpoints)
					fmt.Printf("inserted %d points\n", totalpts)
				}
			}()
			for batch := range src {
				err := s.Insert(batch)
				atomic.AddUint64(&insertedpoints, uint64(len(batch)))
				if !assert.NoError(ctx, err) {
					completeWG.Done()
					ctx.FailNow()
					return
				}
			}
			completeWG.Done()
			firstCompleteDone.Do(func() {
				nw := time.Now()
				totalpts := atomic.LoadUint64(&insertedpoints)
				delta := nw.Sub(startInsert)
				deltams := float64(delta/1000) / 1000
				fmt.Printf("full parallel insert did %d points in %.3f ms (%.3f mpps)\n", totalpts, deltams, float64(totalpts)/deltams/1000)
			})
		}(idx, src)
	}
	completeWG.Wait()
}
func TestInsert(t *testing.T) {
	ctx := Context(t)
	ran := false
	dataInsertion.Do(func() {
		doinsert(ctx)
		ran = true
	})
	if !ran {
		t.Skipf("data insertion already ran")
	}
}

func checkInsertion(ctx *TestContext) {
	dataInsertion.Do(func() {
		child := ctx.Child("dependency", "insert")
		doinsert(child)
	})
}
