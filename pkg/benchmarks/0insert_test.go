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
var insertionInfo tInsertionData

//doinsert actually performs the insert. Pulled out to avoid duplication
func doinsert(ctx *TestContext) {

	db := ctx.DB()

	//TODO this should be parameterized?
	ds := dummyds.NewDummyDataSource()

	insertWithParameters := func(ctx *TestContext, p *iface.MaterializePMUParams) {
		//Store the data source so that tests can query it for information
		insertionInfo.Source = ds
		// The last insert will be the params that all the other tests use
		insertionInfo.Params = p

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

		//For the whole test time (create, barrier, insert, barrier)
		start := time.Now()
		for idx, src := range sources {
			go func(idx int, src chan []iface.Point) {
				uu := uuid.NewRandom()
				insertionInfo.UUIDs[idx] = uu
				fmt.Printf("created %s\n", uu.String())
				s, err := db.CreateStream(uu)
				if !assert.NoError(ctx, err) {
					creationWG.Done()
					completeWG.Done()
					ctx.FailNow()
					return
				}
				//Signal we are done, and wait for others to finish creating
				creationWG.Done()
				creationWG.Wait()
				createReport.Do(func() {
					//TODO real reporting
					//TODO standalone worker for reporting?
					nw := time.Now()
					delta := nw.Sub(start)
					deltams := float64(delta/1000) / 1000
					ReportValue(ctx, "StreamCreation", deltams)
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
					ReportValue(ctx, "FullParallelInsert.Points", float64(totalpts))
					ReportValue(ctx, "FullParallelInsert.Duration", deltams)
					ReportValue(ctx, "FullParallelInsert.MPPS", float64(totalpts)/deltams/1000)
				})
			}(idx, src)
		}
		completeWG.Wait()

	} // end of insert with parameters

	//Multiple nested loops for each parameter
	ParametricSweepInt(ctx, "InsertNumStreams", func(ctx *TestContext, insertNumStreams int) {
		ParametricSweepInt(ctx, "InsertSpanSeconds", func(ctx *TestContext, insertSpanSeconds int) {
			ParametricSweepInt(ctx, "InsertSubsample", func(ctx *TestContext, insertSubsample int) {
				ParametricSweepBool(ctx, "InsertTSJitter", func(ctx *TestContext, insertTSJitter bool) {
					ParametricSweepBool(ctx, "Insert32Bit", func(ctx *TestContext, insert32Bit bool) {
						ParametricSweepInt(ctx, "InsertBatchSize", func(ctx *TestContext, insertBatchSize int) {
							params := &iface.MaterializePMUParams{
								NumStreams:    insertNumStreams,
								Timespan:      uint64(insertSpanSeconds * 1000000000),
								SubSample:     insertSubsample,
								TSJitter:      insertTSJitter,
								TruncateValue: insert32Bit,
								BatchSize:     insertBatchSize,
							}
							insertWithParameters(ctx, params)
						})
					})
				})
			})
		})
	})
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
		ctx.Run("Dependency=insert", func(ctx *TestContext) {
			doinsert(ctx)
		})
	})
}
