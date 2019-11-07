package dummyds

import (
	"fmt"
	"math"
	"time"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/davecgh/go-spew/spew"
	"github.com/valyala/fastrand"
)

type dummyDS struct {
	start int64
	end   int64
}

func NewDummyDataSource() iface.DataSource {
	startTime, err := time.Parse(time.RFC3339, "2010-01-01T1:00:00-00:00")
	if err != nil {
		panic(err)
	}
	return &dummyDS{
		start: startTime.UnixNano(),
	}
}
func (ds *dummyDS) StartTime() int64 {
	return ds.start
}
func (ds *dummyDS) EndTime() int64 {
	if ds.end == 0 {
		panic("can only call EndTime after Materialize")
	}
	return ds.end
}
func (ds *dummyDS) MaterializePMU(p *iface.MaterializePMUParams) []chan []iface.Point {
	fmt.Printf("materializepmu called\n")
	spew.Dump(p)
	rv := make([]chan []iface.Point, p.NumStreams)
	if p.BatchSize == 0 {
		panic("batch size cannot be zero")
	}
	ds.end = ds.start + int64(p.Timespan)
	for i := 0; i < p.NumStreams; i++ {
		//Be careful making this too big, it will use a bit of memory for
		//large numbers of streams
		rv[i] = make(chan []iface.Point, 3)
		go func(i int) {
			cursor := ds.start
			batch := make([]iface.Point, 0, p.BatchSize)
			if p.SubSample == 0 {
				p.SubSample = 1
			}
			rng := &fastrand.RNG{}
			period := int64((1000000000 / 120) * p.SubSample)
			v := 1.0
			for cursor < ds.end {
				//TODO value
				v += float64(rng.Uint32())/float64(math.MaxUint32) - 0.5
				if p.TruncateValue {
					v = float64(float32(v))
				}
				point := iface.Point{Time: cursor, Value: v}
				cursor += period
				if p.TSJitter {
					//TODO
				}
				batch = append(batch, point)
				if len(batch) == p.BatchSize {
					rv[i] <- batch
					batch = make([]iface.Point, 0, p.BatchSize)
				}
			}
			close(rv[i])
		}(i)
	}

	return rv
}
