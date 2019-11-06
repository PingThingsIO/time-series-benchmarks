package seedds

import (
	"log"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type seedDS struct {
	start int64
	end   int64
	path  string
}

// NewSeedDataSource does stuff like creating a new data source from the seed dataset
func NewSeedDataSource(path string) iface.DataSource {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Println("Can't open file")
		return nil
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(PMUDevice), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return nil
	}
	defer pr.ReadStop()

	// get first point so we know initial offset
	points := make([]PMUDevice, 1)
	if err = pr.Read(&points); err != nil {
		log.Println("Read error", err)
	}
	offset := points[0].Timestamp

	// 1546300800000000000 == 2019/1/1
	return &seedDS{
		start: 1546300800000000000 + *offset,
		end:   0,
		path:  path,
	}
}

func (ds *seedDS) StartTime() int64 {
	return ds.start
}

func (ds *seedDS) EndTime() int64 {
	if ds.end == 0 {
		panic("can only call EndTime after Materialize")
	}
	return ds.end
}

func buildTimes(times []int64, ds *seedDS, p *iface.MaterializePMUParams) (results []int64) {
	log.Printf("enter buildTimes: start: %d, end: %d", ds.start, ds.end)
	cursor := ds.start
	prevTime := int64(0)
	offset := ds.start
	period := int64((1000000000 / 120) * p.SubSample)

	for cursor < ds.end {

		// loop through available time offsets
		for idx := 0; idx < len(times); idx++ {

			// do not add first timestamp to ds.start
			if prevTime > 0 {
				cursor = times[idx] + offset
			}

			// TODO handle offsets

			if !p.TSJitter {
				// TODO remove jitter
			}

			if cursor < ds.end {
				results = append(results, cursor)
				prevTime = cursor
			}

		}

		offset = prevTime + period

	}
	return results
}

func enqueue(ch chan []iface.Point, times []int64, values []float64, p *iface.MaterializePMUParams) {
	batch := make([]iface.Point, 0, p.BatchSize)
	timeIndex := 0

	// TODO handle SubSample

	// loop until last time reached
	for timeIndex < len(times) {

		// loop through our seed data and create points
		for _, val := range values {
			batch = append(batch, iface.Point{Time: times[timeIndex], Value: val})
			timeIndex++

			// send batch
			if len(batch) == p.BatchSize || timeIndex == len(times) {
				ch <- batch
				batch = make([]iface.Point, 0, p.BatchSize)
			}

			// exit if we've reached the end of time
			if timeIndex == len(times) {
				break
			}
		}
	}

	close(ch)
}

func (ds *seedDS) MaterializePMU(p *iface.MaterializePMUParams) []chan []iface.Point {
	if p.BatchSize == 0 {
		panic("batch size cannot be zero")
	}
	if p.SubSample == 0 {
		p.SubSample = 1
	}
	ds.end = ds.start + int64(p.Timespan)
	data, offsets := extract(ds.path, false)
	times := buildTimes(offsets, ds, p)
	rv := make([]chan []iface.Point, p.NumStreams)

	for i := 0; i < p.NumStreams; i++ {
		rv[i] = make(chan []iface.Point, 3)

		// go func(ch chan []iface.Point, times []int64, values []float64) {
		// 	enqueue(ch, times, values, p)
		// }(rv[i], times, data[i%15])

		go enqueue(rv[i], times, data[i%15], p)

	}

	return rv
}
