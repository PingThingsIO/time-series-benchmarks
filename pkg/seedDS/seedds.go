package seedds

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

type seedDS struct {
	start      int64
	end        int64
	numSamples int64
	path       string
}

// NewSeedDataSource does stuff
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

	// get first point
	points := make([]PMUDevice, 1)
	if err = pr.Read(&points); err != nil {
		log.Println("Read error", err)
	}
	first := points[0]

	// find total samples and skip the the last
	numSamples := pr.GetNumRows()
	skip := int64(numSamples - 2)
	pr.SkipRows(skip)

	// 1546300800000000000 == 2019/1/1
	return &seedDS{
		start:      1546300800000000000 + *first.Timestamp,
		end:        0,
		numSamples: pr.GetNumRows(),
		path:       path,
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

func enqueue(ch chan []iface.Point, fr source.ParquetFile, streamIndex int, ds *seedDS, p *iface.MaterializePMUParams) {
	parallelReaders := int64(1)

	// create new reader
	pr, err := reader.NewParquetReader(fr, new(PMUDevice), parallelReaders)
	log.Println(streamIndex)
	if err != nil {
		panic(err)
	}

	cursor := ds.start
	prevTime := int64(0)
	offset := ds.start
	batch := make([]iface.Point, 0, p.BatchSize)
	counter := 0

	for cursor < ds.end {

		// read batch of values
		times, _, _, err := pr.ReadColumnByIndex(15, p.BatchSize)
		vals, _, _, err := pr.ReadColumnByIndex(streamIndex, p.BatchSize)
		if err != nil {
			panic(err)
		}

		if len(times) == 0 {
			pr.ReadStop()
			pr, err = reader.NewParquetReader(fr, new(PMUDevice), parallelReaders)
			if err != nil {
				panic(err)
			}
			period := int64((1000000000 / 120) * p.SubSample)
			offset = prevTime + period
			continue
		}

		// create points and add to batch
		for idx := 0; idx < len(times); idx++ {

			// send batch if full or we've reached the end
			if len(batch) == p.BatchSize || cursor >= ds.end {
				log.Printf("adding batch: stream: %d, cursor: %d, batch points: %d,  total points: %d", streamIndex, cursor, len(batch), counter)
				ch <- batch
				batch = make([]iface.Point, 0, p.BatchSize)

				// break if finished
				if cursor >= ds.end {
					close(ch)
					break
				}
			}

			// do not add first timestamp to ds.start
			if prevTime > 0 {
				cursor = times[idx].(int64) + offset
			}

			// TODO: if point.Time within XXX of ds.end then change to ds.end
			if cursor < ds.end {
				val, ok := vals[idx].(float64)
				if !ok {
					val = float64(vals[idx].(int64))
				}

				point := iface.Point{Time: cursor, Value: val}
				batch = append(batch, point)
				prevTime = cursor
				counter++
				// log.Printf("enqueued point: %d, time: %d, offset: %d, counter: %d", cursor, times[idx].(int64), offset, counter)
			}

		}
	}
	pr.ReadStop()

}

func (ds *seedDS) MaterializePMU(p *iface.MaterializePMUParams) []chan []iface.Point {
	fmt.Printf("materializepmu called\n")
	if p.BatchSize == 0 {
		panic("batch size cannot be zero")
	}
	if p.SubSample == 0 {
		p.SubSample = 1
	}

	data, err := ioutil.ReadFile(ds.path)
	if err != nil {
		panic(err)
	}

	fr, err := buffer.NewBufferFile(data)
	if err != nil {
		log.Println("Can't open file")
		panic(err)
	}

	ds.end = ds.start + int64(p.Timespan)
	rv := make([]chan []iface.Point, p.NumStreams)

	for i := 0; i < p.NumStreams; i++ {
		rv[i] = make(chan []iface.Point, 3)
		parquetColumnIndex := i % 15
		go func(ch chan []iface.Point) {
			enqueue(ch, fr, parquetColumnIndex, ds, p)
		}(rv[i])
	}

	return rv
}

// fieldNames := []string{"VPHMA", "VPHMB", "VPHMC", "VPHAA", "VPHAB", "VPHAC", "IPHMA", "IPHMB", "IPHMC", "IPHAA", "IPHAB", "IPHAC", "FREQ", "DFDT", "DIGI"}
// fmt.Println(fieldNames[i%len(fieldNames)])
