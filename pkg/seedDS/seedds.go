package seedds

import (
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type seedDS struct {
	start int64
	end   int64
}

var seedFileURL = "https://ni4ai-seed-data.s3.amazonaws.com/pmu-seed-dataset.parquet.gzip"
var seedFilePath = "pmu-seed-dataset.parquet.gzip"

// NewSeedDataSource does stuff like creating a new data source from the seed dataset
func NewSeedDataSource() iface.DataSource {

	if !fileExists(seedFilePath) {
		log.Println("seed data set not found, downloading")
		err := downloadFile(seedFilePath, seedFileURL)
		if err != nil {
			panic(err)
		}
		log.Printf("download complete")
	}

	fr, err := local.NewLocalFileReader(seedFilePath)
	if err != nil {
		log.Println("Can't open file")
		panic(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(PMUDevice), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		panic(err)
	}
	defer pr.ReadStop()

	// get first point so we know initial offset
	points := make([]PMUDevice, 1)
	if err = pr.Read(&points); err != nil {
		log.Println("Parquet read error", err)
		panic(err)
	}
	offset := points[0].Timestamp

	// 1388534400000000000 == 2014/1/1
	return &seedDS{
		start: 1388534400000000000 + *offset,
		end:   0,
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

func enqueue(ch chan []iface.Point, offsets []int64, values []float64, ds *seedDS, p *iface.MaterializePMUParams) {

	counter := 0
	cursor := ds.start
	prevTime := int64(0)
	boundary := ds.start
	batch := make([]iface.Point, 0, p.BatchSize)
	period := int64((1000000000 / 120) * p.SubSample)
	period120 := float64(8333333)
	period120i := int64(8333333)

	for cursor < ds.end {

		// create points and add to batch
		for idx := 0; idx < len(offsets); idx += p.SubSample {

			// do not add first timestamp to ds.start
			if prevTime > 0 {
				cursor = offsets[idx] + boundary
			}

			// random skip
			if rand.Float64() < p.HoleProbability {
				continue
			}

			// remove jitter if requested
			if !p.TSJitter {
				baseTime := int64(cursor / int64(1e9))
				ns := cursor % int64(1e9)
				increment := int64(math.Round(float64(ns+500) / period120))
				cursor = int64(baseTime*1e9) + ((increment*period120i)/1000)*1000
			}

			// add point
			if cursor < ds.end {
				batch = append(batch, iface.Point{Time: cursor, Value: values[idx]})
				prevTime = cursor
				counter++
			}

			// send batch if full or we've reached the end
			if len(batch) == p.BatchSize || cursor >= ds.end {
				ch <- batch
				batch = make([]iface.Point, 0, p.BatchSize)

				// break if finished
				if cursor >= ds.end {
					break
				}
			}

		}

		// add next time step as time offsets are zero based
		boundary = prevTime + period
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
	data, offsets := extract(seedFilePath, p.TruncateValue)
	rv := make([]chan []iface.Point, p.NumStreams)

	for i := 0; i < p.NumStreams; i++ {
		rv[i] = make(chan []iface.Point, 3)
		go enqueue(rv[i], offsets, data[i%15], ds, p)
	}

	return rv
}

func downloadFile(filepath string, url string) (err error) {

	// Create the file
	f, err := os.OpenFile(seedFilePath+".tmp", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Write to file
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}

	f.Close()

	err = os.Rename(seedFilePath+".tmp", seedFilePath)
	if err != nil {
		return err
	}

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}
