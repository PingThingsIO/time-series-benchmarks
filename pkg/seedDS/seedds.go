package seedds

import (
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
)

type seedDS struct {
	start  int64
	end    int64
	local  string
	remote string
}

// NewSeedDataSource does stuff like creating a new data source from the seed dataset
func NewSeedDataSource() iface.DataSource {

	return csvSeedDataSource()

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

// continuously drops points into a channel given some seed data
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

	// data, offsets := parquetExtract(ds.local, p.TruncateValue)
	data, offsets := csvExtract(ds.local, p.TruncateValue)

	rv := make([]chan []iface.Point, p.NumStreams)

	for i := 0; i < p.NumStreams; i++ {
		rv[i] = make(chan []iface.Point, 3)
		go enqueue(rv[i], offsets, data[i%15], ds, p)
	}

	return rv
}

// used to fetch parquet or csv seed file
func downloadFile(filepath string, url string) (err error) {

	t := time.Now()
	defer func() { log.Printf("seed file downloaded: %s", time.Since(t)) }()

	// Create the file
	f, err := os.OpenFile(filepath+".tmp", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
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

	err = os.Rename(filepath+".tmp", filepath)
	if err != nil {
		return err
	}
	return nil
}

// simple check for file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}
