package seedds

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
)

var csvSeedFileURL = "http://ni4ai-seed-data.s3.amazonaws.com/pmu-seed-dataset.full.csv.gz"
var csvSeedFilePath = "pmu-seed-dataset.csv.gz"

func csvSeedDataSource() iface.DataSource {

	if !fileExists(csvSeedFilePath) {
		log.Println("seed data set not found, downloading")
		err := downloadFile(csvSeedFilePath, csvSeedFileURL)
		if err != nil {
			panic(err)
		}
	}

	offset, err := firstOffset(csvSeedFilePath)
	if err != nil {
		panic(0)
	}

	// 1388534400000000000 == 2014/1/1
	return &seedDS{
		start:  1388534400000000000 + offset,
		end:    0,
		local:  csvSeedFilePath,
		remote: csvSeedFileURL,
	}

}

func firstOffset(path string) (int64, error) {
	f, err := os.Open(csvSeedFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 32768)
	gr, err := gzip.NewReader(reader)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	var rec []string
	r := bufio.NewReader(gr)
	cr := csv.NewReader(r)
	for i := 0; i < 2; i++ {
		rec, err = cr.Read()
		if err != nil {
			log.Fatal(err)
		}
	}

	return strconv.ParseInt(rec[0], 10, 64)

}

func csvTransform(raw string, truncate bool) float64 {
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		log.Fatal(err)
	}

	if truncate {
		return float64(float32(val))
	}
	return val
}

func csvExtract(path string, truncate bool) ([][]float64, []int64) {
	t := time.Now()
	defer func() { log.Printf("data extraction complete: %s", time.Since(t)) }()

	times := make([]int64, 0)
	values := make([][]float64, 15)
	for i := 0; i < len(values); i++ {
		values[i] = make([]float64, 0)
	}

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 32768)
	gr, err := gzip.NewReader(reader)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	// open csv reader and skip header
	r := bufio.NewReader(gr)
	cr := csv.NewReader(r)
	cr.Read()

	// loop through csv rows and add fields to individual value arrays
	for {
		record, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		t, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		times = append(times, t)

		values[0] = append(values[0], csvTransform(record[1], truncate))
		values[1] = append(values[1], csvTransform(record[2], truncate))
		values[2] = append(values[2], csvTransform(record[3], truncate))
		values[3] = append(values[3], csvTransform(record[4], truncate))
		values[4] = append(values[4], csvTransform(record[5], truncate))
		values[5] = append(values[5], csvTransform(record[6], truncate))
		values[6] = append(values[6], csvTransform(record[7], truncate))
		values[7] = append(values[7], csvTransform(record[8], truncate))
		values[8] = append(values[8], csvTransform(record[9], truncate))
		values[9] = append(values[9], csvTransform(record[10], truncate))
		values[10] = append(values[10], csvTransform(record[11], truncate))
		values[11] = append(values[11], csvTransform(record[12], truncate))
		values[12] = append(values[12], csvTransform(record[13], truncate))
		values[13] = append(values[13], csvTransform(record[14], truncate))
		values[14] = append(values[14], csvTransform(record[15], truncate))
	}

	return values, times
}

// Counts the lines in a gzip file.  Was initially for determining array allocation
// size but is not currently used
func gzipLineCounter(path string) (int, error) {
	t := time.Now()
	defer func() { log.Printf("data size calculated: %s", time.Since(t)) }()

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := gr.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
