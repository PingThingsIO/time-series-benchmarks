package seedds

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type PMUDevice struct {
	VPHMA     *float64 `parquet:"name=0f44a8e0-2dd2-5dcb-9e6d-8faf94234d5b, type=DOUBLE"`
	VPHMB     *float64 `parquet:"name=cde1bdeb-f820-510e-86c2-2ba6713f0dd9, type=DOUBLE"`
	VPHMC     *float64 `parquet:"name=e771ed08-0247-545d-b6c2-d4f5c7c0bb5b, type=DOUBLE"`
	VPHAA     *float64 `parquet:"name=3b22be0f-9aff-5617-9d32-841d98b8cf20, type=DOUBLE"`
	VPHAB     *float64 `parquet:"name=8b9c8f95-cdcf-5dca-824e-9b7b2a920712, type=DOUBLE"`
	VPHAC     *float64 `parquet:"name=7630d14b-fb42-52a8-a6c1-8a360318b061, type=DOUBLE"`
	IPHMA     *float64 `parquet:"name=c367c0a9-8df0-586c-ab83-94901aa72b88, type=DOUBLE"`
	IPHMB     *float64 `parquet:"name=60dae85d-971c-5884-a32d-cbb5c5ee5ba0, type=DOUBLE"`
	IPHMC     *float64 `parquet:"name=3a3a8149-0899-5923-a7c2-048c0915ee77, type=DOUBLE"`
	IPHAA     *float64 `parquet:"name=27e94f18-c1be-56d6-9e44-33a29aae22ac, type=DOUBLE"`
	IPHAB     *float64 `parquet:"name=242bbf08-29ca-58bb-a030-05c5fe84a22e, type=DOUBLE"`
	IPHAC     *float64 `parquet:"name=04dde864-f851-5ab2-b600-86adb241ef47, type=DOUBLE"`
	FREQ      *float64 `parquet:"name=54d72e47-44e3-59b3-84d7-4ff7c301318d, type=DOUBLE"`
	DFDT      *float64 `parquet:"name=78de2255-6465-5f12-a917-0c2efa168269, type=DOUBLE"`
	DIGI      *int64   `parquet:"name=f690c3b3-a460-4306-9351-884af1f86bfa, type=INT64"`
	Timestamp *int64   `parquet:"name=timestamp, type=INT64, repetitiontype=OPTIONAL"`
}

func transform(val float64, truncate bool) float64 {
	if truncate {
		return float64(float32(val))
	}
	return val
}

func extract(path string, truncate bool) ([][]float64, []int64) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		panic(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(PMUDevice), 4)
	if err != nil {
		panic(err)
	}
	defer pr.ReadStop()

	// initialize arrays
	l := pr.GetNumRows()
	times := make([]int64, 0, l)
	values := make([][]float64, 15)
	for i := 0; i < len(values); i++ {
		values[i] = make([]float64, 0, l)
	}

	// number of records we should read at once from parquet
	parquetBatchSize := 5000

	for {

		data := make([]PMUDevice, parquetBatchSize)
		if err = pr.Read(&data); err != nil {
			panic(err)
		}
		if len(data) == 0 {
			log.Printf("exhausted seed file: total points: %d (cap: %d)", len(values[0]), cap(values[0]))
			break
		}

		for _, d := range data {
			values[0] = append(values[0], transform(*d.VPHMA, truncate))
			values[1] = append(values[1], transform(*d.VPHMB, truncate))
			values[2] = append(values[2], transform(*d.VPHMC, truncate))
			values[3] = append(values[3], transform(*d.VPHAA, truncate))
			values[4] = append(values[4], transform(*d.VPHAB, truncate))
			values[5] = append(values[5], transform(*d.VPHAC, truncate))
			values[6] = append(values[6], transform(*d.IPHMA, truncate))
			values[7] = append(values[7], transform(*d.IPHMB, truncate))
			values[8] = append(values[8], transform(*d.IPHMC, truncate))
			values[9] = append(values[9], transform(*d.IPHAA, truncate))
			values[10] = append(values[10], transform(*d.IPHAB, truncate))
			values[11] = append(values[11], transform(*d.IPHAC, truncate))
			values[12] = append(values[12], transform(*d.FREQ, truncate))
			values[13] = append(values[13], transform(*d.DFDT, truncate))
			values[14] = append(values[14], float64(*d.DIGI))
			times = append(times, *d.Timestamp)
		}
	}
	return values, times
}
