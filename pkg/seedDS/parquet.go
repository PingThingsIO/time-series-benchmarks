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

func extract(path string, truncate bool) (fvalues [15][]float64, times []int64) {
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

	for true {

		data := make([]PMUDevice, 50)
		if err = pr.Read(&data); err != nil {
			panic(err)
		}
		if len(data) == 0 {
			log.Printf("exhausted seed file: total points: %d", len(fvalues[0]))
			break
		}

		// TODO truncate values

		for _, d := range data {
			fvalues[0] = append(fvalues[0], *d.VPHMA)
			fvalues[1] = append(fvalues[1], *d.VPHMB)
			fvalues[2] = append(fvalues[2], *d.VPHMC)
			fvalues[3] = append(fvalues[3], *d.VPHAA)
			fvalues[4] = append(fvalues[4], *d.VPHAB)
			fvalues[5] = append(fvalues[5], *d.VPHAC)
			fvalues[6] = append(fvalues[6], *d.IPHMA)
			fvalues[7] = append(fvalues[7], *d.IPHMB)
			fvalues[8] = append(fvalues[8], *d.IPHMC)
			fvalues[9] = append(fvalues[9], *d.IPHAA)
			fvalues[10] = append(fvalues[10], *d.IPHAB)
			fvalues[11] = append(fvalues[11], *d.IPHAC)
			fvalues[12] = append(fvalues[12], *d.FREQ)
			fvalues[13] = append(fvalues[13], *d.DFDT)
			fvalues[14] = append(fvalues[14], float64(*d.DIGI))
			times = append(times, *d.Timestamp)
		}
	}
	log.Println("exiting extract")
	return fvalues, times
}
