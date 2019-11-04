package seedds

import (
	"fmt"
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

// Open "sample-pmu-seed-dataset.parquet.gzip"
func Open(path string) {
	filename := path
	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		log.Println("Can't open file")
		return
	}
	fmt.Println("file opened")

	pr, err := reader.NewParquetReader(fr, new(PMUDevice), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	fmt.Println("reader created")
	num := int(pr.GetNumRows())
	fmt.Printf("Num Rows: %d\n", num)

	for i := 0; i < 12; i++ {
		stus := make([]PMUDevice, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		fmt.Printf("%+v\n", stus[0])

		// tmp := stus[0].VPHMA
		fmt.Printf("Timestamp: %d, VPHMA: %f\n", *stus[0].Timestamp, *stus[0].VPHMA)
	}

	pr.ReadStop()
	fr.Close()
	fmt.Println("bye")
}
