package driver

import (
	"context"
	"fmt"

	"github.com/BTrDB/btrdb"
	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/pborman/uuid"
)

//BTrDBProvider implements AbstractDatabaseFactory
type BTrDBProvider struct {
}

//This singleton is used by the benchmarks
var Factory iface.AbstractDatabaseFactory = &BTrDBProvider{}

//BTrDBDatabase implements AbstractDatabase
type BTrDBDatabase struct {
	cprefix       string
	db            *btrdb.BTrDB
	readBatchSize int
}

var _ iface.AbstractDatabase = &BTrDBDatabase{}

//BTrDBStream implements Stream
type BTrDBStream struct {
	s  *btrdb.Stream
	db *BTrDBDatabase
}

var _ iface.Stream = &BTrDBStream{}

//Initialize will open a connection to the database and read
//the configuration parameters that affect subsequent operations
func (prov *BTrDBProvider) Initialize(cfg map[string]interface{}) (iface.AbstractDatabase, error) {
	//Get the config for PredictiveGrid
	PGcfg := cfg["Endpoints"].(map[interface{}]interface{})["PredictiveGrid"].(map[interface{}]interface{})

	//Pull out the fields
	endpoint := PGcfg["Endpoint"].(string)
	apikey := PGcfg["APIKey"].(string)
	batchsize := PGcfg["ReadBatchSize"].(int)
	colprefix := PGcfg["CollectionPrefix"].(string)

	//Connect to the specified endpoint
	db, err := btrdb.ConnectAuth(context.Background(), apikey, endpoint)
	if err != nil {
		return nil, err
	}
	return &BTrDBDatabase{db: db, cprefix: colprefix, readBatchSize: batchsize}, nil
}

//Obtain stream will return a handle to an existing stream
func (db *BTrDBDatabase) ObtainStream(id uuid.UUID) (iface.Stream, error) {
	s := db.db.StreamFromUUID(id)
	ex, err := s.Exists(context.Background())
	if err != nil {
		return nil, err
	}
	if !ex {
		return nil, fmt.Errorf("no such stream exists")
	}
	return &BTrDBStream{s: s, db: db}, nil
}

//CreateStream will create a new stream
func (db *BTrDBDatabase) CreateStream(id uuid.UUID) (iface.Stream, error) {
	s := db.db.StreamFromUUID(id)
	ex, err := s.Exists(context.Background())
	if err != nil {
		return nil, err
	}
	if ex {
		return nil, fmt.Errorf("stream already exists")
	}
	col := fmt.Sprintf("%s/%x", db.cprefix, []byte(id)[:10])
	s, err = db.db.Create(context.Background(), id, col, btrdb.OptKV("name", "stream", "unit", "unit"), nil)
	if err != nil {
		return nil, err
	}
	return &BTrDBStream{s: s, db: db}, nil
}

//QueryValues will stream back raw values in batches of ReadBatchSize
func (st *BTrDBStream) QueryValues(start, end int64) (chan []iface.Point, chan error) {
	rvc := make(chan []iface.Point, 10)
	valchan, _, errchan := st.s.RawValues(context.Background(), start, end, btrdb.LatestVersion)
	go func() {
		buf := make([]iface.Point, 0, st.db.readBatchSize)
		for p := range valchan {
			buf = append(buf, iface.Point{Time: p.Time, Value: p.Value})
			if len(buf) == st.db.readBatchSize {
				rvc <- buf
				buf = make([]iface.Point, 0, st.db.readBatchSize)
			}
		}
		if len(buf) > 0 {
			rvc <- buf
		}
		close(rvc)
	}()
	return rvc, errchan
}

//QueryAggregates will stream back aggregate windows of size rollup ns between start and end
func (st *BTrDBStream) QueryAggregates(start, end int64, rollup uint64) (chan []iface.Aggregate, chan error) {
	rvc := make(chan []iface.Aggregate, 10)
	valchan, _, errchan := st.s.Windows(context.Background(), start, end, rollup, 0, btrdb.LatestVersion)
	go func() {
		for p := range valchan {
			rvc <- []iface.Aggregate{{Time: p.Time, Min: p.Min, Average: p.Mean, Max: p.Max, Count: p.Count}}
		}
		close(rvc)
	}()
	return rvc, errchan
}

//Insert will place the given points into the database. The benchmark
//will not insert points with a timestamp that has been previously inserted
func (st *BTrDBStream) Insert(points []iface.Point) error {
	return st.s.InsertF(context.Background(), len(points),
		func(i int) int64 {
			return points[i].Time
		},
		func(i int) float64 {
			return points[i].Value
		})
}

//Delete will remove all points in the database between the given timestamps
//with start being inclusive and end being exclusive
func (st *BTrDBStream) Delete(start, end int64) error {
	_, err := st.s.DeleteRange(context.Background(), start, end)
	return err
}
