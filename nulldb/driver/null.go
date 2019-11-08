package driver

import (
	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/pborman/uuid"
)

//NullProvider implements AbstractDatabaseFactory
type NullProvider struct {
}

//Factory singleton is used by the benchmarks
var Factory iface.AbstractDatabaseFactory = &NullProvider{}

//NullDatabase implements AbstractDatabase
type NullDatabase struct {
}

var _ iface.AbstractDatabase = &NullDatabase{}

//NullStream implements Stream
type NullStream struct {
}

var _ iface.Stream = &NullStream{}

// Initialize returns a NullDatabase
func (prov *NullProvider) Initialize(cfg map[string]interface{}) (iface.AbstractDatabase, error) {

	return &NullDatabase{}, nil
}

//ObtainStream will return a handle to an existing, empty stream
func (db *NullDatabase) ObtainStream(id uuid.UUID) (iface.Stream, error) {
	return &NullStream{}, nil
}

//CreateStream will return a handle to a new stream
func (db *NullDatabase) CreateStream(id uuid.UUID) (iface.Stream, error) {
	return &NullStream{}, nil
}

//QueryValues returns a closed channel
func (st *NullStream) QueryValues(start, end int64) (chan []iface.Point, chan error) {
	rvv := make(chan []iface.Point, 10)
	rve := make(chan error, 2)
	close(rvv)
	close(rve)
	return rvv, rve
}

//QueryAggregates returns a closed channel
func (st *NullStream) QueryAggregates(start, end int64, rollup uint64) (chan []iface.Aggregate, chan error) {
	rvv := make(chan []iface.Aggregate, 10)
	rve := make(chan error, 2)
	close(rvv)
	close(rve)
	return rvv, rve
}

//Insert will drop points as received
func (st *NullStream) Insert(points []iface.Point) error {
	for _ = range points {
	}
	return nil
}

//Delete does literally nothing
func (st *NullStream) Delete(start, end int64) error {
	return nil
}
