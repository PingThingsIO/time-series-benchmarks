package iface

import "github.com/pborman/uuid"

//AbstractDatabaseFactory is the type that an adapter must adhere to in order
//for the benchmark to be able to use it
type AbstractDatabaseFactory interface {
	//Initialize will connect to the database. It will be passed
	//the config.yaml which it can draw from if required
	Initialize(cfg map[string]interface{}) (AbstractDatabase, error)
}

//AbstractDatabase is the interface that a connection (or connection pool) to
//the database must implement
type AbstractDatabase interface {
	//Obtain stream will return a handle to an existing stream
	ObtainStream(id uuid.UUID) (Stream, error)

	//Create stream will return a handle to a newly created stream, erroring
	//if it does not exist
	CreateStream(id uuid.UUID) (Stream, error)
}

//Point represents a single point stored in the database
type Point struct {
	Time  int64
	Value float64
}

//Aggregate defines the types of aggregate information that is
//required for visualization, reporting and anomoly detection
type Aggregate struct {
	Time    int64
	Min     float64
	Average float64
	Max     float64
	Count   uint64
}

//Stream is a handle to an individual sequence of time series values
type Stream interface {

	//Query functions
	/////////////////////////////

	//Values returns the raw values stored in the database
	//with start being inclusive and end being exclusive
	QueryValues(start, end int64) (chan []Point, chan error)

	//QueryAggregates returns a list of rollups between the start and end, where each
	//rollup is the specified number of nanoseconds wide.
	//start is inclusive and end is exclusive
	QueryAggregates(start, end int64, rollup uint64) (chan []Aggregate, chan error)

	//Data manipulation functions
	///////////////////////////////

	//Insert will place the given points into the database. The benchmark
	//will not insert points with a timestamp that has been previously inserted
	Insert(points []Point) error

	//Delete will remove all points in the database between the given timestamps
	//with start being inclusive and end being exclusive
	Delete(start, end int64) error
}
