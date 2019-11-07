package driver

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/davecgh/go-spew/spew"
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/pborman/uuid"
)

type InfluxProvider struct {
}

var Factory iface.AbstractDatabaseFactory = &InfluxProvider{}

type InfluxDatabase struct {
	client client.Client
}

var dbname string

var _ iface.AbstractDatabase = &InfluxDatabase{}

type InfluxStream struct {
	id uuid.UUID
	db *InfluxDatabase
}

var _ iface.Stream = &InfluxStream{}

func (prov *InfluxProvider) Initialize(cfg map[string]interface{}) (iface.AbstractDatabase, error) {
	Influxcfg := cfg["Endpoints"].(map[interface{}]interface{})["Influx"].(map[interface{}]interface{})
	endpoint := Influxcfg["Endpoint"].(string)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: endpoint,
	})
	if err != nil {
		return nil, err
	}

	//Create a database
	if dbname == "" {
		dbname = fmt.Sprintf("db%x", []byte(uuid.NewRandom())[:6])
		r, err := c.Query(client.NewQuery(fmt.Sprintf("CREATE DATABASE %s", dbname), "", ""))
		if err != nil {
			return nil, fmt.Errorf("failed to create db: %v", err)
		}
		if r.Error() != nil {
			return nil, fmt.Errorf("failed to create db(2): %v", r.Error())
		}
	}

	return &InfluxDatabase{
		client: c,
	}, nil
}

//Obtain stream will return a handle to an existing stream
func (db *InfluxDatabase) ObtainStream(id uuid.UUID) (iface.Stream, error) {

	qry := fmt.Sprintf(`SELECT FIRST(value) FROM "%s"`, id.String())
	fmt.Printf("qry is %s\n", qry)
	res, err := db.client.Query(client.NewQuery(qry, dbname, ""))
	if err != nil {
		return nil, fmt.Errorf("couldn't execute query: %v", err)
	}
	spew.Dump(res)
	if len(res.Results[0].Series) > 0 {
		return &InfluxStream{
			id: id,
			db: db,
		}, nil
	} else {
		return nil, fmt.Errorf("stream does not exist")
	}
}

func (db *InfluxDatabase) CreateStream(id uuid.UUID) (iface.Stream, error) {
	qry := fmt.Sprintf(`SELECT FIRST(value) FROM "%s"`, id.String())
	res, err := db.client.Query(client.NewQuery(qry, dbname, ""))
	if err != nil {
		return nil, fmt.Errorf("couldn't execute query: %v", err)
	}
	if len(res.Results[0].Series) > 0 {
		return nil, fmt.Errorf("stream exists")
	} else {
		return &InfluxStream{
			id: id,
			db: db,
		}, nil
	}
}

func (st *InfluxStream) QueryValues(start, end int64) (chan []iface.Point, chan error) {
	qry := fmt.Sprintf(`SELECT "value" FROM "%s" WHERE time >= %d AND time < %d`, st.id.String(), start, end)
	q := client.NewQuery(qry, dbname, "n")
	rvv := make(chan []iface.Point, 10)
	rve := make(chan error, 2)
	go func() {
		res, err := st.db.client.QueryAsChunk(q)
		if err != nil {
			rve <- err
			close(rvv)
			return
		}
		defer res.Close()
		for {
			r, err := res.NextResponse()
			if err != nil {
				if err == io.EOF {
					close(rvv)
					close(rve)
					return
				}
				rve <- err
				close(rvv)

				return
			}
			if r.Error() != nil {
				rve <- r.Error()
				close(rvv)
				return
			}
			rv := r.Results[0]
			ser := rv.Series[0]
			for _, serRow := range ser.Values {
				t, err := serRow[0].(json.Number).Int64()
				if err != nil {
					panic(err)
				}
				v, err := serRow[1].(json.Number).Float64()
				if err != nil {
					panic(err)
				}
				rvv <- []iface.Point{{Time: t, Value: v}}
			}
		}
	}()
	return rvv, rve
	//
	// qry := fmt.Sprintf(`SELECT "time", "value" FROM %s WHERE "id" = $1 AND "time" >= $2 AND "time" < $3 ORDER BY "time" ASC`, st.db.tbl)
	// rvv := make(chan []iface.Point, 10)
	// rve := make(chan error, 2)
	// go func() {
	// 	res, err := st.db.db.Query(qry, st.id, start, end)
	//
	// 	if err != nil {
	// 		rve <- err
	// 		close(rvv)
	// 		return
	// 	}
	// 	defer res.Close()
	// 	const batchsize = 100
	// 	buf := make([]iface.Point, 0, batchsize)
	// 	for res.Next() {
	// 		var t int64
	// 		var v float64
	// 		err := res.Scan(&t, &v)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		buf = append(buf, iface.Point{Time: t, Value: v})
	// 		if len(buf) >= batchsize {
	// 			rvv <- buf
	// 			buf = make([]iface.Point, 0, batchsize)
	// 		}
	// 	}
	// 	if len(buf) > 0 {
	// 		rvv <- buf
	// 	}
	// 	close(rvv)
	// 	close(rve)
	// }()
	// return rvv, rve
}

/*
	SELECT time_bucket_gapfill(1000000000, time) AS wnd, time
	FROM tbl703cd5f00923 WHERE time > 1262307600000000000 AND time < 1262308227491641567;

	SELECT time_bucket_gapfill(1000000000, time - 500000000) + 500000000 AS wnd, time FROM tbl703cd5f00923 WHERE time > 1262307600000000000 AND time < 1262308227491641567 AND id = '7ec2ab26-ea3d-4803-9f43-eb3c0fdd371b' ORDER by time ASC;

	SELECT time_bucket_gapfill(1000000000, time - 1262307600000000000, 0, 30000000000) AS wnd, avg(value), count(value)
	FROM tbl09bbce53e8d8
	WHERE time > 1262307600000000000 AND time < 1262308227491641567
	AND id = 'c63bb21e-f1f4-41d2-b148-8216b9d1eb05' GROUP BY wnd ORDER by wnd ASC;


*/
func (st *InfluxStream) QueryAggregates(start, end int64, rollup uint64) (chan []iface.Aggregate, chan error) {

	//   epoch = datetime.utcfromtimestamp(0)              # epoch 0
	// start_time = datetime.datetime(<some value>)      # lower bound of time range
	// group_by = <value>                                # number of seconds to group data by
	//
	// offset = (start_time - epoch).total_seconds() % group_by

	offset := start % int64(rollup)
	qry := fmt.Sprintf(`
    SELECT MIN(value), MEAN(value), MAX(value), COUNT(value) FROM "%s" WHERE time >= %d AND time < %d GROUP BY time(%dns, %dns)`,
		st.id.String(), start, end, rollup, offset)

	q := client.NewQuery(qry, dbname, "n")
	rvv := make(chan []iface.Aggregate, 10)
	rve := make(chan error, 2)
	go func() {
		res, err := st.db.client.QueryAsChunk(q)
		if err != nil {
			rve <- err
			close(rvv)
			return
		}
		defer res.Close()
		for {
			r, err := res.NextResponse()
			if err != nil {
				if err == io.EOF {
					close(rvv)
					close(rve)
					return
				}
				rve <- err
				close(rvv)
				return
			}
			if r.Error() != nil {
				rve <- r.Error()
				close(rvv)
				return
			}
			rv := r.Results[0]
			ser := rv.Series[0]
			for _, serRow := range ser.Values {
				t, err := serRow[0].(json.Number).Int64()
				if err != nil {
					panic(err)
				}
				min, err := serRow[1].(json.Number).Float64()
				if err != nil {
					panic(err)
				}
				mean, err := serRow[2].(json.Number).Float64()
				if err != nil {
					panic(err)
				}
				max, err := serRow[3].(json.Number).Float64()
				if err != nil {
					panic(err)
				}
				count, err := serRow[4].(json.Number).Int64()
				if err != nil {
					panic(err)
				}
				rvv <- []iface.Aggregate{{Time: t, Min: min, Average: mean, Max: max, Count: uint64(count)}}
			}
		}
	}()
	return rvv, rve

	// qry := fmt.Sprintf(`
	// 	SELECT
	// 		time_bucket_gapfill($1, time - $2, 0, $3 - $2) AS wnd,
	// 		min(value) as mn,
	// 		avg(value) as av,
	// 		max(value) as mx,
	// 		count(value) as cnt
	// FROM %s
	// WHERE time >= $2 AND time < $3 AND id = $4
	// GROUP BY wnd
	// ORDER BY wnd ASC;
	// 	`, st.db.tbl)
	// rvv := make(chan []iface.Aggregate, 10)
	// rve := make(chan error, 2)
	// go func() {
	// 	res, err := st.db.db.Query(qry, rollup, start, end, st.id)
	//
	// 	if err != nil {
	// 		rve <- err
	// 		close(rvv)
	// 		return
	// 	}
	// 	defer res.Close()
	// 	const batchsize = 100
	// 	buf := make([]iface.Aggregate, 0, batchsize)
	// 	for res.Next() {
	// 		agg := iface.Aggregate{}
	// 		var bucket int64
	// 		err := res.Scan(&bucket, &agg.Min, &agg.Average, &agg.Max, &agg.Count)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		agg.Time = bucket + start
	// 		_ = bucket
	// 		buf = append(buf, agg)
	// 		if len(buf) >= batchsize {
	// 			rvv <- buf
	// 			buf = make([]iface.Aggregate, 0, batchsize)
	// 		}
	// 	}
	// 	if len(buf) > 0 {
	// 		rvv <- buf
	// 	}
	// 	close(rvv)
	// 	close(rve)
	// }()
	// return rvv, rve
}

//Data manipulation functions
///////////////////////////////

//Insert will place the given points into the database. The benchmark
//will not insert points with a timestamp that has been previously inserted
func (st *InfluxStream) Insert(points []iface.Point) error {
	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  dbname,
		Precision: "ns",
	})
	if err != nil {
		panic(err)
	}

	for _, p := range points {
		fields := map[string]interface{}{
			"value": p.Value,
		}
		pt, err := client.NewPoint(st.id.String(), nil, fields, time.Unix(0, p.Time))
		if err != nil {
			return err
		}
		bp.AddPoint(pt)
	}
	return st.db.client.Write(bp)
}

//Delete will remove all points in the database between the given timestamps
//with start being inclusive and end being exclusive
func (st *InfluxStream) Delete(start, end int64) error {
	qry := fmt.Sprintf(`DELETE FROM FROM "%s" WHERE time >= %d AND time < %d`, st.id.String(), start, end)
	q := client.NewQuery(qry, dbname, "n")
	res, err := st.db.client.Query(q)
	if err != nil {
		return err
	}
	if res.Error() != nil {
		return res.Error()
	}
	return nil
}
