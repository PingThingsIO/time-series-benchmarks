package driver

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"

	"github.com/PingThingsIO/time-series-benchmarks/pkg/iface"
	"github.com/pborman/uuid"
)

type TimescaleProvider struct {
}

var Factory iface.AbstractDatabaseFactory = &TimescaleProvider{}

type TimescaleDatabase struct {
	db  *sql.DB
	tbl string
}

var tblname string

func init() {

}

var _ iface.AbstractDatabase = &TimescaleDatabase{}

type TimescaleStream struct {
	id uuid.UUID
	db *TimescaleDatabase
}

var _ iface.Stream = &TimescaleStream{}

func (prov *TimescaleProvider) Initialize(cfg map[string]interface{}) (iface.AbstractDatabase, error) {
	TScfg := cfg["Endpoints"].(map[interface{}]interface{})["Timescale"].(map[interface{}]interface{})
	endpoint := TScfg["Endpoint"].(string)
	password := TScfg["Password"].(string)
	username := TScfg["Username"].(string)
	dbname := TScfg["Database"].(string)

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", username, password, endpoint, dbname))
	if err != nil {
		fmt.Printf("open error: %v\n", err)
		return nil, err
	}
	db.SetMaxOpenConns(10)

	//Create a table
	if tblname == "" {
		tblname = fmt.Sprintf("tbl%x", []byte(uuid.NewRandom())[:6])

		createqry := fmt.Sprintf(`CREATE TABLE "%s" (
		"time" int8 NOT NULL,
		"id" UUID NOT NULL,
		"value" DOUBLE PRECISION NOT NULL
	)`, tblname)
		createindex := fmt.Sprintf(`CREATE INDEX ON %s ("id", "time" DESC)`, tblname)
		timescaleqry := fmt.Sprintf(`SELECT create_hypertable('%s', 'time', chunk_time_interval => %d)`, tblname, 24*60*60*1000000000)

		_, err = db.Exec(createqry)
		if err != nil {
			return nil, fmt.Errorf("create error: %v", err)
		}
		_, err = db.Exec(createindex)
		if err != nil {
			return nil, fmt.Errorf("index error: %v", err)
		}
		_, err = db.Exec(timescaleqry)
		if err != nil {
			return nil, fmt.Errorf("timescale error: %v", err)
		}
	}
	return &TimescaleDatabase{
		db:  db,
		tbl: tblname,
	}, nil
}

//Obtain stream will return a handle to an existing stream
func (db *TimescaleDatabase) ObtainStream(id uuid.UUID) (iface.Stream, error) {
	qry := fmt.Sprintf(`SELECT 1
			FROM %s WHERE "id" = $1 LIMIT 1`, db.tbl)
	res, err := db.db.Query(qry, id)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		return &TimescaleStream{
			id: id,
			db: db,
		}, nil
	} else {
		return nil, fmt.Errorf("stream does not exist")
	}
}

func (db *TimescaleDatabase) CreateStream(id uuid.UUID) (iface.Stream, error) {
	qry := fmt.Sprintf(`SELECT 1
			FROM %s WHERE "id" = $1 LIMIT 1`, db.tbl)
	res, err := db.db.Query(qry, id)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		return nil, fmt.Errorf("stream already exists")

	} else {
		return &TimescaleStream{
			id: id,
			db: db,
		}, nil
	}
}

func (st *TimescaleStream) QueryValues(start, end int64) (chan []iface.Point, chan error) {
	qry := fmt.Sprintf(`SELECT "time", "value" FROM %s WHERE "id" = $1 AND "time" >= $2 AND "time" < $3 ORDER BY "time" ASC`, st.db.tbl)
	rvv := make(chan []iface.Point, 10)
	rve := make(chan error, 2)
	go func() {
		res, err := st.db.db.Query(qry, st.id, start, end)

		if err != nil {
			rve <- err
			close(rvv)
			return
		}
		defer res.Close()
		const batchsize = 100
		buf := make([]iface.Point, 0, batchsize)
		for res.Next() {
			var t int64
			var v float64
			err := res.Scan(&t, &v)
			if err != nil {
				panic(err)
			}
			buf = append(buf, iface.Point{Time: t, Value: v})
			if len(buf) >= batchsize {
				rvv <- buf
				buf = make([]iface.Point, 0, batchsize)
			}
		}
		if len(buf) > 0 {
			rvv <- buf
		}
		close(rvv)
		close(rve)
	}()
	return rvv, rve
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
func (st *TimescaleStream) QueryAggregates(start, end int64, rollup uint64) (chan []iface.Aggregate, chan error) {
	qry := fmt.Sprintf(`
		SELECT
			time_bucket_gapfill($1, time - $2, 0, $3 - $2) AS wnd,
			min(value) as mn,
			avg(value) as av,
			max(value) as mx,
			count(value) as cnt
	FROM %s
	WHERE time >= $2 AND time < $3 AND id = $4
	GROUP BY wnd
	ORDER BY wnd ASC;
		`, st.db.tbl)
	rvv := make(chan []iface.Aggregate, 10)
	rve := make(chan error, 2)
	go func() {
		res, err := st.db.db.Query(qry, rollup, start, end, st.id)

		if err != nil {
			rve <- err
			close(rvv)
			return
		}
		defer res.Close()
		const batchsize = 100
		buf := make([]iface.Aggregate, 0, batchsize)
		for res.Next() {
			agg := iface.Aggregate{}
			var bucket int64
			err := res.Scan(&bucket, &agg.Min, &agg.Average, &agg.Max, &agg.Count)
			if err != nil {
				panic(err)
			}
			agg.Time = bucket + start
			_ = bucket
			buf = append(buf, agg)
			if len(buf) >= batchsize {
				rvv <- buf
				buf = make([]iface.Aggregate, 0, batchsize)
			}
		}
		if len(buf) > 0 {
			rvv <- buf
		}
		close(rvv)
		close(rve)
	}()
	return rvv, rve
}

//Data manipulation functions
///////////////////////////////

//Insert will place the given points into the database. The benchmark
//will not insert points with a timestamp that has been previously inserted
func (st *TimescaleStream) Insert(points []iface.Point) error {
	///INSERT INTO conditions
	// VALUES
	//   (NOW(), 'office', 70.0, 50.0),
	//   (NOW(), 'basement', 66.5, 60.0),
	//   (NOW(), 'garage', 77.0, 65.2);
	//txn, err := st.db.db.Begin()

	qry := strings.Builder{}
	qry.WriteString("INSERT INTO ")
	qry.WriteString(st.db.tbl)
	qry.WriteString(` ("id","time","value") VALUES `)
	first := true
	argnum := 1
	args := []interface{}{}
	for _, p := range points {
		if !first {
			qry.WriteString(",")
		}
		first = false
		qry.WriteString(fmt.Sprintf(`($%d, $%d, $%d)`, argnum, argnum+1, argnum+2))
		args = append(args, st.id, p.Time, p.Value)
		argnum += 3
	}
	//fmt.Printf("quey is %s\n", qry.String())
	_, err := st.db.db.Exec(qry.String(), args...)
	if err != nil {
		return err
	}
	return nil
}

//Delete will remove all points in the database between the given timestamps
//with start being inclusive and end being exclusive
func (st *TimescaleStream) Delete(start, end int64) error {
	panic("ni")
}
