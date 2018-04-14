// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package snapshot

import (
	"database/sql"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var cfg *config.AppConfig

func execSQL(conn *sql.DB, t *testing.T, query string, param ...interface{}) {
	test.CheckFail(util.ExecSQL(conn, query, param...), t)
}

func resetState(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	a := db.GetInfo(&db.Loc{Service: "snap_test_svc1", Name: "snap_test_db1", Cluster: "snap_test_cluster1"},
		db.Slave)
	test.Assert(t, a != nil, "Error")

	a.Db = ""
	conn, err := db.Open(a)
	test.CheckFail(err, t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	execSQL(conn, t, "DROP DATABASE IF EXISTS "+types.MyDbName)

	if !state.Init(cfg) {
		log.Fatalf("State init failed")
	}
}

func createDB(t *testing.T) *sql.DB {
	a := db.GetInfo(&db.Loc{Service: "snap_test_svc1", Name: "snap_test_db1", Cluster: "snap_test_cluster1"},
		db.Slave)
	test.Assert(t, a != nil, "Error")

	a.Db = ""
	conn, err := db.Open(a)
	test.CheckFail(err, t)

	execSQL(conn, t, "drop database if exists snap_test_db1")
	execSQL(conn, t, "create database snap_test_db1")
	execSQL(conn, t, "create table snap_test_db1.snap_test_t1 ( f1 int not null primary key, f2 varchar(32), f3 double)")

	state.DeregisterTable("snap_test_svc1", "snap_test_db1", "snap_test_t1", "mysql", "", 0)

	if !state.RegisterTable(&db.Loc{Service: "snap_test_svc1", Name: "snap_test_db1"}, "snap_test_t1", "mysql", "", 0, "") {
		t.FailNow()
	}

	err = conn.Close()
	test.CheckFail(err, t)

	a.Db = "snap_test_db1"
	conn, err = db.Open(a)
	test.CheckFail(err, t)

	return conn
}

func TestGetGtidError(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	tx, err := conn.Begin()
	test.CheckFail(err, t)

	err = tx.Rollback()
	test.CheckFail(err, t)

	_, err = s.StartFromTx("snap_test_svc1", "snap_test_db1", "non_existent_table", nil, tx)

	test.Assert(t, err != nil, "non existent table should fail")
}

func TestNonExistentTable(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "non_existent_table", nil)

	test.Assert(t, err != nil, "non existent table should fail")
}

func TestNonExistentDb(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "non_existent_db", "snap_test_t1", nil)

	test.Assert(t, err != nil, "non existent db should fail")
}

func TestEmptyTable(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	enc, err := encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1")

	test.CheckFail(err, t)

	_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "snap_test_t1", enc)
	test.CheckFail(err, t)
	defer s.End()

	for s.HasNext() {
		_, _, err := s.GetNext()
		if err != nil {
			test.CheckFail(err, t)
		}
	}
}

func TestBasic(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	for i := 0; i < 100; i++ {
		execSQL(conn, t, "insert into snap_test_t1 values(?,?,?)", i, strconv.Itoa(i), float64(i)/3)
	}

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	var enc encoder.Encoder
	enc, err = encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1")

	test.CheckFail(err, t)

	_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "snap_test_t1", enc)
	test.CheckFail(err, t)
	defer s.End()

	var i int64
	for s.HasNext() {
		key, data, err := s.GetNext()
		if err != nil {
			test.CheckFail(err, t)
		} else {
			cf, err := encoder.Internal.DecodeEvent(data)
			cf.Timestamp = 0
			test.CheckFail(err, t)
			refcf := types.CommonFormatEvent{Type: "insert", Key: []interface{}{float64(i)}, SeqNo: 0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: float64(i)}, {Name: "f2", Value: strconv.FormatInt(i, 10)}, {Name: "f3", Value: float64(i) / 3}}}
			// refcf := types.CommonFormatEvent{Type: "insert", Key: []interface{}{i}, SeqNo: 0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: i}, {Name: "f2", Value: strconv.FormatInt(i, 10)}, {Name: "f3", Value: float64(i) / 3}}}

			ChangeCfFields(cf)
			if !reflect.DeepEqual(&refcf, cf) {
				log.Errorf("Received: %+v %+v", cf, cf.Fields)
				log.Errorf("Reference: %+v %+v", &refcf, refcf.Fields)
				t.FailNow()
			}
			if key != encoder.GetCommonFormatKey(cf) {
				log.Errorf("Received: %+v", key)
				log.Errorf("Reference: %+v", encoder.GetCommonFormatKey(cf))
				t.FailNow()
			}
			i++
		}
	}

}

func TestMoreFieldTypes(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	for i := 0; i < 1; i++ {
		execSQL(conn, t, "insert into snap_test_t1(f1) values(?)", i)
	}

	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f4 text")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f5 timestamp")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f6 date")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f7 time")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f8 year")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f9 bigint")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f10 binary(128)")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f11 int")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f12 float")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f13 double")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f14 decimal")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f15 numeric")

	//msgpack doesn't preserve int size, so all int32 became int64
	expectedType := []string{
		"int32",
		"string",
		"float64",
		"string",
		"string",
		"string",
		"string",
		"int32",
		"int64",
		"[]uint8",
		"int32",
		"float32",
		"float64",
		"float64",
		"float64",
	}

	execSQL(conn, t, "insert into snap_test_t1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 1567, strconv.Itoa(1567), float64(1567)/3, "testtextfield", time.Now(), time.Now(), time.Now(), 2017, 98878, []byte("testbinaryfield"), 827738, 111.23, 222.34, 333.45, 444.56)

	if !state.DeregisterTable("snap_test_svc1", "snap_test_db1", "snap_test_t1", "mysql", "", 0) {
		t.FailNow()
	}

	if !state.RegisterTable(&db.Loc{Service: "snap_test_svc1", Name: "snap_test_db1"}, "snap_test_t1", "mysql", "", 0, "") {
		t.FailNow()
	}

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	var enc encoder.Encoder
	enc, err = encoder.Create("msgpack", "snap_test_svc1", "snap_test_db1", "snap_test_t1")
	test.CheckFail(err, t)

	_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "snap_test_t1", enc)
	test.CheckFail(err, t)

	for s.HasNext() {
		_, msg, err1 := s.GetNext()
		test.CheckFail(err1, t)
		d, err2 := enc.DecodeEvent(msg)
		test.CheckFail(err2, t)
		sch := enc.Schema()
		for i, v := range *d.Fields {
			if v.Value != nil && reflect.TypeOf(v.Value).String() != expectedType[i] {
				log.Errorf("%v: got: %v expected: %v %v %v", v.Name, reflect.TypeOf(v.Value).String(), expectedType[i], sch.Columns[i].DataType, sch.Columns[i].Type)
				s.End()
				t.FailNow()
			}
		}
	}

	s.End()

	execSQL(conn, t, "ALTER TABLE snap_test_t1 drop f15")

	_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "snap_test_t1", enc)
	test.CheckFail(err, t)

	test.Assert(t, s.HasNext(), "there should be at leas one record")

	_, _, err = s.GetNext()
	test.Assert(t, err != nil, "encoder has old schema")

	s.End()
}

func ChangeCfFields(cf *types.CommonFormatEvent) {
	switch (cf.Key[0]).(type) {
	case int64:
		cf.Key[0] = float64(cf.Key[0].(int64))
	case uint64:
		cf.Key[0] = float64(cf.Key[0].(uint64))
	}

	if cf.Fields != nil {
		for f := range *cf.Fields {
			switch ((*cf.Fields)[f].Value).(type) {
			case int64:
				val := ((*cf.Fields)[f].Value).(int64)
				newFloat := float64(val)
				(*cf.Fields)[f].Value = newFloat
			case uint64:
				val := ((*cf.Fields)[f].Value).(uint64)
				newFloat := float64(val)
				(*cf.Fields)[f].Value = newFloat
			}
		}
	}
}

func TestSnapshotConsistency(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	for i := 0; i < 100; i++ {
		execSQL(conn, t, "insert into snap_test_t1 values(?,?,?)", i, strconv.Itoa(i), float64(i)/3)
	}

	/* Make some gaps */
	execSQL(conn, t, "delete from snap_test_t1 where f1 > 70 && f1 < 80")
	execSQL(conn, t, "delete from snap_test_t1 where f1 > 30 && f1 < 40")

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	var enc encoder.Encoder
	enc, err = encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1")

	test.CheckFail(err, t)
	_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "snap_test_t1", enc)
	test.CheckFail(err, t)

	var i int64
	for s.HasNext() {
		key, data, err := s.GetNext()
		if err != nil {
			test.CheckFail(err, t)
		} else {
			cf, err := encoder.Internal.DecodeEvent(data)
			// cf, err := encoder.CommonFormatDecode(data)
			test.CheckFail(err, t)
			cf.Timestamp = 0
			refcf := types.CommonFormatEvent{Type: "insert", Key: []interface{}{float64(i)}, SeqNo: 0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: float64(i)}, {Name: "f2", Value: strconv.FormatInt(i, 10)}, {Name: "f3", Value: float64(i) / 3}}}
			// refcf := types.CommonFormatEvent{Type: "insert", Key: []interface{}{i}, SeqNo: 0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: i}, {Name: "f2", Value: strconv.FormatInt(i, 10)}, {Name: "f3", Value: float64(i) / 3}}}

			ChangeCfFields(cf)

			if !reflect.DeepEqual(&refcf, cf) {
				log.Errorf("Received: %+v %+v", cf, cf.Fields)
				log.Errorf("Reference: %+v %+v", &refcf, refcf.Fields)
				t.FailNow()
			}
			if key != encoder.GetCommonFormatKey(cf) {
				log.Errorf("Received: %+v", key)
				log.Errorf("Reference: %+v", encoder.GetCommonFormatKey(cf))
				t.FailNow()
			}
			if i == 30 {
				i = 40
			} else if i == 70 {
				/* Insert/delete something in the gap we already left in the past */
				execSQL(conn, t, "insert into snap_test_t1 values(?,?,?)", 35, "35", 35.0/3)
				execSQL(conn, t, "delete from snap_test_t1 where f1 > 10 && f1 < 20")
				/* Insert/delete data in the future gap, we shouldn't see it */
				execSQL(conn, t, "insert into snap_test_t1 values(?,?,?)", 75, "75", 75.0/3)
				execSQL(conn, t, "delete from snap_test_t1 where f1 > 85 && f1 < 90")
				execSQL(conn, t, "update snap_test_t1 set f2='bbbb' where f1 > 95 && f1 < 97")
				i = 80
			} else {
				i++
			}
		}
	}

	s.End()
}

func TestSnapshotNilConnection(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	s, err := InitReader("mysql")
	test.CheckFail(err, t)

	var enc encoder.Encoder
	enc, err = encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1")

	test.CheckFail(err, t)

	_, err = s.Start("please_return_nil_db_addr", "snap_test_svc1", "snap_test_db1", "snap_test_t1", enc)
	test.Assert(t, err != nil, "get connection should return nil")
}

func prepareBench(t *testing.T) Reader {
	resetState(t)

	input := "mysql"
	conn := createDBLow(t, input)

	defer func() { test.CheckFail(conn.Close(), t) }()

	for i := 0; i < 100; i++ {
		execSQL(conn, t, "insert into snap_test_t1(f1,f2,f3) values(?,?,?)", i, strconv.Itoa(i), float64(i)/3)
	}

	enc, err := encoder.Create("msgpack", "snap_test_svc1", "snap_test_db1", "snap_test_t1", input, "kafka", 0)
	test.CheckFail(err, t)

	s, err := InitReader(input, enc, metrics.NewSnapshotMetrics(nil))
	test.CheckFail(err, t)

	return s
}

func runBenchmarks() {
	t := new(testing.T)
	s := prepareBench(t)
	_, err := s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "snap_test_t1")
	test.CheckFail(err, t)
	defer s.End()

	benchmarks := []testing.InternalBenchmark{}
	bm := testing.InternalBenchmark{
		Name: "[name=mysql-msgpack]",
		F: func(b *testing.B) {
			for i := b.N; i > 0; {
				for ; i > 0 && s.HasNext(); i-- {
					_, _, err := s.GetNext()
					test.CheckFail(err, t)
				}
				if i > 0 {
					b.StopTimer()
					s.End()
					_, err = s.Start("snap_test_cluster1", "snap_test_svc1", "snap_test_db1", "snap_test_t1")
					test.CheckFail(err, t)
					b.StartTimer()
				}
			}
		},
	}
	benchmarks = append(benchmarks, bm)

	log.Configure(cfg.LogType, "error", config.EnvProduction())

	anything := func(pat, str string) (bool, error) { return true, nil }
	testing.Main(anything, nil, benchmarks, nil)
}
