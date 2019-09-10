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
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
	"gopkg.in/yaml.v2"
)

var cfg *config.AppConfig

func resetState(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	// cleanup the state tables before running the tests
	require.True(t, state.Reset())
}

func execSQL(conn *sql.DB, t *testing.T, query string, param ...interface{}) {
	test.CheckFail(util.ExecSQL(conn, query, param...), t)
}

func createDBLow(t *testing.T, input string) *sql.DB {
	a, err := db.GetConnInfo(&db.Loc{Service: "snap_test_svc1", Name: "snap_test_db1", Cluster: "snap_test_cluster1"},
		db.Slave, "mysql")
	test.CheckFail(err, t)

	a.Db = ""
	conn, err := db.Open(a)
	test.CheckFail(err, t)

	execSQL(conn, t, "drop database if exists snap_test_db1")
	execSQL(conn, t, "create database snap_test_db1")
	execSQL(conn, t, "create table snap_test_db1.snap_test_t1 ( f1 int not null primary key, f2 varchar(32), f3 double)")

	if input == "schemaless" {
		execSQL(conn, t, "alter table snap_test_db1.snap_test_t1 add row_key binary(16), add column_key varchar(191), add ref_key bigint(20), add body mediumblob")
		execSQL(conn, t, "ALTER TABLE snap_test_db1.snap_test_t1 ADD KEY uc_key (row_key, column_key, ref_key)")
	}

	if !state.RegisterTableInState(&db.Loc{Service: "snap_test_svc1", Cluster: "test_cluster1", Name: "snap_test_db1"},
		"snap_test_t1", input, "kafka", 0, "", "", 0) {

		t.FailNow()
	}

	err = conn.Close()
	test.CheckFail(err, t)

	a.Db = "snap_test_db1"
	conn, err = db.Open(a)
	test.CheckFail(err, t)

	return conn
}

func createDB(t *testing.T) *sql.DB {
	return createDBLow(t, "mysql")
}

func changeCfFields(cf *types.CommonFormatEvent) {
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

/*
FIXME: find a way to test erronous transaction
func TestGetGtidError(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	sr, err := createMySQLReader(nil, nil)
	test.CheckFail(err, t)

	s, ok := sr.(*mysqlReader)
	test.Assert(t, ok, "expected type *mysqlReader")

	tx, err := conn.Begin()
	test.CheckFail(err, t)

	err = tx.Rollback()
	test.CheckFail(err, t)

	_, err = s.startFromTx("snap_test_svc1", "snap_test_clst1", "snap_test_db1", "non_existent_table", tx)

	test.Assert(t, err != nil, "non existent table should fail")
}
*/

func TestNonExistentTable(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	_, err := Start("mysql", "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "non_existent_table", nil, nil, nil)
	test.Assert(t, err != nil, "non existent table should fail")
}

func TestNonExistentDb(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	_, err := Start("mysql", "snap_test_svc1", "snap_test_cluster1", "non_existent_db", "snap_test_t1", nil, nil, nil)
	test.Assert(t, err != nil, "non existent db should fail")
}

func TestEmptyTable(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	enc, err := encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1", types.InputMySQL, "kafka", 0)
	test.CheckFail(err, t)

	s, err := Start("mysql", "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "snap_test_t1", nil, enc, nil)
	test.CheckFail(err, t)
	defer s.End()

	for s.FetchNext() {
		_, _, _, err := s.Pop()
		if err != nil {
			test.CheckFail(err, t)
		}
	}
}

func TestFilters(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		output string
	}{
		{"no_filter", "", ""},
		{"noValues", `
tInput:
   tTable:
     column: "tKey1"
     condition: "!="`, ""},
		{"noColumn", `
tInput:
   tTable:
     values: ["tVal1"]
     condition: "!="`, ""},
		{"noCondition", `
tInput:
   tTable:
     column: "tKey1"
     values: ["tVal1"]`, ""},
		{"oneField", `
tInput:
   tTable:
     column: "tKey1"
     values: ["tVal1"]
     condition: "!="`, "WHERE `tKey1` != 'tVal1'"},
		{"multiField", `
tInput:
   tTable:
     column: "tKey1"
     values: ["tVal1", "tVal2"]
     condition: "!="`, "WHERE `tKey1` != 'tVal1' AND `tKey1` != 'tVal2'"},
		{"testCond", `
tInput:
   tTable:
     column: "tKey1"
     values: ["tVal1", "tVal2"]
     condition: "=="`, "WHERE `tKey1` == 'tVal1' AND `tKey1` == 'tVal2'"},
		{"testOP", `
tInput:
   tTable:
     column: "tKey1"
     values: ["tVal1", "tVal2"]
     operator: "OR"
     condition: "=="`, "WHERE `tKey1` == 'tVal1' OR `tKey1` == 'tVal2'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f map[string]map[string]config.RowFilter
			err := yaml.Unmarshal([]byte(tt.input), &f)
			require.NoError(t, err)
			config.Get().Filters = f
			assert.Equal(t, tt.output, FilterRow("tInput", "tTable", nil))
		})
	}
}

func TestDynamicRowFilters(t *testing.T) {
	input := `
tInput:
   tTable:
     column: "tKey1"
     values: ["tVal1", "tVal2"]
     condition: "=="`
	output := "WHERE `tKey1` == 'tVal1' AND `tKey1` == 'tVal2'"
	var f map[string]map[string]config.RowFilter
	err := yaml.Unmarshal([]byte(input), &f)
	require.NoError(t, err)
	config.Get().Filters = f
	assert.Equal(t, output, FilterRow("tInput", "tTable", nil))

	params := config.TableParams{RowFilter: config.RowFilter{Column: "tKey2", Condition: "!=", Values: []string{"tVal3"}}}
	output1 := "WHERE `tKey2` != 'tVal3'"
	assert.Equal(t, output1, FilterRow("tInput", "tTable", &params))
}

func testBasic(input string, t *testing.T) {
	resetState(t)

	conn := createDBLow(t, input)

	defer func() { test.CheckFail(conn.Close(), t) }()

	//From schemaless code
	var _magicByteAlwaysOnesMask byte = 0x80

	for i := 0; i < 100; i++ {
		if input == "schemaless" {
			body := []byte{}
			body = append(body, _magicByteAlwaysOnesMask)
			body = append(body, []byte(fmt.Sprintf("{\"js\":%d}", i))...)
			execSQL(conn, t, "insert into snap_test_t1(f1,f2,f3,row_key,body) values(?,?,?,?,?)", i, strconv.Itoa(i), float64(i)/3, fmt.Sprintf("%03d", i), body)
		} else {
			execSQL(conn, t, "insert into snap_test_t1(f1,f2,f3) values(?,?,?)", i, strconv.Itoa(i), float64(i)/3)
		}
	}

	enc, err := encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1", input, "kafka", 0)

	test.CheckFail(err, t)

	s, err := Start(input, "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "snap_test_t1", nil, enc, metrics.NewSnapshotMetrics("", nil))
	test.CheckFail(err, t)
	defer s.End()

	var i int64
	for s.FetchNext() {
		key, pKey, data, err := s.Pop()
		if err != nil {
			test.CheckFail(err, t)
		} else {
			cf, err := encoder.Internal.DecodeEvent(data)
			cf.Timestamp = 0
			test.CheckFail(err, t)
			refCF := types.CommonFormatEvent{Type: "insert", Key: []interface{}{float64(i)}, SeqNo: ^uint64(0), Timestamp: 0,
				Fields: &[]types.CommonFormatField{
					{Name: "f1", Value: float64(i)},
					{Name: "f2", Value: strconv.FormatInt(i, 10)},
					{Name: "f3", Value: float64(i) / 3}}}

			if input == "schemaless" {
				b64 := base64.StdEncoding.EncodeToString(append([]byte(fmt.Sprintf("%03d", i)), make([]byte, 13)...))
				*refCF.Fields = append(*refCF.Fields, types.CommonFormatField{Name: "row_key", Value: b64})
				*refCF.Fields = append(*refCF.Fields, types.CommonFormatField{Name: "column_key", Value: nil})
				*refCF.Fields = append(*refCF.Fields, types.CommonFormatField{Name: "ref_key", Value: nil})
				*refCF.Fields = append(*refCF.Fields, types.CommonFormatField{Name: "body", Value: map[string]interface{}{"js": float64(i)}})
			}

			changeCfFields(cf)
			if !reflect.DeepEqual(&refCF, cf) {
				log.Errorf("Received: %+v %+v", cf, cf.Fields)
				log.Errorf("Reference: %+v %+v", &refCF, refCF.Fields)
				t.FailNow()
			}
			if key != encoder.GetCommonFormatKey(cf) {
				log.Errorf("Received: %+v %+v", key, pKey)
				log.Errorf("Reference: %+v", encoder.GetCommonFormatKey(cf))
				t.FailNow()
			}
			i++
		}
	}

}

func TestBasic(t *testing.T) {
	testBasic("mysql", t)
}

func TestMoreFieldTypes(t *testing.T) {
	resetState(t)

	conn := createDB(t)
	defer func() { test.CheckFail(conn.Close(), t) }()

	for i := 0; i < 1; i++ {
		execSQL(conn, t, "insert into snap_test_t1(f1) values(?)", i)
	}

	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f4 text")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f5 timestamp(6)")
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
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f16 datetime(6)")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f17 enum('one','two')")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f18 set('one','two')")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f19 boolean")
	execSQL(conn, t, "ALTER TABLE snap_test_t1 add f20 json")

	//msgpack doesn't preserve int size, so all int32 became int64
	expectedType := []string{
		"int32",
		"string",
		"float64",
		"string",
		"time.Time",
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
		"time.Time",
		"string",
		"string",
		"bool",
		"string",
	}

	t0 := time.Now()
	execSQL(conn, t, "update snap_test_t1 set f5=?", t0) //Reset timestamp field for the rows inserted before alter
	execSQL(conn, t, "insert into snap_test_t1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 1567, strconv.Itoa(1567), float64(1567)/3, "testtextfield", t0, t0, t0, 2017, 98878, []byte("testbinaryfield"), 827738, 111.23, 222.34, 333.45, 444.56, t0, "one", "one,two", true, "{\"one\":\"two\"}")

	if !state.RegisterTableInState(&db.Loc{Service: "snap_test_svc1", Name: "snap_test_db1",
		Cluster: "snap_test_cluster1"}, "snap_test_t1", "mysql", "", 1, "", "", 0) {
		t.FailNow()
	}

	enc, err := encoder.Create("msgpack", "snap_test_svc1", "snap_test_db1", "snap_test_t1", "mysql", "", 1)
	test.CheckFail(err, t)

	s, err := Start("mysql", "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "snap_test_t1", nil, enc, metrics.NewSnapshotMetrics("", nil))
	test.CheckFail(err, t)
	defer s.End()

	for s.FetchNext() {
		_, _, msg, err := s.Pop()
		test.CheckFail(err, t)
		d, err := enc.DecodeEvent(msg)
		test.CheckFail(err, t)
		sch := enc.Schema()
		for i, v := range *d.Fields {
			log.Debugf("%v: types %v = %v(%v). value %v", v.Name, expectedType[i], sch.Columns[i].DataType, sch.Columns[i].Type, v.Value)
			if v.Value != nil && reflect.TypeOf(v.Value).String() != expectedType[i] {
				t.Fatalf("%v: got: %v expected: %v %v %v", v.Name, reflect.TypeOf(v.Value).String(), expectedType[i], sch.Columns[i].DataType, sch.Columns[i].Type)
			}
		}
	}

	s.End()

	execSQL(conn, t, "ALTER TABLE snap_test_t1 drop f15")

	_, err = Start("mysql", "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "snap_test_t1", nil, enc, metrics.NewSnapshotMetrics("", nil))
	test.Assert(t, err != nil, "encoder has old schema")
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

	enc, err := encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1", "mysql", "kafka", 0)

	test.CheckFail(err, t)

	s, err := Start("mysql", "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "snap_test_t1", nil, enc, metrics.NewSnapshotMetrics("", nil))
	test.CheckFail(err, t)

	var i int64
	for s.FetchNext() {
		key, pKey, data, err := s.Pop()
		if err != nil {
			test.CheckFail(err, t)
		} else {
			cf, err := encoder.Internal.DecodeEvent(data)
			// cf, err := encoder.CommonFormatDecode(data)
			test.CheckFail(err, t)
			cf.Timestamp = 0
			refcf := types.CommonFormatEvent{Type: "insert", Key: []interface{}{float64(i)}, SeqNo: ^uint64(0), Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: float64(i)}, {Name: "f2", Value: strconv.FormatInt(i, 10)}, {Name: "f3", Value: float64(i) / 3}}}
			// refcf := types.CommonFormatEvent{Type: "insert", Key: []interface{}{i}, SeqNo: 0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: i}, {Name: "f2", Value: strconv.FormatInt(i, 10)}, {Name: "f3", Value: float64(i) / 3}}}

			changeCfFields(cf)

			if !reflect.DeepEqual(&refcf, cf) {
				log.Errorf("Received: %+v %+v", cf, cf.Fields)
				log.Errorf("Reference: %+v %+v", &refcf, refcf.Fields)
				t.FailNow()
			}
			if key != encoder.GetCommonFormatKey(cf) || pKey != key {
				log.Errorf("Received: %+v %+v", key, pKey)
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

	enc, err := encoder.Create(encoder.Internal.Type(), "snap_test_svc1", "snap_test_db1", "snap_test_t1", types.InputMySQL, "kafka", 0)

	test.CheckFail(err, t)

	_, err = Start("mysql", "snap_test_svc1", "please_return_nil_db_addr", "snap_test_db1", "snap_test_t1", nil, enc, nil)
	test.Assert(t, err != nil, "get connection should return nil")
}

func prepareBench(t *testing.T) (Reader, encoder.Encoder) {
	resetState(t)

	conn := createDBLow(t, types.InputMySQL)

	defer func() { test.CheckFail(conn.Close(), t) }()

	for i := 0; i < 100; i++ {
		execSQL(conn, t, "insert into snap_test_t1(f1,f2,f3) values(?,?,?)", i, strconv.Itoa(i), float64(i)/3)
	}

	enc, err := encoder.Create("msgpack", "snap_test_svc1", "snap_test_db1", "snap_test_t1", types.InputMySQL, "kafka", 0)
	test.CheckFail(err, t)

	s, err := Start(types.InputMySQL, "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "snap_test_t1", nil, enc, metrics.NewSnapshotMetrics("", nil))
	test.CheckFail(err, t)

	return s, enc
}

func runBenchmarks() {
	t := new(testing.T)
	s, enc := prepareBench(t)
	defer s.End()

	benchmarks := []testing.InternalBenchmark{}
	bm := testing.InternalBenchmark{
		Name: "[name=mysql-msgpack]",
		F: func(b *testing.B) {
			for i := b.N; i > 0; {
				for ; i > 0 && s.FetchNext(); i-- {
					_, _, _, err := s.Pop()
					test.CheckFail(err, t)
				}
				if i > 0 {
					b.StopTimer()
					s.End()
					var err error
					s, err = Start(types.InputMySQL, "snap_test_svc1", "snap_test_cluster1", "snap_test_db1", "snap_test_t1", nil, enc, metrics.NewSnapshotMetrics("", nil))
					test.CheckFail(err, t)
					b.StartTimer()
				}
			}
		},
	}
	benchmarks = append(benchmarks, bm)

	log.Configure(cfg.LogType, "error", config.Environment() == config.EnvProduction)

	anything := func(pat, str string) (bool, error) { return true, nil }
	testing.Main(anything, nil, benchmarks, nil)
}
