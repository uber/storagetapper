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

package encoder

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var cfg *config.AppConfig
var testServ = "test_svc1"
var testDB = "db1"
var testTable = "t1"

var testBasicResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{1.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 1.0}}},
	{Type: "insert", Key: []interface{}{2.0}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 2.0}}},
	{Type: "delete", Key: []interface{}{2.0}, SeqNo: 3.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{12.0}, SeqNo: 4.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 12.0}}},
	{Type: "delete", Key: []interface{}{1.0}, SeqNo: 5.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{3.0}, SeqNo: 6.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 3.0}}},
}

type rowRec struct {
	tp     int
	fields []interface{}
}

var testBasicResultRow = []rowRec{
	/* Test basic insert, update, delete */
	{types.Insert, []interface{}{int64(1)}},
	{types.Insert, []interface{}{int64(2)}},
	{types.Delete, []interface{}{int64(2)}},
	{types.Insert, []interface{}{int64(12)}},
	{types.Delete, []interface{}{int64(1)}},
	{types.Insert, []interface{}{int64(3)}},
}

var testAllDataTypesResultRow = []rowRec{
	//Test various data types
	{types.Insert, []interface{}{int64(1), "asdf", "jkl;", []byte("abc"), "2017-07-06 11:55:57.8467835 -0700 PDT", "2017-07-06 11:55:57.846950724 -0700 PDT", "2017-07-06 11:55:57.846955766 -0700 PDT", int32(2017), int64(1) << 54, []byte("abc"), int32(8765), float32(1111), 2222.67, 3333.67, 4444.67}},
	{types.Delete, []interface{}{int64(1)}},
}

var testAllDataTypesResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{1.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 1.0}, {Name: "f2", Value: "asdf"}, {Name: "f3", Value: "jkl;"}, {Name: "f4", Value: []byte("abc")}, {Name: "f5", Value: "2017-07-06 11:55:57.8467835 -0700 PDT"}, {Name: "f6", Value: "2017-07-06 11:55:57.846950724 -0700 PDT"}, {Name: "f7", Value: "2017-07-06 11:55:57.846955766 -0700 PDT"}, {Name: "f8", Value: 2017.0}, {Name: "f9", Value: 1.8014398509481984e+16}, {Name: "f10", Value: []byte("abc")}, {Name: "f11", Value: 8765.0}, {Name: "f12", Value: float64(1111)}, {Name: "f13", Value: 2222.67}, {Name: "f14", Value: 3333.67}, {Name: "f15", Value: 4444.67}}},
	{Type: "delete", Key: []interface{}{1.0}, SeqNo: 2.0, Timestamp: 0, Fields: nil},
}

var testErrorDecoding = [][]byte{
	[]byte("123234"),
	[]byte("1231224212324132"),
	[]byte("asdfasdcasfa"),
}

var testBasicPrepare = []string{
	"drop database if exists db1",
	"create database if not exists db1",
	"drop database if exists db2",
	"create database if not exists db2",

	`create table db1.t1 (
		f1 bigint not null primary key
	)`,
	`create table db1.t2 (
		f1 bigint not null primary key,
		f2 char(16),
		f3 varchar(32),
		f4 text,
		f5 timestamp,
		f6 date,
		f7 time,
		f8 year,
		f9 bigint,
		f10 binary,
		f11 int,
		f12 float,
		f13 double,
		f14 decimal,
		f15 numeric
	)`,
}

// TestGetType tests basic type method
func TestType(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", t)

		enc, err := InitEncoder(encType, testServ, testDB, testTable)
		test.CheckFail(err, t)

		test.Assert(t, enc.Type() == encType, "type diff")
	}
}

func TestEncodeDecodeCommonFormat(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, "enc_test_svc1", "db1", "t1")
		test.CheckFail(err, t)

		for _, ref := range testBasicResult {
			log.Debugf("Initial CF: %v %+v\n", ref, ref.Fields)
			encoded, err := enc.CommonFormat(&ref)
			test.CheckFail(err, t)

			decoded, err := enc.DecodeEvent(encoded)
			log.Debugf("Post CF: %v %+v\n", decoded, decoded.Fields)
			test.CheckFail(err, t)

			ChangeCfFields(encType, decoded, &ref, t)

			if enc.Type() == "avro" && ref.Type == "delete" {
				key := GetCommonFormatKey(&ref)
				ref.Key = make([]interface{}, 0)
				ref.Key = append(ref.Key, key)
			}

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

func TestEncodeDecodeSchema(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, "enc_test_svc1", "db1", "t1")
		test.CheckFail(err, t)

		// Avro doesn't support schema in the stream
		if enc.Type() == "avro" {
			continue
		}

		encoded, err := enc.Row(types.Schema, nil, 0)
		test.CheckFail(err, t)

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)
		decoded.Timestamp = 0

		ref := types.CommonFormatEvent{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 0.0, Timestamp: 0.0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}}}
		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}
}

func TestEncodeDecodeRow(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, "enc_test_svc1", "db1", "t1")
		test.CheckFail(err, t)

		var seqno uint64
		for _, row := range testBasicResultRow {
			log.Debugf("Initial CF: %+v\n", row)
			seqno++
			encoded, err := enc.Row(row.tp, &row.fields, seqno)
			test.CheckFail(err, t)

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)
			decoded.Timestamp = 0

			ref := testBasicResult[seqno-1]
			log.Debugf("Initial CF: %+v %+v\n", ref, ref.Fields)

			ChangeCfFields(encType, decoded, &ref, t)

			if enc.Type() == "avro" && ref.Type == "delete" {
				key := GetCommonFormatKey(&ref)
				ref.Key = make([]interface{}, 0)
				ref.Key = append(ref.Key, key)
			}

			log.Debugf("Post CF: %v %v\n", decoded, decoded.Fields)

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := InitEncoder(encType, "", "", "")
		test.CheckFail(err, t)

		if enc.Type() == "avro" {
			continue
		}

		for _, cf := range testBasicResult {
			log.Debugf("Initial CF: %v\n", cf)
			encoded, err := enc.CommonFormat(&cf)
			test.CheckFail(err, t)

			decoded, err := enc.DecodeEvent(encoded)
			log.Debugf("Post CF: %v\n", decoded)
			test.CheckFail(err, t)

			test.Assert(t, reflect.DeepEqual(&cf, decoded), "decoded different from initial")
		}
	}
}

func TestUnmarshalError(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", t)
		enc, err := InitEncoder(encType, "", "", "")
		test.CheckFail(err, t)

		if enc.Type() == "avro" {
			continue
		}

		for _, encoded := range testErrorDecoding {
			_, err := enc.DecodeEvent(encoded)
			test.Assert(t, err != nil, "not getting an error from garbage input")
		}
	}

}

func TestEncodeDecodeCommonFormatAllDataTypes(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, "enc_test_svc1", "db1", "t2")
		test.CheckFail(err, t)

		for _, ref := range testBasicResult {
			log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

			encoded, err := enc.CommonFormat(&ref)
			test.CheckFail(err, t)

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)
			decoded.Timestamp = 0

			ChangeCfFields(enc.Type(), decoded, &ref, t)

			if enc.Type() == "avro" && ref.Type == "delete" {
				key := GetCommonFormatKey(&ref)
				ref.Key = make([]interface{}, 0)
				ref.Key = append(ref.Key, key)
			}

			log.Debugf("1Post CF: %v %v\n", decoded, decoded.Fields)
			test.CheckFail(err, t)

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

func TestEncodeDecodeRowAllDataTypes(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, "enc_test_svc1", "db1", "t2")
		test.CheckFail(err, t)

		var seqno uint64
		for _, row := range testAllDataTypesResultRow {
			log.Debugf("Initial CF: %+v\n", row)
			seqno++
			encoded, err := enc.Row(row.tp, &row.fields, seqno)
			test.CheckFail(err, t)

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)
			decoded.Timestamp = 0

			ref := testAllDataTypesResult[seqno-1]

			ChangeCfFields(encType, decoded, &ref, t)

			if enc.Type() == "avro" && ref.Type == "delete" {
				key := GetCommonFormatKey(&ref)
				ref.Key = make([]interface{}, 0)
				ref.Key = append(ref.Key, key)
			}

			/*
				if decoded.Fields != nil {
					for i, v := range *decoded.Fields {
						log.Debugf("%v %v %v %v", v.Value, (*ref.Fields)[i].Value, reflect.TypeOf(v.Value), reflect.TypeOf((*ref.Fields)[i].Value))
					}
				}
			*/

			//log.Debugf("decoded: %+v %+v", decoded, decoded.Fields)
			//log.Debugf("%+v %+v", ref, ref.Fields)

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

//TODO: add test to ensure bad connection gives error and not panic in create

func ExecSQL(db *sql.DB, t *testing.T, query string) {
	test.CheckFail(util.ExecSQL(db, query), t)
}

func schemaGet(namespace string, schemaName string, typ string) (*types.AvroSchema, error) {
	var err error
	var a *types.AvroSchema

	s := state.GetOutputSchema(schemaName, typ)
	if s != "" {
		a = &types.AvroSchema{}
		err = json.Unmarshal([]byte(s), a)
	} else {
		a, err = GetSchemaWebster(namespace, schemaName, typ)
	}

	return a, err
}

func Prepare(t *testing.T, create []string, table string) {
	test.SkipIfNoMySQLAvailable(t)

	dbc, err := db.OpenService(&db.Loc{Cluster: "test_cluster1", Service: "enc_test_svc1"}, "")
	test.CheckFail(err, t)

	ExecSQL(dbc, t, "RESET MASTER")
	ExecSQL(dbc, t, "SET GLOBAL binlog_format = 'ROW'")
	ExecSQL(dbc, t, "SET GLOBAL server_id=1")
	ExecSQL(dbc, t, "DROP TABLE IF EXISTS "+types.MyDbName+".state")
	ExecSQL(dbc, t, "DROP TABLE IF EXISTS "+types.MyDbName+".columns")
	ExecSQL(dbc, t, "DROP TABLE IF EXISTS "+types.MyDbName+".outputSchema")

	log.Debugf("Preparing database")
	if !state.Init(cfg) {
		t.FailNow()
	}

	for _, s := range create {
		ExecSQL(dbc, t, s)
	}

	if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "enc_test_svc1", Name: "db1"}, table, "mysql", "") {
		t.FailNow()
	}

	avroSchema, err := schema.ConvertToAvro(&db.Loc{Cluster: "test_cluster1", Service: "enc_test_svc1", Name: "db1"}, table, "avro")
	test.CheckFail(err, t)
	n := fmt.Sprintf("hp-tap-%s-%s-%s", "enc_test_svc1", "db1", table)
	err = state.InsertSchema(n, "avro", string(avroSchema))
	test.CheckFail(err, t)

	GetLatestSchema = schemaGet
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	os.Exit(m.Run())
}
