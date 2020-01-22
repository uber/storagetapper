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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var cfg *config.AppConfig
var (
	testSvc    = "enc_test_svc1"
	testDB     = "db1"
	testTable  = "t1"
	testInput  = "mysql"
	testOutput = "kafka"
)

var testBasicResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{int64(1)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1)}}},
	{Type: "insert", Key: []interface{}{int64(2)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(2)}}},
	{Type: "delete", Key: []interface{}{int64(2)}, SeqNo: 3, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(12)}, SeqNo: 4, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(12)}}},
	{Type: "delete", Key: []interface{}{int64(1)}, SeqNo: 5, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(3)}, SeqNo: 6, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(3)}}},
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

//Get a time with 0s in micro and nano seconds. Avro encoder has ms precision only
var timeMs = time.Unix(time.Now().UnixNano()/1000000000, ((time.Now().UnixNano()%1000000000)/1000000)*1000000)

var testAllDataTypesResultRow = []rowRec{
	//Test various data types
	{types.Insert, []interface{}{int64(1), "asdf", "jkl;", "abc", timeMs, "2017-07-06 11:55:57.846950724 -0700 PDT", "2017-07-06 11:55:57.846955766 -0700 PDT", int32(2017), int64(1) << 54, []byte("abc"), int32(8765), float32(1111), 2222.67, 3333.67, 4444.67, timeMs.UTC(), int8(1), "{\"one\":\"two\"}"}},
	{types.Delete, []interface{}{int64(1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	//MySQL binlog reader returns string instread of time.Time for the "zero"
	//time. Test that we correctly encode/decode it
	{types.Insert, []interface{}{int64(2), nil, nil, nil, "0000-00-00 00:00:00.123", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "0000-00-00 00:00:00", nil, nil}},
}

var testAllDataTypesResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{int64(1)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1)}, {Name: "f2", Value: "asdf"}, {Name: "f3", Value: "jkl;"}, {Name: "f4", Value: "abc"}, {Name: "f5", Value: timeMs}, {Name: "f6", Value: "2017-07-06 11:55:57.846950724 -0700 PDT"}, {Name: "f7", Value: "2017-07-06 11:55:57.846955766 -0700 PDT"}, {Name: "f8", Value: int32(2017)}, {Name: "f9", Value: int64(1.8014398509481984e+16)}, {Name: "f10", Value: []byte("abc")}, {Name: "f11", Value: int32(8765)}, {Name: "f12", Value: float32(1111)}, {Name: "f13", Value: 2222.67}, {Name: "f14", Value: 3333.67}, {Name: "f15", Value: 4444.67}, {Name: "f16", Value: timeMs.UTC()}, {Name: "f17", Value: true}, {Name: "f18", Value: "{\"one\":\"two\"}"}}},
	{Type: "delete", Key: []interface{}{int64(1)}, SeqNo: 2, Timestamp: 0, Fields: nil},
	//There is special handling for zero time fields(f5,f16) in Avro, see TestEncodeDecodeRowAllDataTypes
	{Type: "insert", Key: []interface{}{int64(2)}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(2)}, {Name: "f2"}, {Name: "f3"}, {Name: "f4"}, {Name: "f5", Value: time.Time{}.In(time.UTC)}, {Name: "f6"}, {Name: "f7"}, {Name: "f8"}, {Name: "f9"}, {Name: "f10"}, {Name: "f11"}, {Name: "f12"}, {Name: "f13"}, {Name: "f14"}, {Name: "f15"}, {Name: "f16", Value: time.Time{}.In(time.UTC)}, {Name: "f17"}, {Name: "f18"}}},
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
		f15 numeric,
		f16 datetime,
		f17 boolean,
		f18 json
	)`,
}

var outJSONSchema = `{"Type":"schema","Key":["f1"],"SeqNo":1,"Timestamp":0,"Fields":[{"Name":"f1","Value":"bigint"},{"Name":"f2","Value":"char(16)"},{"Name":"f3","Value":"varchar(32)"},{"Name":"f4","Value":"text"},{"Name":"f5","Value":"timestamp"},{"Name":"f6","Value":"date"},{"Name":"f7","Value":"time"},{"Name":"f8","Value":"year"},{"Name":"f9","Value":"bigint"},{"Name":"f10","Value":"binary(1)"},{"Name":"f11","Value":"int"},{"Name":"f12","Value":"float"},{"Name":"f13","Value":"double"},{"Name":"f14","Value":"decimal(10,0)"},{"Name":"f15","Value":"decimal(10,0)"},{"Name":"f16","Value":"datetime"},{"Name":"f17","Value":"tinyint(1)"},{"Name":"f18","Value":"json"}]}`

// TestGetType tests basic type method
func TestType(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", t)

		enc, err := InitEncoder(encType, testSvc, testDB, testTable, testInput, testOutput, 0)
		test.CheckFail(err, t)

		test.Assert(t, enc.Type() == encType, "type diff")
	}
}

func SQLType(format string) bool {
	return strings.HasPrefix(format, "ansisql") || strings.HasPrefix(format, "mysql")
}

//func changeFloat64ToInt64(ref *types.CommonFormatEvent) {
func changeInt64ToFloat64(ref *types.CommonFormatEvent) {
	for k, v := range ref.Key {
		switch vv := v.(type) {
		//case float64:
		//	ref.Key[k] = int64(vv)
		case int64:
			ref.Key[k] = float64(vv)
		}
	}

	if ref.Fields == nil {
		return
	}

	for k, v := range *ref.Fields {
		switch vv := v.Value.(type) {
		//		case float64:
		//			(*ref.Fields)[k].Value = int64(vv)
		case int64:
			(*ref.Fields)[k].Value = float64(vv)
		case int32:
			(*ref.Fields)[k].Value = float64(vv)
		case float32:
			(*ref.Fields)[k].Value = float64(vv)
		}
	}
}

func copyEvent(ref *types.CommonFormatEvent) {
	key := make([]interface{}, len(ref.Key))
	copy(key, ref.Key)
	ref.Key = key
	if ref.Fields != nil {
		fields := make([]types.CommonFormatField, len(*ref.Fields))
		copy(fields, *ref.Fields)
		ref.Fields = &fields
	}
}

func TestEncodeDecodeCommonFormat(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, testSvc, testDB, testTable, testInput, testOutput, 0)
		test.CheckFail(err, t)

		if SQLType(encType) {
			continue
		}

		for _, ref := range testBasicResult {
			log.Debugf("Initial CF: %v %+v\n", ref, ref.Fields)
			encoded, err := enc.CommonFormat(&ref)
			test.CheckFail(err, t)

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)
			log.Debugf("Post CF: %v %+v\n", decoded, decoded.Fields)

			decoded.Timestamp = 0
			if enc.Type() == "avro" && ref.Type == "delete" {
				key := GetCommonFormatKey(&ref)
				ref.Key = make([]interface{}, 0)
				ref.Key = append(ref.Key, key)
			}

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
		log.Debugf("after Encoder: %v", encType)
	}
}

func TestEncodeDecodeSchema(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, testSvc, testDB, testTable, testInput, testOutput, 0)
		test.CheckFail(err, t)

		// Avro doesn't support schema in the stream
		if enc.Type() == "avro" {
			continue
		}

		encoded, err := enc.Row(types.Schema, nil, 0, time.Time{})
		test.CheckFail(err, t)

		if SQLType(encType) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)
		decoded.Timestamp = 0

		ref := types.CommonFormatEvent{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 0.0, Timestamp: 0.0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}}}
		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}
}

func TestEncodeDecodeRow(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, testSvc, testDB, testTable, testInput, testOutput, 0)
		test.CheckFail(err, t)

		var seqno uint64
		for _, row := range testBasicResultRow {
			log.Debugf("Initial CF: %+v\n", row)
			seqno++
			encoded, err := enc.Row(row.tp, &row.fields, seqno, time.Time{})
			test.CheckFail(err, t)

			if SQLType(enc.Type()) {
				continue
			}

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)
			decoded.Timestamp = 0

			ref := testBasicResult[seqno-1]
			copyEvent(&ref)

			log.Debugf("Initial CF: %+v %+v\n", ref, ref.Fields)

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
		enc, err := InitEncoder(encType, "", "", "", "", "", 0)
		test.CheckFail(err, t)

		// This test only schemaless formats like JSON or MsgPack
		if enc.Type() == "avro" || SQLType(enc.Type()) {
			continue
		}

		for _, ref := range testBasicResult {
			log.Debugf("Initial CF: %v\n", ref)

			encoded, err := enc.CommonFormat(&ref)
			test.CheckFail(err, t)

			//InitEncoder doesn't initialize schema so JSON encoder will not fix
			//float64 and base64 fields
			if encType == "json" {
				copyEvent(&ref)
				changeInt64ToFloat64(&ref)
			}

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)

			log.Debugf("Post CF: %v\n", decoded)

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

func TestUnmarshalError(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", t)
		enc, err := InitEncoder(encType, "", "", "", "", "", 0)
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
		enc, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		for _, ref := range testAllDataTypesResult {
			copyEvent(&ref)

			log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

			encoded, err := enc.CommonFormat(&ref)
			test.CheckFail(err, t)

			if SQLType(enc.Type()) {
				continue
			}

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)

			decoded.Timestamp = 0

			//There is no way to reconstruct key for delete from Avro
			//message, so hack original key to match key in message.
			if enc.Type() == "avro" {
				if ref.Type == "delete" {
					key := GetCommonFormatKey(&ref)
					ref.Key = make([]interface{}, 0)
					ref.Key = append(ref.Key, key)
				}
				if ref.SeqNo == 3 {
					(*ref.Fields)[4].Value = nil
					(*ref.Fields)[15].Value = nil
				}
			}

			log.Debugf("Post CF: %v %v\n", decoded, decoded.Fields)

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

func TestSchema(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	ref, err := state.GetSchema(testSvc, testDB, "t2", testInput, testOutput, 0)
	test.CheckFail(err, t)

	log.Debugf("Reference schema %+v", ref)

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		test.Assert(t, reflect.DeepEqual(enc.Schema(), ref), "this is not equal to ref %+v", enc.Schema())
	}

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		schema, err := enc.EncodeSchema(1)
		if enc.Type() == "avro" {
			test.Assert(t, schema == nil && err == nil, "Avro doesn't support schema encoding")
			continue
		} else if enc.Type() == "msgpack" {
			d, err := enc.DecodeEvent(schema)
			test.CheckFail(err, t)
			schema, err = json.Marshal(d)
			test.CheckFail(err, t)
		}
		test.CheckFail(err, t)

		s := outJSONSchema
		if SQLType(enc.Type()) {
			s = `CREATE TABLE "t2" ("seqno" BIGINT NOT NULL, "f1" bigint NOT NULL, "f2" char(16), "f3" varchar(32), "f4" text, "f5" timestamp, "f6" date, "f7" time, "f8" year, "f9" bigint, "f10" binary(1), "f11" int, "f12" float, "f13" double, "f14" decimal(10,0), "f15" decimal(10,0), "f16" datetime, "f17" tinyint(1), "f18" json, UNIQUE KEY("seqno"), PRIMARY KEY ("f1"));`
		}
		if enc.Type() == "mysql" || enc.Type() == "mysql_idempotent" {
			s = strings.Replace(s, `"`, "`", -1)
			//			s = "CREATE TABLE `t2` (seqno BIGINT NOT NULL, `f1` bigint NOT NULL, `f2` char(16), `f3` varchar(32), `f4` text, `f5` timestamp NOT NULL, `f6` date, `f7` time, `f8` year, `f9` bigint, `f10` binary(1), `f11` int, `f12` float, `f13` double, `f14` decimal(10,0), `f15` decimal(10,0), `f16` datetime, UNIQUE KEY(seqno), PRIMARY KEY (`f1`));"
		}
		log.Debugf("%v %v", enc.Type(), s)
		require.Equal(t, s, string(schema))
	}
}

//f16 and f17 is deleted from schema as well
var outAvroSchemaWithDeletedf2f10f15 = `{"fields":[{"name":"f1","type":["null","long"]},{"name":"f3","type":["null","string"]},{"name":"f4","type":["null","string"]},{"name":"f5","type":["null","long"]},{"name":"f6","type":["null","string"]},{"name":"f7","type":["null","string"]},{"name":"f8","type":["null","int"]},{"name":"f9","type":["null","long"]},{"name":"f11","type":["null","int"]},{"name":"f12","type":["null","float"]},{"name":"f13","type":["null","double"]},{"name":"f14","type":["null","double"]},{"name":"ref_key","type":["long"]},{"name":"row_key","type":["bytes"]},{"name":"is_deleted","type":["null","boolean"]}],"name":"db1-t2","namespace":"storagetapper","owner":"db1","schema_id":0,"schemaVersion":0,"type":"record","last_modified":""}`

var outJSONSchemaWithDeletedf1f2f10f15 = `{"Type":"schema","Key":["f1"],"SeqNo":1,"Timestamp":0,"Fields":[{"Name":"f3","Value":"varchar(32)"},{"Name":"f4","Value":"text"},{"Name":"f5","Value":"timestamp"},{"Name":"f6","Value":"date"},{"Name":"f7","Value":"time"},{"Name":"f8","Value":"year"},{"Name":"f9","Value":"bigint"},{"Name":"f11","Value":"int"},{"Name":"f12","Value":"float"},{"Name":"f13","Value":"double"},{"Name":"f14","Value":"decimal(10,0)"}]}`

var outAvroSchemaWithDeletedf2f10f15f3f8 = `{"fields":[{"name":"f1","type":["null","long"]},{"name":"f4","type":["null","string"]},{"name":"f5","type":["null","long"]},{"name":"f6","type":["null","string"]},{"name":"f7","type":["null","string"]},{"name":"f9","type":["null","long"]},{"name":"f11","type":["null","int"]},{"name":"f12","type":["null","float"]},{"name":"f13","type":["null","double"]},{"name":"f14","type":["null","double"]},{"name":"ref_key","type":["long"]},{"name":"row_key","type":["bytes"]},{"name":"is_deleted","type":["null","boolean"]}],"name":"db1-t2","namespace":"storagetapper","owner":"db1","schema_id":0,"schemaVersion":0,"type":"record","last_modified":""}`

var outJSONSchemaWithDeletedf1f2f10f15f3f8 = `{"Type":"schema","Key":["f1"],"SeqNo":1,"Fields":[{"Name":"f4","Value":"text"},{"Name":"f5","Value":"timestamp"},{"Name":"f6","Value":"date"},{"Name":"f7","Value":"time"},{"Name":"f9","Value":"bigint"},{"Name":"f11","Value":"int"},{"Name":"f12","Value":"float"},{"Name":"f13","Value":"double"},{"Name":"f14","Value":"decimal(10,0)"}]}`

//We can't remove metadata and primary key "f1" from the schema
var outAvroSchemaWithAllFieldsDeleted = `{"fields":[{"name":"f1","type":["null","long"]}, {"name":"ref_key","type":["long"]},{"name":"row_key","type":["bytes"]},{"name":"is_deleted","type":["null","boolean"]}],"name":"db1-t2","namespace":"storagetapper","owner":"db1","schema_id":0,"schemaVersion":0,"type":"record","last_modified":""}`

var outJSONSchemaWithAllFieldsDeleted = `{"Type":"schema","Key":["f1"],"SeqNo":1}`

func testFilteredField(name string) bool {
	return name == "f2" || name == "f10" || name == "f15" || name == "f16" || name == "f17" || name == "f18"
}

func TestOutputFilters(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	//Insert schema with deleted fields "f2", "f10", "f15" for all encoders
	//and additionally "f1" for json and msgpack, which shouldn't be filtered
	//because it's primary
	err := state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", outAvroSchemaWithDeletedf2f10f15)
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchemaWithDeletedf1f2f10f15)
	test.CheckFail(err, t)

	encs := make([]Encoder, 0)
	for encType := range encoders {
		e, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		encs = append(encs, e)
	}

	for _, enc := range encs {
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.CommonFormat(&ref)
		test.CheckFail(err, t)

		if enc.Type() == "avro" || SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		f := make([]types.CommonFormatField, 0)
		for _, v := range *ref.Fields {
			if !testFilteredField(v.Name) {
				f = append(f, v)
			}
		}
		ref.Fields = &f

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}

	//Remove "f3" and "f8"
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", outAvroSchemaWithDeletedf2f10f15f3f8)
	test.CheckFail(err, t)
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "json")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchemaWithDeletedf1f2f10f15f3f8)
	test.CheckFail(err, t)

	//Fake schema event should trigger schema update
	for _, enc := range encs {
		_, err := enc.CommonFormat(&types.CommonFormatEvent{Type: "schema"})
		test.CheckFail(err, t)
	}

	for _, enc := range encs {
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.CommonFormat(&ref)
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		f := make([]types.CommonFormatField, 0)
		for _, v := range *ref.Fields {
			if v.Name != "f3" && v.Name != "f8" && !testFilteredField(v.Name) {
				f = append(f, v)
			}
		}
		ref.Fields = &f

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}
}

func TestOutputFiltersRow(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	//Insert schema with deleted fields "f2", "f10", "f15" for all encoders
	//and additionally "f1" for json and msgpack, which shouldn't be filtered
	//because it's primary
	err := state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", outAvroSchemaWithDeletedf2f10f15)
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchemaWithDeletedf1f2f10f15)
	test.CheckFail(err, t)

	encs := make([]Encoder, 0)
	for encType := range encoders {
		e, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		encs = append(encs, e)
	}

	for _, enc := range encs {
		refRow := testAllDataTypesResultRow[0]
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.Row(refRow.tp, &refRow.fields, 1, time.Time{})
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		f := make([]types.CommonFormatField, 0)
		for _, v := range *ref.Fields {
			//Remove filtered fields from reference row
			if !testFilteredField(v.Name) {
				f = append(f, v)
			}
		}
		ref.Fields = &f

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}

	//Remove "f3" and "f8"
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", outAvroSchemaWithDeletedf2f10f15f3f8)
	test.CheckFail(err, t)
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "json")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchemaWithDeletedf1f2f10f15f3f8)
	test.CheckFail(err, t)

	//Fake schema event should trigger schema update
	for _, enc := range encs {
		_, err := enc.CommonFormat(&types.CommonFormatEvent{Type: "schema"})
		test.CheckFail(err, t)
	}

	for _, enc := range encs {
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.CommonFormat(&ref)
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		f := make([]types.CommonFormatField, 0)
		for _, v := range *ref.Fields {
			if v.Name != "f3" && v.Name != "f8" && !testFilteredField(v.Name) {
				f = append(f, v)
			}
		}
		ref.Fields = &f

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}
}

func TestOutputFiltersBound(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	outAvroSchema, err := schema.ConvertToAvro(&db.Loc{Cluster: "test_cluster1", Service: testSvc, Name: testDB}, "t2", types.InputMySQL, "avro")
	test.CheckFail(err, t)

	//Complete schema no fields should be filtered
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", string(outAvroSchema))
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchema)
	test.CheckFail(err, t)

	encs := make([]Encoder, 0)
	for encType := range encoders {
		e, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		encs = append(encs, e)
	}

	for _, enc := range encs {
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.CommonFormat(&ref)
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}

	//All fields should be filtered
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", outAvroSchemaWithAllFieldsDeleted)
	test.CheckFail(err, t)
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "json")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchemaWithAllFieldsDeleted)
	test.CheckFail(err, t)

	//Fake schema event should trigger schema update
	for _, enc := range encs {
		_, err := enc.CommonFormat(&types.CommonFormatEvent{Type: "schema"})
		test.CheckFail(err, t)
	}

	for _, enc := range encs {
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.CommonFormat(&ref)
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		*ref.Fields = (*ref.Fields)[:1]

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}
}

func TestOutputFiltersRowBound(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	outAvroSchema, err := schema.ConvertToAvro(&db.Loc{Cluster: "test_cluster1", Service: testSvc, Name: testDB}, "t2", types.InputMySQL, "avro")
	test.CheckFail(err, t)

	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", string(outAvroSchema))
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchema)
	test.CheckFail(err, t)

	encs := make([]Encoder, 0)
	for encType := range encoders {
		e, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		encs = append(encs, e)
	}

	for _, enc := range encs {
		refRow := testAllDataTypesResultRow[0]
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.Row(refRow.tp, &refRow.fields, 1, time.Time{})
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}

	//Remove "f3" and "f8"
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "avro", outAvroSchemaWithAllFieldsDeleted)
	test.CheckFail(err, t)
	err = state.DeleteSchema("hp-tap-enc_test_svc1-db1-t2", "json")
	test.CheckFail(err, t)
	err = state.InsertSchema("hp-tap-enc_test_svc1-db1-t2", "json", outJSONSchemaWithAllFieldsDeleted)
	test.CheckFail(err, t)

	//Fake schema event should trigger schema update
	for _, enc := range encs {
		_, err := enc.CommonFormat(&types.CommonFormatEvent{Type: "schema"})
		test.CheckFail(err, t)
	}

	for _, enc := range encs {
		ref := testAllDataTypesResult[0]
		copyEvent(&ref)
		log.Debugf("Encoder: %v", enc.Type())
		log.Debugf("Initial CF: %v %v\n", ref, ref.Fields)

		encoded, err := enc.CommonFormat(&ref)
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		decoded, err := enc.DecodeEvent(encoded)
		test.CheckFail(err, t)

		decoded.Timestamp = 0

		*ref.Fields = (*ref.Fields)[:1]

		log.Debugf("Patched ref CF: %v %v\n", ref, ref.Fields)
		log.Debugf("Post CF       : %v %v\n", decoded, decoded.Fields)

		test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
	}
}

func TestEncodeDecodeRowAllDataTypes(t *testing.T) {
	Prepare(t, testBasicPrepare, "t2")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
		test.CheckFail(err, t)

		var seqno uint64
		for _, row := range testAllDataTypesResultRow {
			log.Debugf("Initial CF: %+v\n", row)

			seqno++
			encoded, err := enc.Row(row.tp, &row.fields, seqno, time.Time{})
			test.CheckFail(err, t)

			if SQLType(enc.Type()) {
				continue
			}

			decoded, err := enc.DecodeEvent(encoded)
			test.CheckFail(err, t)

			decoded.Timestamp = 0

			log.Debugf("Decoded CF: %v %v\n", decoded, decoded.Fields)

			ref := testAllDataTypesResult[seqno-1]
			log.Debugf("Ref CF: %v %v\n", ref, ref.Fields)

			copyEvent(&ref)

			if enc.Type() == "avro" {
				if ref.Type == "delete" {
					key := GetCommonFormatKey(&ref)
					ref.Key = make([]interface{}, 0)
					ref.Key = append(ref.Key, key)
				}
				if ref.SeqNo == 3 {
					(*ref.Fields)[4].Value = nil
					(*ref.Fields)[15].Value = nil
				}
			}

			log.Debugf("Post CF: %v %v\n", decoded, decoded.Fields)

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

func TestUnwrapEvent(t *testing.T) {
	Prepare(t, testBasicPrepare, "t1")

	for encType := range encoders {
		log.Debugf("Encoder: %v", encType)
		enc, err := Create(encType, testSvc, testDB, "t1", testInput, testOutput, 0)
		test.CheckFail(err, t)

		if SQLType(enc.Type()) {
			continue
		}

		for _, ref := range testBasicResult {
			log.Debugf("Initial CF: %v %+v\n", ref, ref.Fields)
			encoded, err := enc.CommonFormat(&ref)
			test.CheckFail(err, t)

			Internal = enc
			wrapped, err := WrapEvent("test_format", "test key", encoded, 1)
			test.CheckFail(err, t)

			envelope := &types.CommonFormatEvent{}
			payload, err := enc.UnwrapEvent(wrapped, envelope)
			if enc.Type() == "avro" {
				test.Assert(t, err != nil, "Avro doesn't support wrapping")
				continue
			}

			test.CheckFail(err, t)

			envelope.Timestamp = 0
			test.Assert(t, reflect.DeepEqual(envelope, &types.CommonFormatEvent{Type: "test_format", Key: []interface{}{"test key"}, SeqNo: 1}), "got: %+v", envelope)

			decoded, err := enc.DecodeEvent(payload)
			log.Debugf("Post CF: %v %+v\n", decoded, decoded.Fields)
			test.CheckFail(err, t)

			if enc.Type() == "avro" && ref.Type == "delete" {
				key := GetCommonFormatKey(&ref)
				ref.Key = make([]interface{}, 0)
				ref.Key = append(ref.Key, key)
			}

			test.Assert(t, reflect.DeepEqual(&ref, decoded), "decoded different from initial")
		}
	}
}

func TestUnwrapBinaryKey(t *testing.T) {
	key := "test_key"
	enc, err := Create("json", testSvc, testDB, "t1", testInput, testOutput, 0)
	test.CheckFail(err, t)
	Internal = enc
	b, err := WrapEvent("test_format", key, []byte("test_body"), 1)
	test.CheckFail(err, t)
	var cf types.CommonFormatEvent
	_, err = enc.UnwrapEvent(b, &cf)
	test.CheckFail(err, t)
	test.Assert(t, len(cf.Key) == 1, "only row key in envelope")
	s, ok := cf.Key[0].(string)
	test.Assert(t, ok, "row key is marshalled as a string")
	test.Assert(t, s == key, "keys should be equal")
	b = []byte(`{"Key": ["test_key"]}`)
	var cf1 types.CommonFormatEvent
	_, err = enc.UnwrapEvent(b, &cf1)
	test.CheckFail(err, t)
	test.Assert(t, len(cf1.Key) == 1, "only row key in envelope")
	s, ok = cf1.Key[0].(string)
	test.Assert(t, ok, "row key is marshalled as a string")
	test.Assert(t, s == key, "keys should be equal")
}

func ExecSQL(db *sql.DB, t test.Failer, query string) {
	test.CheckFail(util.ExecSQL(db, query), t)
}

func schemaGet(namespace string, schemaName string, typ string) (*types.AvroSchema, error) {
	var err error
	var a *types.AvroSchema

	s := state.GetOutputSchema(schemaName, typ)
	if s != "" {
		a = &types.AvroSchema{}
		err = json.Unmarshal([]byte(s), a)
	}

	return a, err
}

func Prepare(t test.Failer, create []string, table string) {
	test.SkipIfNoMySQLAvailable(t)
	state.Reset()

	loc := &db.Loc{Cluster: "test_cluster1", Service: testSvc}

	dbc, err := db.OpenService(loc, "", types.InputMySQL)
	test.CheckFail(err, t)

	ExecSQL(dbc, t, "RESET MASTER")
	ExecSQL(dbc, t, "SET GLOBAL binlog_format = 'ROW'")
	ExecSQL(dbc, t, "SET GLOBAL server_id=1")

	for _, s := range create {
		ExecSQL(dbc, t, s)
	}

	loc.Name = testDB

	if !state.RegisterTableInState(loc, table, testInput, testOutput, 0, "", "", 0) {
		t.FailNow()
	}

	n := fmt.Sprintf("hp-tap-%s-%s-%s", testSvc, testDB, table)

	avroSchema, err := schema.ConvertToAvro(loc, table, types.InputMySQL, "avro")
	test.CheckFail(err, t)
	err = state.InsertSchema(n, "avro", string(avroSchema))
	test.CheckFail(err, t)

	GetLatestSchema = schemaGet
}

var result []byte
var resCf *types.CommonFormatEvent

type encBench struct {
	enc Encoder
	cf  types.CommonFormatEvent
	res []byte
}

func benchImpl(encType string, arr []byte) (*encBench, error) {
	var e encBench
	var err error
	e.enc, err = Create(encType, testSvc, testDB, "t2", testInput, testOutput, 0)
	if err != nil {
		return nil, err
	}

	e.cf = testAllDataTypesResult[0]
	copyEvent(&e.cf)

	(*e.cf.Fields)[9].Value = arr

	return &e, nil
}

func (e *encBench) encode(n int, b *testing.B) {
	var err error
	for i := n; i > 0; i-- {
		e.cf.SeqNo = uint64(i)
		(*e.cf.Fields)[0].Value = int64(i)
		result, err = e.enc.CommonFormat(&e.cf)
		test.CheckFail(err, b)
	}
}

func (e *encBench) decode(n int, b *testing.B) {
	var err error
	for i := n; i > 0; i-- {
		resCf, err = e.enc.DecodeEvent(e.res)
		test.CheckFail(err, b)
	}
}

func runBenchmarks() {
	arr := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		arr[i] = byte(i)
	}

	t := new(testing.T)
	Prepare(t, testBasicPrepare, "t2")

	benchmarks := []testing.InternalBenchmark{}
	for encType := range encoders {
		impl, err := benchImpl(encType, arr)
		test.CheckFail(err, t)
		bm := testing.InternalBenchmark{
			Name: fmt.Sprintf("[name=%v-encode-1k]", encType),
			F: func(b *testing.B) {
				impl.encode(b.N, b)
			},
		}
		benchmarks = append(benchmarks, bm)
		impl.res, err = impl.enc.CommonFormat(&impl.cf)
		test.CheckFail(err, t)
		bm = testing.InternalBenchmark{
			Name: fmt.Sprintf("[name=%v-decode-1k]", encType),
			F: func(b *testing.B) {
				impl.decode(b.N, b)
			},
		}
		benchmarks = append(benchmarks, bm)

		impls, err := benchImpl(encType, make([]byte, 1))
		test.CheckFail(err, t)

		bm = testing.InternalBenchmark{
			Name: fmt.Sprintf("[name=%v-encode-small]", encType),
			F: func(b *testing.B) {
				impls.encode(b.N, b)
			},
		}
		benchmarks = append(benchmarks, bm)

		impls.res, err = impl.enc.CommonFormat(&impl.cf)
		test.CheckFail(err, t)
		bm = testing.InternalBenchmark{
			Name: fmt.Sprintf("[name=%v-decode-small]", encType),
			F: func(b *testing.B) {
				impls.decode(b.N, b)
			},
		}
		benchmarks = append(benchmarks, bm)
	}

	log.Configure(cfg.LogType, "error", config.Environment() == config.EnvProduction)

	anything := func(pat, str string) (bool, error) { return true, nil }
	testing.Main(anything, nil, benchmarks, nil)
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	log.Debugf("Preparing database")
	if err := state.InitManager(shutdown.Context, cfg); err != nil {
		log.Errorf("Failed to initialize state")
		os.Exit(1)
	}
	defer state.Close()

	GenTime = func() int64 { return 0 }

	if m.Run() != 0 {
		os.Exit(1)
	}

	runBenchmarks()
}
