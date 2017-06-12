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
	"os"
	"reflect"
	"testing"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
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
	`create table db2.t1 (
		f1 bigint not null primary key
	)`,
}

// TestGetType tests basic type method
func TestType(t *testing.T) {
	Prepare(t, testBasicPrepare)

	for encType := range encoders {
		log.Debugf("Encoder: %v", t)

		enc, err := InitEncoder(encType, testServ, testDB, testTable)
		test.CheckFail(err, t)

		test.Assert(t, enc.Type() == encType, "type diff")
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	Prepare(t, testBasicPrepare)

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
	Prepare(t, testBasicPrepare)

	for encType := range encoders {
		log.Debugf("Encoder: %v", t)
		enc, err := InitEncoder(encType, "", "", "")
		test.CheckFail(err, t)

		for _, encoded := range testErrorDecoding {
			_, err := enc.DecodeEvent(encoded)
			test.Assert(t, err != nil, "not getting an error from garbage input")
		}
	}

}

//TODO: add test to ensure bad connection gives error and not panic in create

func ExecSQL(db *sql.DB, t *testing.T, query string) {
	test.CheckFail(util.ExecSQL(db, query), t)
}

func Prepare(t *testing.T, create []string) {
	test.SkipIfNoMySQLAvailable(t)

	dbc, err := db.OpenService(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1"}, "")
	test.CheckFail(err, t)

	ExecSQL(dbc, t, "RESET MASTER")
	ExecSQL(dbc, t, "SET GLOBAL binlog_format = 'ROW'")
	ExecSQL(dbc, t, "SET GLOBAL server_id=1")
	ExecSQL(dbc, t, "DROP TABLE IF EXISTS "+types.MyDbName+".state")
	ExecSQL(dbc, t, "DROP TABLE IF EXISTS "+types.MyDbName+".columns")

	log.Debugf("Preparing database")
	if !state.Init(cfg) {
		t.FailNow()
	}

	for _, s := range create {
		ExecSQL(dbc, t, s)
	}

	if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db1"}, "t1", "mysql", "") {
		t.FailNow()
	}
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	os.Exit(m.Run())
}
