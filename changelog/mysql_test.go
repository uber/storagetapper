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

package changelog

import (
	"database/sql"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/pool"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var cfg *config.AppConfig
var saveCfg config.AppConfig
var globalTPoolProcs int32
var fakePool pool.Thread
var alterCh = make(chan bool)
var testReader *mysqlReader

//TODO: 1.8 export the t.Name() so no hack is needed
var testName string

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

/* Test basic insert, update, delete */
var testBasic = []string{
	"insert into db1.t1(f1) value (1)",
	"insert into db1.t1(f1) value (2)",
	"update db1.t1 set f1=f1+10 where f1=2",
	"delete from db1.t1 where f1=1",
	"insert into db1.t1(f1) value (3)",
}

var testBasicResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{int64(1)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1)}}},
	{Type: "insert", Key: []interface{}{int64(2)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(2)}}},
	{Type: "delete", Key: []interface{}{int64(2)}, SeqNo: 3, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(12)}, SeqNo: 4, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(12)}}},
	{Type: "delete", Key: []interface{}{int64(1)}, SeqNo: 5, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(3)}, SeqNo: 6, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(3)}}},
}

/* Test with default database */
var testUseDB = []string{
	"use db1",
	"insert into t1(f1) value (4)",
	"insert into db1.t1(f1) value (5)",
	"use db2",
	"insert into db1.t1(f1) value (6)",
	"update t1 set f1=f1+11 where f1=3",
	"insert into db2.t1 values (7)",
}

var testUseDBResult = []types.CommonFormatEvent{
	/* Test with default database */
	{Type: "insert", Key: []interface{}{int64(4)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(4)}}},
	{Type: "insert", Key: []interface{}{int64(5)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(5)}}},
	{Type: "insert", Key: []interface{}{int64(6)}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(6)}}},
}

var testMultiColumnPrepare = []string{
	"drop database if exists db1",
	"create database if not exists db1",

	`create table db1.t1 (
		f1 bigint not null primary key,
		f2 bigint,
		f3 bigint
	)`,
}

/* Test multi column queries */
var testMultiColumn = []string{
	"insert into db1.t1(f1,f2,f3) values (7,2,3)",
	"insert into db1.t1(f1,f2,f3) values (8,2,4)",
	"insert into db1.t1(f1,f2) values (9,2)",
	"insert into db1.t1(f1,f3) values (10,2)",
	"update db1.t1 set f1=f1+10, f2=f2-1 where f3=3",
}

/* Test multi column queries */
var testMultiColumnResult = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{int64(7)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(7)}, {Name: "f2", Value: int64(2)}, {Name: "f3", Value: int64(3)}}},
	{Type: "insert", Key: []interface{}{int64(8)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(8)}, {Name: "f2", Value: int64(2)}, {Name: "f3", Value: int64(4)}}},
	{Type: "insert", Key: []interface{}{int64(9)}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(9)}, {Name: "f2", Value: int64(2)}, {Name: "f3", Value: nil}}},
	{Type: "insert", Key: []interface{}{int64(10)}, SeqNo: 4, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(10)}, {Name: "f2", Value: nil}, {Name: "f3", Value: int64(2)}}},
	{Type: "delete", Key: []interface{}{int64(7)}, SeqNo: 5, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(17)}, SeqNo: 6, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(17)}, {Name: "f2", Value: int64(1)}, {Name: "f3", Value: int64(3)}}},
}

/* Test multi row binlog events */
var testMultiRow = []string{
	"insert db1.t1(f1,f2,f3) values (100, 101, 102), (110, 111, 112), (120, 121, 122)",
	"update db1.t1 set f1=f1+11, f3=f3+1",
	"delete from db1.t1 where f1 >= 100",
}

/* Test multi row binlog events */
var testMultiRowResult = []types.CommonFormatEvent{
	//"insert db1.t1(f1,f2,f3) values (100, 101, 102), (110, 111, 112), (120, 121, 122)"
	{Type: "insert", Key: []interface{}{int64(100)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(100)}, {Name: "f2", Value: int64(101)}, {Name: "f3", Value: int64(102)}}},
	{Type: "insert", Key: []interface{}{int64(110)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(110)}, {Name: "f2", Value: int64(111)}, {Name: "f3", Value: int64(112)}}},
	{Type: "insert", Key: []interface{}{int64(120)}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(120)}, {Name: "f2", Value: int64(121)}, {Name: "f3", Value: int64(122)}}},
	//"update db1.t1 set f1=f1+11, f3=f3+1 where f2=2"
	{Type: "delete", Key: []interface{}{int64(100)}, SeqNo: 4, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(111)}, SeqNo: 5, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(111)}, {Name: "f2", Value: int64(101)}, {Name: "f3", Value: int64(103)}}},
	{Type: "delete", Key: []interface{}{int64(110)}, SeqNo: 6, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(121)}, SeqNo: 7, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(121)}, {Name: "f2", Value: int64(111)}, {Name: "f3", Value: int64(113)}}},
	//"delete from db1.t1 where f1 >= 100"
	{Type: "delete", Key: []interface{}{int64(120)}, SeqNo: 8, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(131)}, SeqNo: 9, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(131)}, {Name: "f2", Value: int64(121)}, {Name: "f3", Value: int64(123)}}},
	{Type: "delete", Key: []interface{}{int64(111)}, SeqNo: 10, Timestamp: 0, Fields: nil},
	{Type: "delete", Key: []interface{}{int64(121)}, SeqNo: 11, Timestamp: 0, Fields: nil},
	{Type: "delete", Key: []interface{}{int64(131)}, SeqNo: 12, Timestamp: 0, Fields: nil},
}

/*Test compound primary key */
var testCompoundKeyPrepare = []string{
	"drop database if exists db1",
	"create database if not exists db1",

	`create table db1.t1 (
		f1 bigint not null,
		f2 varchar(15) not null,
		primary key (f1, f2)
	)`,
}

var testCompoundKey = []string{
	"insert into db1.t1(f1,f2) value (1,'aa aa')",
	"insert into db1.t1(f1,f2) value (2,'bbb')",
	"update db1.t1 set f1=f1+10 where f1=2",
	"delete from db1.t1 where f1=1",
	"insert into db1.t1(f1, f2) value (3,'aaa')",
}

var testCompoundKeyResult = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{int64(1), "aa aa"}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1)}, {Name: "f2", Value: "aa aa"}}},
	{Type: "insert", Key: []interface{}{int64(2), "bbb"}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(2)}, {Name: "f2", Value: "bbb"}}},
	{Type: "delete", Key: []interface{}{int64(2), "bbb"}, SeqNo: 3, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(12), "bbb"}, SeqNo: 4, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(12)}, {Name: "f2", Value: "bbb"}}},
	{Type: "delete", Key: []interface{}{int64(1), "aa aa"}, SeqNo: 5, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(3), "aaa"}, SeqNo: 6, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(3)}, {Name: "f2", Value: "aaa"}}},
}

var testDDLPrepare = []string{
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

//If modify alter logic here please check the alter-sync logic in
//consumeTableEvents
var testDDL = []string{
	"insert into db1.t1 value (1)",
	"insert into db1.t1 value (2)",
	`	 alter table 
	db1.t1 add 	f2 
	varchar(32) 	`,
	"alter table db2.t1 add f2 varchar(32)", //we don't track db2 so this should not affect db1.t1
	"insert into db1.t1 value (3, 'aaa')",
	"use db1",
	"alter table t1 drop f2",
	"insert into t1 value (4)",
	"insert into db1.t1 value (5)",
	"use db2",
	"insert into db2.t1 value (7, 'eee')",
	"alter table t1 drop f2", // this is db2 table. should be skipped as well
	"insert into db1.t1 value (6)",
	"update t1 set f1=f1+1 where f1=2",
	"insert into db2.t1 value (8)",
	"use db1",
	"update t1 set f1=f1+7 where f1=2",
	"alter table t1 add f3 varchar(128), add f4 text, add f5 blob, add f6 varchar(32), add f7 int",
	"insert into db1.t1 value (45676, 'ggg', 'ttt', 'yyy', 'vvv', 7543)",
	"alter table t1 add index(f3,f6,f7)",
	"ALTER TABLE t1 MODIFY f4 varchar(20)",
	/*Test names in backticks */
	"ALTER TABLE `t1` drop f7, drop f6, drop f5, drop f4",
	"ALTER TABLE `db1`.`t1` drop f3",
}

var testDDLResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{int64(1)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1)}}},
	{Type: "insert", Key: []interface{}{int64(2)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(2)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f2", Value: "varchar(32)"}}},
	{Type: "insert", Key: []interface{}{int64(3)}, SeqNo: 4, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(3)}, {Name: "f2", Value: "aaa"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 5, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}}},
	{Type: "insert", Key: []interface{}{int64(4)}, SeqNo: 6, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(4)}}},
	{Type: "insert", Key: []interface{}{int64(5)}, SeqNo: 7, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(5)}}},
	{Type: "insert", Key: []interface{}{int64(6)}, SeqNo: 8, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(6)}}},
	{Type: "delete", Key: []interface{}{int64(2)}, SeqNo: 9, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(9)}, SeqNo: 10, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(9)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 11, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "text"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int(11)"}}},
	//{Type: "insert", Key: []interface{}{45676.0}, SeqNo: 12.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 45676.0}, {Name: "f3", Value: "ggg"}, {Name: "f4", Value: "dHR0"}, {Name: "f5", Value: "eXl5"}, {Name: "f6", Value: "vvv"}, {Name: "f7", Value: 7543.0}}},
	{Type: "insert", Key: []interface{}{int64(45676)}, SeqNo: 12, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(45676)}, {Name: "f3", Value: "ggg"}, {Name: "f4", Value: []byte{116, 116, 116}}, {Name: "f5", Value: []byte{121, 121, 121}}, {Name: "f6", Value: "vvv"}, {Name: "f7", Value: int32(7543)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 13, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "text"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int(11)"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 14, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "varchar(20)"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int(11)"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 15, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 16, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}}},
}

var testMultiTablePrepare = []string{
	"drop database if exists db1",
	"create database if not exists db1",
	"drop database if exists db9",
	"create database if not exists db9",

	`create table db1.t1 (
		f1 bigint not null primary key
	)`,
	`create table db1.t2 (
		f1 bigint not null primary key
	)`,
	`create table db9.t1 (
		f1 bigint not null primary key
	)`,
	`create table db9.t2 (
		f1 bigint not null primary key
	)`,
}

var testMultiTable = []string{
	"insert into db1.t1(f1) values (7)",
	"insert into db1.t2(f1) values (8)",
	"insert into db9.t1(f1) values (9)",
	"insert into db9.t2(f1) values (10)",
	"update db1.t1 set f1=f1+10 where f1=7",
	"update db9.t1 set f1=f1+10 where f1=9",
}

var testMultiTableResult1 = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{int64(7)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(7)}}},
	//{"delete", []interface{}{7.0}, 3.0, nil},
	//{"insert", []interface{}{17.0}, 4.0, &[]types.CommonFormatField{{"f1", 17.0}}},
}

var testMultiTableResult2 = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{int64(9)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(9)}}},
	//{"delete", []interface{}{9.0}, 5.0, nil},
	//{"insert", []interface{}{19.0}, 7.0, &[]types.CommonFormatField{{"f1", 19.0}}},
}

var testShutdownPrepare = []string{
	"drop database if exists db1",
	"create database if not exists db1",

	`create table db1.t1 (
		f1 bigint not null primary key
	)`,
}

func worker(cfg *config.AppConfig, bp pipe.Pipe, m *map[string]pipe.Pipe, tpool pool.Thread, t *testing.T) {
	defer shutdown.Done()

	var err error
	log.Debugf("Starting binlog reader in test")
	testReader = &mysqlReader{ctx: shutdown.Context, tpool: tpool, bufPipe: bp, outPipes: m}
	test.CheckFail(err, t)

	if !testReader.Worker() {
		t.FailNow()
	}

	log.Debugf("Finished binlog worker in test")
}

func ExecSQL(db *sql.DB, t *testing.T, query string) {
	test.CheckFail(util.ExecSQL(db, query), t)
}

func regTableForMultiTableTest(pipeType string, secondPipe string, t *testing.T) {
	if testName == "TestMultiTable" {
		cfg.ChangelogBuffer = false
		if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db9"}, "t1", "mysql", pipeType, 0) {
			t.FailNow()
		}
		if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db9"}, "t1", "mysql", pipeType, 1) {
			t.FailNow()
		}
		if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db9"}, "t1", "mysql", secondPipe, 1) {
			t.FailNow()
		}
		if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db9"}, "t1", "not_mysql", pipeType, 1) {
			t.FailNow()
		}
	}
}

func Prepare(pipeType string, create []string, encoding string, t *testing.T) (*sql.DB, pipe.Pipe, pipe.Pipe) {
	shutdown.Setup()

	if testName == "TestMultiTable" {
		cfg.OutputTopicNameFormat = "hp-tap-%s-%s-%s-v%d"
		cfg.ChangelogTopicNameFormat = types.MySvcName + ".service.%s.db.%s.table.%s-v%d"
		cfg.OutputFormat = encoding
	}

	cfg.InternalEncoding = encoding
	cfg.ChangelogOutputFormat = encoding
	var err error
	encoder.Internal, err = encoder.InitEncoder(cfg.InternalEncoding, "", "", "")
	test.CheckFail(err, t)

	log.Debugf("Test encoding: %+v", encoding)

	dbc, err := db.OpenService(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1"}, "")
	test.CheckFail(err, t)

	ExecSQL(dbc, t, "RESET MASTER")
	ExecSQL(dbc, t, "SET GLOBAL binlog_format = 'ROW'")
	ExecSQL(dbc, t, "SET GLOBAL server_id=1")
	ExecSQL(dbc, t, "DROP TABLE IF EXISTS "+types.MyDbName+".state")
	ExecSQL(dbc, t, "DROP TABLE IF EXISTS "+types.MyDbName+".columns")

	if !state.Init(cfg) {
		t.FailNow()
	}

	ExecSQL(state.GetDB(), t, "DROP TABLE IF EXISTS kafka_offsets")

	log.Debugf("Preparing database")
	for _, s := range create {
		ExecSQL(dbc, t, s)
	}

	p1, err := pipe.Create(shutdown.Context, pipeType, 16, cfg, state.GetDB())
	test.CheckFail(err, t)
	secondPipe := "local"
	if pipeType == "local" {
		secondPipe = "kafka"
	}
	p2, err := pipe.Create(shutdown.Context, secondPipe, 16, cfg, state.GetDB())
	test.CheckFail(err, t)

	log.Debugf("Starting binlog reader. PipeType=%v", pipeType)

	if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db1"}, "t1", "mysql", pipeType, 0) {
		t.FailNow()
	}

	regTableForMultiTableTest(pipeType, secondPipe, t)

	fakePool = pool.Create()
	globalTPoolProcs = 0

	fakePool.Start(0, func() {
		atomic.AddInt32(&globalTPoolProcs, 1)
		tickCh := time.NewTicker(time.Millisecond * 50).C
		for {
			select {
			case <-shutdown.InitiatedCh():
				return
			case <-tickCh:
				if fakePool.Terminate() {
					return
				}
			}
		}
	})

	shutdown.Register(1)

	m := make(map[string]pipe.Pipe)
	m[p1.Type()] = p1
	m[p2.Type()] = p2

	go worker(cfg, p1, &m, fakePool, t)

	/* Let binlog reader to initialize */
	for {
		g, err := state.GetGTID(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db1"})
		test.CheckFail(err, t)
		if g != "" {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(50))
	}

	return dbc, p1, p2
}

/*
func CheckBinlogFormat(t *testing.T) {
	shutdown.Setup()
	log.Debugf("TestBinlogFormat start")
	dbc, err := db.OpenService(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: ""}, "")
	test.CheckFail(err, t)
	defer func() { test.CheckFail(dbc.Close(), t) }()
	ExecSQL(dbc, t, "SET GLOBAL binlog_format = 'STATEMENT'")
	for _, s := range testBasicPrepare {
		ExecSQL(dbc, t, s)
	}
	if !state.Init(cfg) {
		t.FailNow()
	}
	defer func() { test.CheckFail(state.Close(), t) }()
	if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db1"}, "t1") {
		t.FailNow()
	}
	p := pipe.Create("local", 16, cfg, state.GetDB(), shutdown.Context)

	tpool := pool.Create()

	go func() {
		shutdown.Register(1)
		//FIXME: How to really test the Reader returned bacause of row format?
		//return "error" from reader?
		if !Worker(cfg, p, tpool, shutdown.Context) {
			log.Errorf("Binlog return false")
			t.Fail()
		}
		shutdown.Done()
	}()

	time.Sleep(time.Second * time.Duration(2))
	if shutdown.NumProcs() == 1 {
		log.Errorf("Binlog reader still running")
		t.Fail()
	}
	ExecSQL(dbc, t, "SET GLOBAL binlog_format = 'ROW'")
	shutdown.Initiate()
	shutdown.Wait()
}
*/

func initConsumeTableEvents(p pipe.Pipe, db string, table string, version int, t *testing.T) pipe.Consumer {
	tn := config.GetTopicName(cfg.ChangelogTopicNameFormat, "test_svc1", db, table, version)
	if !cfg.ChangelogBuffer {
		tn = config.GetTopicName(cfg.OutputTopicNameFormat, "test_svc1", db, table, version)
	}
	pc, err := p.NewConsumer(tn)
	test.CheckFail(err, t)
	log.Debugf("Start event consumer from: " + tn)
	return pc
}

func consumeTableEvents(pc pipe.Consumer, db string, table string, result []types.CommonFormatEvent, seqnoShift uint64, t *testing.T) {
	log.Debugf("consuming events %+v %+v", db, table)
	enc, err := encoder.Create(cfg.ChangelogOutputFormat, "test_svc1", db, table)
	test.CheckFail(err, t)
	if !cfg.ChangelogBuffer {
		enc, err = encoder.Create(cfg.OutputFormat, "test_svc1", db, table)
		test.CheckFail(err, t)
	}

	for i, v := range result {
		if !pc.FetchNext() {
			break
		}
		b, err := pc.Pop()
		test.CheckFail(err, t)
		if b == nil {
			t.Fatalf("No empty msg allowed")
		}

		var cf *types.CommonFormatEvent
		switch m := b.(type) {
		case *types.RowMessage:
			b, err = enc.Row(m.Type, m.Data, m.SeqNo)
			test.CheckFail(err, t)
			cf, err = enc.DecodeEvent(b.([]byte))
			test.CheckFail(err, t)
		case []byte:
			cf = &types.CommonFormatEvent{}
			if !cfg.ChangelogBuffer {
				cf, err = enc.DecodeEvent(b.([]byte))
				test.CheckFail(err, t)
			} else {
				_, err := enc.UnwrapEvent(b.([]byte), cf)
				test.CheckFail(err, t)

				if cf.Type == "schema" {
					log.Debugf("alterch receive %v", cf)
					alterCh <- true
				}
			}
		}

		cf.SeqNo -= 1000000 + seqnoShift
		cf.Timestamp = 0
		if !reflect.DeepEqual(*cf, v) {
			log.Errorf("Received: %+v %+v", cf, cf.Fields)
			log.Errorf("Reference: %+v %+v", &v, v.Fields)
			t.FailNow()
		} else {
			log.Infof("Successfully matched: i=%v %+v Fields=%v", i, cf, cf.Fields)
		}
		if cf.Type == "schema" {
			err := enc.UpdateCodec()
			if log.E(err) {
				t.FailNow()
			}
		}
	}
}

/*
//TODO: 1.8 export the t.Name() so no hack is needed
func testName(t *testing.T) string {
	v := reflect.ValueOf(*t)
	name := v.FieldByName("name")
	return name.String()
}
*/

func checkPoolControl(pipeType string, testName string, t *testing.T) {
	if pipeType == "local" {
		if (testName == "TestMultiTable" && globalTPoolProcs != 5) || (testName != "TestMultiTable" && globalTPoolProcs != 2) {
			t.Fatalf("Binlog reader should control number of streamers num=%v, pipe=%v", globalTPoolProcs, pipeType)
		}
	} else if globalTPoolProcs != 0 {
		t.Errorf("Binlog reader shouldn't control number of streamers num=%v pipe=%v", globalTPoolProcs, pipeType)
		t.Fail()
	}
}

func CheckQueries(pipeType string, prepare []string, queries []string, result []types.CommonFormatEvent, encoding string, t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)
	if pipeType == "kafka" {
		test.SkipIfNoKafkaAvailable(t)
	}

	dbc, p1, p2 := Prepare(pipeType, prepare, encoding, t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	log.Debugf("Starting consumers")

	initCh := make(chan bool)
	shutdown.Register(1)
	go func() {
		defer shutdown.Done()
		pc := initConsumeTableEvents(p1, "db1", "t1", 0, t)
		var pc1, pc2, pc3 pipe.Consumer
		if testName == "TestMultiTable" {
			pc1 = initConsumeTableEvents(p1, "db9", "t1", 0, t)
			pc2 = initConsumeTableEvents(p1, "db9", "t1", 1, t)
			pc3 = initConsumeTableEvents(p2, "db9", "t1", 1, t)
		}
		log.Debugf("Signal that all consumer has been initialized")
		initCh <- true
		consumeTableEvents(pc, "db1", "t1", result, 0, t)
		if testName == "TestMultiTable" {
			consumeTableEvents(pc1, "db9", "t1", testMultiTableResult2, 0, t)
			consumeTableEvents(pc2, "db9", "t1", testMultiTableResult2, 1, t)
			consumeTableEvents(pc3, "db9", "t1", testMultiTableResult2, 2, t)
		}
		log.Debugf("Signal that we finished consuming events")
		initCh <- false
		log.Debugf("Finished consumers")
	}()

	log.Debugf("Waiting pipe consumers to initialize")
	<-initCh
	log.Debugf("Starting workload")

	usedb := 0
	for _, s := range queries {
		ExecSQL(dbc, t, s)
		if strings.ToLower(s) == "use db1" {
			usedb = 1
		}
		if strings.ToLower(s) == "use db2" {
			usedb = 2
		}
		//HACK: This logic depends from content of queries arrays
		if strings.Contains(strings.ToLower(s), "alter") && !strings.Contains(s, "db2.") && usedb != 2 {
			log.Debugf("alterch wait %v", s)
			<-alterCh
			log.Debugf("alterch afterwait %v", s)
		}
	}

	log.Debugf("Waiting consumer to consume all events and finish")
	<-initCh
	log.Debugf("Finishing test")

	shutdown.Initiate()
	shutdown.Wait()

	checkPoolControl(pipeType, testName, t)

	*cfg = saveCfg

	log.Debugf("Finished test")
}

func inVersionArray(a []*table, output string, version int) bool {
	j := 0
	for ; j < len(a) && (a[j].version != version || a[j].output != output); j++ {
	}
	return j < len(a)
}

func TestMultiVersionsArray(t *testing.T) {
	testName = "TestMultiTable"

	//The content of this array predetermined by hardcoded RegisterTable(s) in
	//Prepare()
	tests := []struct {
		db      string
		t       string
		input   string
		output  string
		version int
		real    int
	}{
		{db: "db1", t: "t1", input: "mysql", output: "local", version: 0, real: 1},     //1
		{db: "db9", t: "t1", input: "mysql", output: "local", version: 0, real: 1},     //2
		{db: "db9", t: "t1", input: "mysql", output: "local", version: 1, real: 1},     //3
		{db: "db9", t: "t1", input: "mysql", output: "kafka", version: 1, real: 1},     //4
		{db: "db9", t: "t1", input: "not_mysql", output: "local", version: 1, real: 0}, //not managed by binlog reader
	}

	save := cfg.StateUpdateTimeout
	cfg.StateUpdateTimeout = 1

	dbc, _, _ := Prepare("local", testMultiTablePrepare, "json", t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	n := 0
	for i := 0; i < len(tests); i++ {
		if !test.WaitForNumProc(int32(len(tests)-n+2), 80*200) {
			t.Fatalf("Binlog reader didn't finish int %v secs. NumProcs: %v", 80*50/1000, shutdown.NumProcs())
		}

		var dlen, tlen = 0, 0
		if tests[i].real != 0 {
			test.Assert(t, testReader.tables[tests[i].db] != nil, "Db not found in map: %v", tests[i].db)
			dlen = len(testReader.tables[tests[i].db])
			a := testReader.tables[tests[i].db][tests[i].t]
			test.Assert(t, a != nil, "Table not found in map: %v", tests[i].t)
			tlen = len(a)
			test.Assert(t, inVersionArray(a, tests[i].output, tests[i].version), "Not found in map %v %v %v %v", tests[i].db, tests[i].t, tests[i].output, tests[i].version)
		}

		n += tests[i].real

		if !state.DeregisterTable("test_svc1", tests[i].db, tests[i].t, tests[i].input, tests[i].output, tests[i].version) {
			t.Fatalf("Failed to deregister table")
		}

		if !test.WaitForNumProc(int32(len(tests)-n+2), 80*200) {
			t.Fatalf("Binlog reader didn't finish int %v secs. NumProcs: %v", 80*50/1000, shutdown.NumProcs())
		}

		//Check that table has been properly removed from the map and version array.
		if tests[i].real != 0 {
			if tlen > 1 {
				test.Assert(t, testReader.tables[tests[i].db] != nil, "Db not found in map: %v", tests[i].db)
				a := testReader.tables[tests[i].db][tests[i].t]
				test.Assert(t, a != nil, "Table not found in map: %v", tests[i].t)

				test.Assert(t, !inVersionArray(a, tests[i].output, tests[i].version), "Found in map %v %v %v %v", tests[i].db, tests[i].t, tests[i].output, tests[i].version)
			} else {
				if dlen > 1 {
					test.Assert(t, testReader.tables[tests[i].db] != nil, "Db not found in map: %v", tests[i].db)
					a := testReader.tables[tests[i].db][tests[i].t]
					test.Assert(t, a == nil, "Table found in map: %v", tests[i].t)
				} else {
					test.Assert(t, testReader.tables[tests[i].db] == nil, "Db found in map: %v", tests[i].db)
				}
			}
		}
	}

	fakePool.Adjust(0)
	//so as we removed all the tables, we expect binlog reader to terminate
	if !test.WaitForNumProc(1, 80*200) { // 1 remaining thread is signal handler
		t.Fatalf("Binlog reader didn't finish int %v secs. NumProcs: %v", 80*50/1000, shutdown.NumProcs())
	}

	shutdown.Initiate()
	shutdown.Wait()

	cfg.StateUpdateTimeout = save

	testName = ""
}

func TestBasic(t *testing.T) {
	CheckQueries("local", testBasicPrepare, testBasic, testBasicResult, "json", t)
}

func TestUseDB(t *testing.T) {
	CheckQueries("local", testBasicPrepare, testUseDB, testUseDBResult, "json", t)
}

func TestMultiColumn(t *testing.T) {
	CheckQueries("local", testMultiColumnPrepare, testMultiColumn, testMultiColumnResult, "json", t)
}

func TestMultiRow(t *testing.T) {
	CheckQueries("local", testMultiColumnPrepare, testMultiRow, testMultiRowResult, "json", t)
}

func TestCompoundKey(t *testing.T) {
	CheckQueries("local", testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult, "json", t)
}

func TestDDL(t *testing.T) {
	CheckQueries("local", testDDLPrepare, testDDL, testDDLResult, "json", t)
}

func TestMultiTable(t *testing.T) {
	testName = "TestMultiTable"
	CheckQueries("local", testMultiTablePrepare, testMultiTable, testMultiTableResult1, "json", t)
	testName = ""
}

func TestDirectOutput(t *testing.T) {
	cfg.ChangelogBuffer = false
	cfg.OutputFormat = "msgpack" //set to different from "json" to check that reader output in final format and not in buffer format
	CheckQueries("kafka", testBasicPrepare, testBasic, testBasicResult, "json", t)
}

func TestReaderShutdown(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	save := cfg.StateUpdateTimeout
	cfg.StateUpdateTimeout = 1

	dbc, _, _ := Prepare("local", testShutdownPrepare, "json", t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	if !state.DeregisterTable("test_svc1", "db1", "t1", "mysql", "local", 0) {
		t.Fatalf("Failed to deregister table")
	}

	if !test.WaitForNumProc(2, 80*200) {
		t.Fatalf("Binlog reader didn't finish int %v secs", 80*50/1000)
	}

	fakePool.Adjust(0)
	log.Debugf("adjusted pool to 0")

	if !test.WaitForNumProc(1, 80*200) {
		t.Fatalf("Binlog reader didn't finish int %v secs. NumProcs: %v", 80*50/1000, shutdown.NumProcs())
	}

	cfg.StateUpdateTimeout = save

	shutdown.Initiate()
	shutdown.Wait()
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	cfg.ChangelogOutputFormat = encoder.Internal.Type()
	cfg.MaxNumProcs = 1
	saveCfg = *cfg

	pipe.InitialOffset = sarama.OffsetNewest
	pipe.KafkaConfig = sarama.NewConfig()
	pipe.KafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	pipe.KafkaConfig.Producer.Return.Successes = true
	pipe.KafkaConfig.Consumer.MaxWaitTime = 10 * time.Millisecond

	log.Debugf("Config loaded %v", cfg)
	os.Exit(m.Run())
	log.Debugf("Starting shutdown")
}
