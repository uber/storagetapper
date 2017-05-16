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

package binlog

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
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
var globalTPoolProcs int32
var fakePool pool.Thread

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
	{Type: "insert", Key: []interface{}{1.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 1.0}}},
	{Type: "insert", Key: []interface{}{2.0}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 2.0}}},
	{Type: "delete", Key: []interface{}{2.0}, SeqNo: 3.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{12.0}, SeqNo: 4.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 12.0}}},
	{Type: "delete", Key: []interface{}{1.0}, SeqNo: 5.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{3.0}, SeqNo: 6.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 3.0}}},
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
	{Type: "insert", Key: []interface{}{4.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 4.0}}},
	{Type: "insert", Key: []interface{}{5.0}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 5.0}}},
	{Type: "insert", Key: []interface{}{6.0}, SeqNo: 3.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 6.0}}},
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
	{Type: "insert", Key: []interface{}{7.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 7.0}, {Name: "f2", Value: 2.0}, {Name: "f3", Value: 3.0}}},
	{Type: "insert", Key: []interface{}{8.0}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 8.0}, {Name: "f2", Value: 2.0}, {Name: "f3", Value: 4.0}}},
	{Type: "insert", Key: []interface{}{9.0}, SeqNo: 3.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 9.0}, {Name: "f2", Value: 2.0}, {Name: "f3", Value: nil}}},
	{Type: "insert", Key: []interface{}{10.0}, SeqNo: 4.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 10.0}, {Name: "f2", Value: nil}, {Name: "f3", Value: 2.0}}},
	{Type: "delete", Key: []interface{}{7.0}, SeqNo: 5.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{17.0}, SeqNo: 6.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 17.0}, {Name: "f2", Value: 1.0}, {Name: "f3", Value: 3.0}}},
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
	{Type: "insert", Key: []interface{}{100.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 100.0}, {Name: "f2", Value: 101.0}, {Name: "f3", Value: 102.0}}},
	{Type: "insert", Key: []interface{}{110.0}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 110.0}, {Name: "f2", Value: 111.0}, {Name: "f3", Value: 112.0}}},
	{Type: "insert", Key: []interface{}{120.0}, SeqNo: 3.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 120.0}, {Name: "f2", Value: 121.0}, {Name: "f3", Value: 122.0}}},
	//"update db1.t1 set f1=f1+11, f3=f3+1 where f2=2"
	{Type: "delete", Key: []interface{}{100.0}, SeqNo: 4.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{111.0}, SeqNo: 5.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 111.0}, {Name: "f2", Value: 101.0}, {Name: "f3", Value: 103.0}}},
	{Type: "delete", Key: []interface{}{110.0}, SeqNo: 6.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{121.0}, SeqNo: 7.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 121.0}, {Name: "f2", Value: 111.0}, {Name: "f3", Value: 113.0}}},
	//"delete from db1.t1 where f1 >= 100"
	{Type: "delete", Key: []interface{}{120.0}, SeqNo: 8.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{131.0}, SeqNo: 9.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 131.0}, {Name: "f2", Value: 121.0}, {Name: "f3", Value: 123.0}}},
	{Type: "delete", Key: []interface{}{111.0}, SeqNo: 10.0, Timestamp: 0, Fields: nil},
	{Type: "delete", Key: []interface{}{121.0}, SeqNo: 11.0, Timestamp: 0, Fields: nil},
	{Type: "delete", Key: []interface{}{131.0}, SeqNo: 12.0, Timestamp: 0, Fields: nil},
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
	{Type: "insert", Key: []interface{}{1.0, "aa aa"}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 1.0}, {Name: "f2", Value: "aa aa"}}},
	{Type: "insert", Key: []interface{}{2.0, "bbb"}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 2.0}, {Name: "f2", Value: "bbb"}}},
	{Type: "delete", Key: []interface{}{2.0, "bbb"}, SeqNo: 3.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{12.0, "bbb"}, SeqNo: 4.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 12.0}, {Name: "f2", Value: "bbb"}}},
	{Type: "delete", Key: []interface{}{1.0, "aa aa"}, SeqNo: 5.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{3.0, "aaa"}, SeqNo: 6.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 3.0}, {Name: "f2", Value: "aaa"}}},
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

var testDDL = []string{
	"insert into db1.t1 value (1)",
	"insert into db1.t1 value (2)",
	`	 alter table 
	db1.t1 add 	f2 
	varchar(32) 	`,
	"alter table db2.t1 add f2 varchar(32)",
	"insert into db1.t1 value (3, 'aaa')",
	"use db1",
	"alter table t1 drop f2",
	"insert into t1 value (4)",
	"insert into db1.t1 value (5)",
	"use db2",
	"insert into db2.t1 value (7, 'eee')",
	"alter table t1 drop f2",
	"insert into db1.t1 value (6)",
	"update t1 set f1=f1+1 where f1=2",
	"insert into db2.t1 value (8)",
	"use db1",
	"update t1 set f1=f1+7 where f1=2",
	"alter table t1 add f3 varchar(128), add f4 text, add f5 blob, add f6 varchar(32), add f7 int",
	"insert into db1.t1 value (45676, 'ggg', 'ttt', 'yyy', 'vvv', 7543)",
	"alter table t1 add index(f3,f6,f7)",
	"ALTER TABLE t1 MODIFY f4 bigint(20)",
	/*Test names in backticks */
	"ALTER TABLE `t1` drop f7, drop f6, drop f5, drop f4",
	"ALTER TABLE `db1`.`t1` drop f3",
}

var testDDLResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{1.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 1.0}}},
	{Type: "insert", Key: []interface{}{2.0}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 2.0}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 3.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f2", Value: "varchar(32)"}}},
	{Type: "insert", Key: []interface{}{3.0}, SeqNo: 4.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 3.0}, {Name: "f2", Value: "aaa"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 5.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}}},
	{Type: "insert", Key: []interface{}{4.0}, SeqNo: 6.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 4.0}}},
	{Type: "insert", Key: []interface{}{5.0}, SeqNo: 7.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 5.0}}},
	{Type: "insert", Key: []interface{}{6.0}, SeqNo: 8.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 6.0}}},
	{Type: "delete", Key: []interface{}{2.0}, SeqNo: 9.0, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{9.0}, SeqNo: 10.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 9.0}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 11.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "text"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int(11)"}}},
	{Type: "insert", Key: []interface{}{45676.0}, SeqNo: 12.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 45676.0}, {Name: "f3", Value: "ggg"}, {Name: "f4", Value: "dHR0"}, {Name: "f5", Value: "eXl5"}, {Name: "f6", Value: "vvv"}, {Name: "f7", Value: 7543.0}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 13.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "text"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int(11)"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 14.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "bigint(20)"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int(11)"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 15.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}, {Name: "f3", Value: "varchar(128)"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 16.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint(20)"}}},
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
	{Type: "insert", Key: []interface{}{7.0}, SeqNo: 1.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 7.0}}},
	//{"delete", []interface{}{7.0}, 3.0, nil},
	//{"insert", []interface{}{17.0}, 4.0, &[]types.CommonFormatField{{"f1", 17.0}}},
}

var testMultiTableResult2 = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{9.0}, SeqNo: 2.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 9.0}}},
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

func worker(cfg *config.AppConfig, p pipe.Pipe, tpool pool.Thread, t *testing.T) {
	defer shutdown.Done()

	log.Debugf("Starting binlog reader in test")
	if !Worker(shutdown.Context, cfg, p, tpool) {
		t.FailNow()
	}
	log.Debugf("Finished binlog worker in test")
}

func ExecSQL(db *sql.DB, t *testing.T, query string) {
	test.CheckFail(util.ExecSQL(db, query), t)
}

func Prepare(pipeType int, create []string, t *testing.T) (*sql.DB, pipe.Pipe) {
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

	p := pipe.Create(shutdown.Context, pipeType, 16, cfg, state.GetDB())

	if pipeType == pipe.Kafka {
		//FIXME: Rewrite test so it doesn't require events to come out inorder
		//Configure producer so as everything will go to one partition
		pipe.InitialOffset = sarama.OffsetNewest
		pk := p.(*pipe.KafkaPipe)
		pk.Config = sarama.NewConfig()
		pk.Config.Producer.Partitioner = sarama.NewManualPartitioner
		pk.Config.Producer.Return.Successes = true
	}

	log.Debugf("Starting binlog reader")

	if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db1"}, "t1", "mysql", "") {
		t.FailNow()
	}

	if testName == "TestMultiTable" {
		if !state.RegisterTable(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db9"}, "t1", "mysql", "") {
			t.FailNow()
		}
	}

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
	go worker(cfg, p, fakePool, t)

	/* Let binlog reader to initialize */
	for {
		g, err := state.GetGTID(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db1"})
		test.CheckFail(err, t)
		if g != "" {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(50))
	}

	return dbc, p
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
	p := pipe.Create(pipe.Local, 16, cfg, state.GetDB(), shutdown.Context)

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

func initConsumeTableEvents(p pipe.Pipe, db string, table string, t *testing.T) pipe.Consumer {
	tn := types.MySvcName + ".service.test_svc1.db." + db + ".table." + table
	pc, err := p.RegisterConsumer(tn)
	test.CheckFail(err, t)
	log.Debugf("Start event consumer from: " + tn)
	return pc
}

func consumeTableEvents(pc pipe.Consumer, db string, table string, result []types.CommonFormatEvent, t *testing.T) {
	enc, err := encoder.Create("json", "test_svc1", db, table)
	test.CheckFail(err, t)

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
			cf, err = encoder.CommonFormatDecode(b.([]byte))
			test.CheckFail(err, t)
		case []byte:
			buf := bytes.NewBuffer(b.([]byte))

			cf = &types.CommonFormatEvent{}
			dec := json.NewDecoder(buf)
			err = dec.Decode(&cf)
			test.CheckFail(err, t)

			if cf.Type != "schema" {
				_, err = buf.ReadFrom(dec.Buffered())
				test.CheckFail(err, t)
				cf, err = encoder.CommonFormatDecode(buf.Bytes())
				test.CheckFail(err, t)
			}
		}

		cf.SeqNo -= 1000000
		cf.Timestamp = 0
		if !reflect.DeepEqual(*cf, v) {
			log.Errorf("Received: %+v %+v", cf, cf.Fields)
			log.Errorf("Reference: %+v %+v", &v, v.Fields)
			t.Fail()
			break
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

func CheckQueries(pipeType int, prepare []string, queries []string, result []types.CommonFormatEvent, t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)
	if pipeType == pipe.Kafka {
		test.SkipIfNoKafkaAvailable(t)
	}

	shutdown.Setup()

	dbc, p := Prepare(pipeType, prepare, t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	log.Debugf("Starting consumers")

	initCh := make(chan bool)
	shutdown.Register(1)
	go func() {
		defer shutdown.Done()
		pc := initConsumeTableEvents(p, "db1", "t1", t)
		var pc1 pipe.Consumer
		if testName == "TestMultiTable" {
			pc1 = initConsumeTableEvents(p, "db9", "t1", t)
		}
		initCh <- true
		consumeTableEvents(pc, "db1", "t1", result, t)
		if testName == "TestMultiTable" {
			consumeTableEvents(pc1, "db9", "t1", testMultiTableResult2, t)
		}
		initCh <- false
		log.Debugf("Finished consumers")
	}()

	<-initCh

	log.Debugf("Starting workload")

	for _, s := range queries {
		ExecSQL(dbc, t, s)
	}

	<-initCh

	log.Debugf("Finishing test")

	shutdown.Initiate()
	shutdown.Wait()

	if pipeType == pipe.Local {
		if (testName == "TestMultiTable" && globalTPoolProcs != 3) || (testName != "TestMultiTable" && globalTPoolProcs != 2) {
			t.Errorf("Binlog reader should control number of streamers num=%v, pipe=%v", globalTPoolProcs, pipeType)
			t.Fail()
		}
	} else if globalTPoolProcs != 0 {
		t.Errorf("Binlog reader shouldn't control number of streamers num=%v pipe=%v", globalTPoolProcs, pipeType)
		t.Fail()
	}

	log.Debugf("Finished test")
}

func TestBasic(t *testing.T) {
	CheckQueries(pipe.Local, testBasicPrepare, testBasic, testBasicResult, t)
}

func TestUseDB(t *testing.T) {
	CheckQueries(pipe.Local, testBasicPrepare, testUseDB, testUseDBResult, t)
}

func TestMultiColumn(t *testing.T) {
	CheckQueries(pipe.Local, testMultiColumnPrepare, testMultiColumn, testMultiColumnResult, t)
}

func TestMultiRow(t *testing.T) {
	CheckQueries(pipe.Local, testMultiColumnPrepare, testMultiRow, testMultiRowResult, t)
}

func TestCompoundKey(t *testing.T) {
	CheckQueries(pipe.Local, testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult, t)
}

func TestDDL(t *testing.T) {
	CheckQueries(pipe.Local, testDDLPrepare, testDDL, testDDLResult, t)
}

func TestMultiTable(t *testing.T) {
	testName = "TestMultiTable"
	CheckQueries(pipe.Local, testMultiTablePrepare, testMultiTable, testMultiTableResult1, t)
	testName = ""
}

func TestReaderShutdown(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	shutdown.Setup()

	save := cfg.StateUpdateTimeout
	cfg.StateUpdateTimeout = 1

	dbc, _ := Prepare(pipe.Local, testShutdownPrepare, t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	if !state.DeregisterTable("test_svc1", "db1", "t1") {
		t.Fatalf("Failed to deregister table")
	}

	if !test.WaitForNumProc(3, 80*200) {
		t.Fatalf("Binlog reader didn't finish int %v secs", 80*50/1000)
	}

	fakePool.Adjust(0)
	log.Debugf("adjusted pool to 0")

	if !test.WaitForNumProc(1, 80*200) {
		t.Fatalf("Binlog reader didn't finish int %v secs", 80*50/1000)
	}

	cfg.StateUpdateTimeout = save

	shutdown.Initiate()
	shutdown.Wait()
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	cfg.ReaderOutputFormat = "json"
	cfg.MaxNumProcs = 1
	log.Debugf("Config loaded %v", cfg)
	os.Exit(m.Run())
	log.Debugf("Starting shutdown")
}
