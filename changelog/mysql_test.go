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
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
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
var outputFormat string

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

/* Test basic insert, update, delete after graceful restart */
var testRestart = []string{
	"insert into db1.t1(f1) value (100)",
	"insert into db1.t1(f1) value (200)",
	"update db1.t1 set f1=f1+10 where f1=200",
	"delete from db1.t1 where f1=100",
	"insert into db1.t1(f1) value (300)",
}

var testRestartResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{int64(100)}, SeqNo: 1000007, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(100)}}},
	{Type: "insert", Key: []interface{}{int64(200)}, SeqNo: 1000008, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(200)}}},
	{Type: "delete", Key: []interface{}{int64(200)}, SeqNo: 1000009, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(210)}, SeqNo: 1000010, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(210)}}},
	{Type: "delete", Key: []interface{}{int64(100)}, SeqNo: 1000011, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(300)}, SeqNo: 1000012, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(300)}}},
}

//Alter alter is failure injected should fail first time
//but retried after restart
var testAlterFailure = []string{
	"alter table db1.t1 add f2 bigint",
}

//So as alter insert with new schema should succeed
var testAlterFailureRestarted = []string{
	"/* alterch_wait */ insert into db1.t1(f1,f2) value (200, 300)",
}

var testAlterFailureResult = []types.CommonFormatEvent{
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 1000001, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}, {Name: "f2", Value: "bigint"}}},
	{Type: "insert", Key: []interface{}{int64(200)}, SeqNo: 1000002, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(200)}, {Name: "f2", Value: int64(300)}}},
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

// alterch_wait in the comment allows to synchronizes producer and consumer,
// meaning that producer wait till consumer sees the alter table event, before
// performing next schema change
var testDDL = []string{
	"insert into db2.t1 value (1),(2),(3),(4),(5),(6),(9),(45676)", //to satisfy fk1 foreign key
	"insert into db1.t1 value (1)",
	"insert into db1.t1 value (2)",
	`	 alter table 
	db1.t1 add 	f2 
	varchar(32) 	`,
	"/* alterch_wait */ alter table db2.t1 add f2 varchar(32)", //we don't track db2 so this should not affect db1.t1
	"insert into db1.t1 value (3, 'aaa')",
	"use db1",
	"/* alterch_wait */ alter table t1 drop f2",
	"alter table t1 add constraint fk1 foreign key (f1) references db2.t1(f1)",
	"insert into t1 value (4)",
	"insert into db1.t1 value (5)",
	"use db2",
	"insert into db2.t1 value (7, 'eee')",
	"update t1 set f2='uuu' where f1=2",
	"alter table t1 drop f2", // this is db2 table. should be skipped as well
	"insert into db1.t1 value (6)",
	"insert into db2.t1 value (8)",
	"use db1",
	"update t1 set f1=f1+7 where f1=2",
	"/* alterch_wait */ alter table t1 add f3 varchar(128), add f4 text, add f5 blob, add f6 varchar(32), add f7 int",
	"insert into db1.t1 value (45676, 'ggg', 'ttt', 'yyy', 'vvv', 7543)",
	"/* alterch_wait */ alter table t1 add index(f3,f6,f7)",
	"/* alterch_wait */ ALTER TABLE t1 MODIFY f4 varchar(20)",
	"alter table db1.t1 drop foreign key fk1",
	"alter table t1 add foreign key (f1) references db2.t1(f1), add index (f7)",
	/*Test names in backticks */
	"/* alterch_wait */ ALTER TABLE `t1` drop f7, drop f6, drop f5, drop f4",
	"/* alterch_wait */ ALTER TABLE `db1`.`t1` drop f3",
}

var testDDLResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{int64(1)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1)}}},
	{Type: "insert", Key: []interface{}{int64(2)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(2)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}, {Name: "f2", Value: "varchar(32)"}}},
	{Type: "insert", Key: []interface{}{int64(3)}, SeqNo: 4, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(3)}, {Name: "f2", Value: "aaa"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 5, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}}},
	{Type: "insert", Key: []interface{}{int64(4)}, SeqNo: 6, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(4)}}},
	{Type: "insert", Key: []interface{}{int64(5)}, SeqNo: 7, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(5)}}},
	{Type: "insert", Key: []interface{}{int64(6)}, SeqNo: 8, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(6)}}},
	{Type: "delete", Key: []interface{}{int64(2)}, SeqNo: 9, Timestamp: 0, Fields: nil},
	{Type: "insert", Key: []interface{}{int64(9)}, SeqNo: 10, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(9)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 11, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "text"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int"}}},
	//{Type: "insert", Key: []interface{}{45676.0}, SeqNo: 12.0, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: 45676.0}, {Name: "f3", Value: "ggg"}, {Name: "f4", Value: "dHR0"}, {Name: "f5", Value: "eXl5"}, {Name: "f6", Value: "vvv"}, {Name: "f7", Value: 7543.0}}},
	{Type: "insert", Key: []interface{}{int64(45676)}, SeqNo: 12, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(45676)}, {Name: "f3", Value: "ggg"}, {Name: "f4", Value: "ttt"}, {Name: "f5", Value: []byte{121, 121, 121}}, {Name: "f6", Value: "vvv"}, {Name: "f7", Value: int32(7543)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 13, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "text"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 14, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}, {Name: "f3", Value: "varchar(128)"}, {Name: "f4", Value: "varchar(20)"}, {Name: "f5", Value: "blob"}, {Name: "f6", Value: "varchar(32)"}, {Name: "f7", Value: "int"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 15, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}, {Name: "f3", Value: "varchar(128)"}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 16, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}}},
}

var testRenameTablePrepare = []string{
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

var testRenameTable = []string{
	"insert into db2.t1 value (1)",
	"insert into db1.t1 value (1)",
	"insert into db1.t1 value (2)",
	"create table db1.t1_tmp like db1.t1",
	"alter table db1.t1_tmp add f2 int",
	"insert into db1.t1_tmp value (3, 30)",
	"insert into db1.t1_tmp value (4, 40)",
	"/* alterch_wait */ rename table db1.t1 to db1.t1_old, db1.t1_tmp to db1.t1",
	"insert into db1.t1 value (5, 50)",
	"rename table db1.t1_old to db1.t1_new_old",
	"use db2",
	"create table t1_tmp like t1",
	"alter table t1_tmp add f2 int",
	"rename table t1 to t1_old, t1_tmp to t1",
	"insert into db1.t1 value (6, 60)",
	"/* alterch_wait */ rename table `db1`.`t1` to `db1`.`t1_old`, `db1`.`t1_new_old` to `db1`.`t1`",
	"insert into db1.t1 value (7)",
}

var testRenameTableResult = []types.CommonFormatEvent{
	/* Test basic insert, update, delete */
	{Type: "insert", Key: []interface{}{int64(1)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1)}}},
	{Type: "insert", Key: []interface{}{int64(2)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(2)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}, {Name: "f2", Value: "int"}}},
	{Type: "insert", Key: []interface{}{int64(5)}, SeqNo: 4, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(5)}, {Name: "f2", Value: int32(50)}}},
	{Type: "insert", Key: []interface{}{int64(6)}, SeqNo: 5, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(6)}, {Name: "f2", Value: int32(60)}}},
	{Type: "schema", Key: []interface{}{"f1"}, SeqNo: 6, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: "bigint"}}},
	{Type: "insert", Key: []interface{}{int64(7)}, SeqNo: 7, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(7)}}},
}

var testDataTypesPrepare = []string{
	"drop database if exists db1",
	"create database if not exists db1",

	`create table db1.t1 (
		f1 bigint not null primary key,
		f2 varchar(32),
		f3 double,
		f4 text,
		f5 timestamp null default null,
		f6 date,
		f7 time,
		f8 year,
		f9 bigint,
		f10 binary(128),
		f11 int,
		f12 float,
		f13 double,
		f14 decimal(10,2),
		f15 numeric(10,2),
		f16 datetime,
		f17 enum('one', 'two'),
		f18 set('one', 'two'),
		f19 json
	)`,
}

var testDataTypes = []string{
	"insert into db1.t1 values(1567, '1567', 1567/3, 'testtextfield', '2018-05-03 17:18:25', '2018-05-03', '17:18:25', 2017, 98878, 'testbinaryfield', 827738, 111.23, 222.34, 333.45, 444.56, '2018-05-03 17:18:25', 'one', 'one,two', '{\"one\":\"two\"}')",
	"insert into db1.t1(f1,f5,f16) values(1568,NULL,NULL)",
	"insert into db1.t1(f1,f5,f16) values(1569,0,0)",
}

var testDataTypesResult = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{int64(1567)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1567)}, {Name: "f2", Value: "1567"}, {Name: "f3", Value: 522.333333333}, {Name: "f4", Value: "testtextfield"}, {Name: "f5", Value: "2018-05-03T17:18:25"}, {Name: "f6", Value: "2018-05-03"}, {Name: "f7", Value: "17:18:25"}, {Name: "f8", Value: int32(2017)}, {Name: "f9", Value: int64(98878)}, {Name: "f10", Value: []byte{116, 101, 115, 116, 98, 105, 110, 97, 114, 121, 102, 105, 101, 108, 100}}, {Name: "f11", Value: int32(827738)}, {Name: "f12", Value: float32(111.23)}, {Name: "f13", Value: 222.34}, {Name: "f14", Value: float64(333.45)}, {Name: "f15", Value: float64(444.56)}, {Name: "f16", Value: "2018-05-03T17:18:25"}, {Name: "f17", Value: int64(1)}, {Name: "f18", Value: int64(3)}, {Name: "f19", Value: "{\"one\":\"two\"}"}}},
	{Type: "insert", Key: []interface{}{int64(1568)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1568)}, {Name: "f2"}, {Name: "f3"}, {Name: "f4"}, {Name: "f5"}, {Name: "f6"}, {Name: "f7"}, {Name: "f8"}, {Name: "f9"}, {Name: "f10"}, {Name: "f11"}, {Name: "f12"}, {Name: "f13"}, {Name: "f14"}, {Name: "f15"}, {Name: "f16"}, {Name: "f17"}, {Name: "f18"}, {Name: "f19"}}},
	{Type: "insert", Key: []interface{}{int64(1569)}, SeqNo: 3, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(1569)}, {Name: "f2"}, {Name: "f3"}, {Name: "f4"}, {Name: "f5", Value: encoder.ZeroTime}, {Name: "f6"}, {Name: "f7"}, {Name: "f8"}, {Name: "f9"}, {Name: "f10"}, {Name: "f11"}, {Name: "f12"}, {Name: "f13"}, {Name: "f14"}, {Name: "f15"}, {Name: "f16", Value: encoder.ZeroTime}, {Name: "f17"}, {Name: "f18"}, {Name: "f19"}}},
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
	//"update db1.t1 set f1=f1+10 where f1=7",
	//"update db9.t1 set f1=f1+10 where f1=9",
}

var testMultiTableResult1 = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{int64(7)}, SeqNo: 1, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(7)}}},
	//{Type: "delete", Key: []interface{}{int64(7.0)}, SeqNo: 5.0, Fields: nil},
	//{Type: "insert", Key: []interface{}{int64(17.0)}, SeqNo: 6.0, Fields: &[]types.CommonFormatField{{"f1", int64(17.0)}}},
}

var testMultiTableResult2 = []types.CommonFormatEvent{
	{Type: "insert", Key: []interface{}{int64(9)}, SeqNo: 2, Timestamp: 0, Fields: &[]types.CommonFormatField{{Name: "f1", Value: int64(9)}}},
	//{Type: "delete", Key: []interface{}{int64(9.0)}, SeqNo: 7.0, Fields: nil},
	//{Type: "insert", Key: []interface{}{int64(19.0)}, SeqNo: 8.0, Fields: &[]types.CommonFormatField{{"f1", int64(19.0)}}},
}

var testShutdownPrepare = []string{
	"drop database if exists db1",
	"create database if not exists db1",

	`create table db1.t1 (
		f1 bigint not null primary key
	)`,
}

func worker(cfg *config.AppConfig, bp pipe.Pipe, tpool pool.Thread) {
	defer shutdown.Done()

	log.Debugf("Starting binlog reader in test")
	testReader = &mysqlReader{ctx: shutdown.Context, tpool: tpool, bufPipe: bp, inputType: types.InputMySQL}

	if !testReader.Worker() {
		log.Errorf("worker error")
		return
	}

	log.Debugf("Finished binlog worker in test")
}

func execSQL(db *sql.DB, t *testing.T, query string) {
	test.CheckFail(util.ExecSQL(db, query), t)
}

func regTableForMultiTableTest(pipeType string, secondPipe string, t *testing.T) {
	loc := &db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db9"}
	if strings.HasSuffix(t.Name(), "multi_table") {
		cfg.ChangelogBuffer = false
		if !state.RegisterTableInState(loc, "t1", "mysql", pipeType, 0, outputFormat, "", 0) {
			t.FailNow()
		}
		if !state.RegisterTableInState(loc, "t1", "mysql", pipeType, 1, outputFormat, "", 0) {
			t.FailNow()
		}
		if !state.RegisterTableInState(loc, "t1", "mysql", secondPipe, 1, outputFormat, "", 0) {
			t.FailNow()
		}
		if !state.RegisterTableInState(loc, "t1", "not_mysql", pipeType, 1, outputFormat, "", 0) {
			t.FailNow()
		}
		loc.Service = "test_svc2"
		if !state.RegisterTableInState(loc, "t1", "mysql", pipeType, 0, outputFormat, "", 0) {
			t.FailNow()
		}
	}
}

func startWorker(p1 pipe.Pipe, t *testing.T) {
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

	go worker(cfg, p1, fakePool)

	/* Let binlog reader to initialize */
	for {
		g, _, err := state.GetGTID("test_cluster1")
		test.CheckFail(err, t)
		if g != "" {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(50))
	}
}

func Prepare(pipeType string, create []string, encoding string, init bool, t *testing.T) (*sql.DB, pipe.Pipe, pipe.Pipe) {
	shutdown.Setup()

	if strings.HasSuffix(t.Name(), "multi_table") {
		outputFormat = encoding
	}

	cfg.InternalEncoding = encoding
	var err error
	encoder.Internal, err = encoder.InitEncoder(cfg.InternalEncoding, "", "", "", "", "", 0)
	test.CheckFail(err, t)

	log.Debugf("Test encoding: %+v", encoding)

	loc := &db.Loc{Cluster: "test_cluster1", Service: "test_svc1"}
	dbc, err := db.OpenService(loc, "", "mysql")
	test.CheckFail(err, t)

	if err := state.InitManager(shutdown.Context, cfg); err != nil {
		t.FailNow()
	}

	if init {
		execSQL(dbc, t, "RESET MASTER")
		execSQL(dbc, t, "SET GLOBAL binlog_format = 'ROW'")
		execSQL(dbc, t, "SET GLOBAL server_id=1")

		require.True(t, state.Reset())

		execSQL(state.GetDB(), t, "DROP TABLE IF EXISTS kafka_offsets")

		if create != nil {
			log.Debugf("Preparing database")
			for _, s := range create {
				execSQL(dbc, t, s)
			}
		}

		loc.Name = "db1"
		if !state.RegisterTableInState(loc, "t1", "mysql", pipeType, 0, encoding, "", 0) {
			t.FailNow()
		}
	} else {
		execSQL(state.GetDB(), t, "UPDATE cluster_state set locked_at=NULL")
	}

	p1, err := pipe.CacheGet(pipeType, &cfg.Pipe, state.GetDB())
	test.CheckFail(err, t)
	secondPipe := "local"
	if pipeType == "local" {
		secondPipe = "kafka"
	}
	p2, err := pipe.CacheGet(secondPipe, &cfg.Pipe, state.GetDB())
	test.CheckFail(err, t)

	log.Debugf("Starting binlog reader. PipeType=%v", pipeType)

	regTableForMultiTableTest(pipeType, secondPipe, t)

	startWorker(p1, t)

	return dbc, p1, p2
}

/*
func CheckBinlogFormat(t *testing.T) {
	shutdown.Setup()
	log.Debugf("TestBinlogFormat start")
	dbc, err := db.OpenService(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: ""}, "")
	test.CheckFail(err, t)
	defer func() { test.CheckFail(dbc.Close(), t) }()
	execSQL(dbc, t, "SET GLOBAL binlog_format = 'STATEMENT'")
	for _, s := range testBasicPrepare {
		execSQL(dbc, t, s)
	}
	if !state.Init(cfg) {
		t.FailNow()
	}
	defer func() { test.CheckFail(state.Close(), t) }()
	if !state.RegisterTableInState(&db.Loc{Cluster: "test_cluster1", Service: "test_svc1", Name: "db1"}, "t1") {
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
	execSQL(dbc, t, "SET GLOBAL binlog_format = 'ROW'")
	shutdown.Initiate()
	shutdown.Wait()
}
*/

func initConsumeTableEvents(p pipe.Pipe, svc, db string, table string, version int, t *testing.T) pipe.Consumer {
	tn, err := config.Get().GetChangelogTopicName(svc, db, table, "mysql", "local", version, time.Now())
	test.CheckFail(err, t)
	pc, err := p.NewConsumer(tn)
	test.CheckFail(err, t)
	log.Debugf("Start event consumer from: " + tn)
	return pc
}

func convertToCommonFormat(enc encoder.Encoder, b interface{}, seqnoShift []uint64) (*types.CommonFormatEvent, error) {
	var err error
	var cf *types.CommonFormatEvent
	switch m := b.(type) {
	case *types.RowMessage:
		b, err = enc.Row(m.Type, m.Data, m.SeqNo, m.Timestamp)
		if err != nil {
			return nil, err
		}
		cf, err = enc.DecodeEvent(b.([]byte))
		if err != nil {
			return nil, err
		}
	case []byte:
		cf = &types.CommonFormatEvent{}
		if !cfg.ChangelogBuffer {
			cf, err = enc.DecodeEvent(b.([]byte))
			if err != nil {
				return nil, err
			}
		} else {
			_, err := enc.UnwrapEvent(b.([]byte), cf)
			if err != nil {
				return nil, err
			}
		}
		if cf.Type == "schema" {
			log.Debugf("received schema %v", cf)
			alterCh <- true
			log.Debugf("alterch_sent")
		}
	}

	cf.SeqNo -= 1000000
	cf.Timestamp = 0

	return cf, nil
}

func consumeTableEvents(pc pipe.Consumer, svc, db, table, input string, output string, version int, result []types.CommonFormatEvent, seqnoShift []uint64, t *testing.T) error {
	log.Debugf("consuming events %+v %+v %+v %v %+v", svc, db, table, cfg.InternalEncoding, seqnoShift)
	enc, err := encoder.Create(cfg.InternalEncoding, svc, db, table, input, output, version)
	if log.E(err) {
		return err
	}

	for i, v := range result {
		if !pc.FetchNext() {
			break
		}
		b, err := pc.Pop()
		if err != nil {
			return err
		}
		if b == nil {
			t.Fatalf("No empty msg allowed")
		}
		cf, err := convertToCommonFormat(enc, b, seqnoShift)
		if err != nil {
			return err
		}

		//This hack is because of Golang's non-deterministic state map iteration
		// in changelog reader
		require.True(t, cf.SeqNo == v.SeqNo+seqnoShift[0] || cf.SeqNo == v.SeqNo+seqnoShift[1])
		cf.SeqNo = 0
		v.SeqNo = 0

		require.Equal(t, v, *cf)

		log.Infof("Successfully matched: i=%v %+v Fields=%v", i, cf, cf.Fields)
		if cf.Type == "schema" {
			err := enc.UpdateCodec()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func checkPoolControl(pipeType string, t *testing.T) {
	if pipeType == "local" {
		mt := strings.HasSuffix(t.Name(), "multi_table")
		if (mt && globalTPoolProcs != 6) || (!mt && globalTPoolProcs != 2) {
			t.Fatalf("Binlog reader should control number of streamers num=%v, pipe=%v", globalTPoolProcs, pipeType)
		}
	} else if globalTPoolProcs != 0 {
		t.Errorf("Binlog reader shouldn't control number of streamers num=%v pipe=%v", globalTPoolProcs, pipeType)
		t.Fail()
	}
}

func CheckQueries(pipeType string, prepare []string, queries []string, result []types.CommonFormatEvent, encoding string, init bool, t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)
	if pipeType == "kafka" {
		test.SkipIfNoKafkaAvailable(t)
	}

	dbc, p1, p2 := Prepare(pipeType, prepare, encoding, init, t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	log.Debugf("Starting consumers")

	initCh := make(chan error)
	shutdown.Register(1)
	go func() {
		var err error
		var pc, pc1, pc2, pc3, pc4 pipe.Consumer
		defer shutdown.Done()
		defer func() {
			log.Debugf("Signal that we finished consuming events")
			if pc != nil {
				test.CheckFail(pc.Close(), t)
			}
			if pc1 != nil {
				test.CheckFail(pc1.Close(), t)
			}
			if pc2 != nil {
				test.CheckFail(pc2.Close(), t)
			}
			if pc3 != nil {
				test.CheckFail(pc3.Close(), t)
			}
			if pc4 != nil {
				test.CheckFail(pc4.Close(), t)
			}
			initCh <- err
		}()
		pc = initConsumeTableEvents(p1, "test_svc1", "db1", "t1", 0, t)
		if strings.HasSuffix(t.Name(), "multi_table") {
			pc1 = initConsumeTableEvents(p1, "test_svc1", "db9", "t1", 0, t)
			pc2 = initConsumeTableEvents(p1, "test_svc1", "db9", "t1", 1, t)
			pc3 = initConsumeTableEvents(p2, "test_svc1", "db9", "t1", 1, t)
			pc4 = initConsumeTableEvents(p1, "test_svc2", "db9", "t1", 0, t)
		}
		log.Debugf("Signal that all consumer has been initialized")
		initCh <- nil
		err = consumeTableEvents(pc, "test_svc1", "db1", "t1", "mysql", pipeType, 0, result, []uint64{0, 0}, t)
		if err != nil {
			return
		}
		if strings.HasSuffix(t.Name(), "multi_table") {
			err = consumeTableEvents(pc1, "test_svc1", "db9", "t1", "mysql", pipeType, 0, testMultiTableResult2, []uint64{0, 1}, t)
			if log.E(err) {
				return
			}
			err = consumeTableEvents(pc2, "test_svc1", "db9", "t1", "mysql", pipeType, 0, testMultiTableResult2, []uint64{1, 2}, t)
			if log.E(err) {
				return
			}
			err = consumeTableEvents(pc3, "test_svc1", "db9", "t1", "mysql", pipeType, 0, testMultiTableResult2, []uint64{2, 3}, t)
			if log.E(err) {
				return
			}
			err = consumeTableEvents(pc4, "test_svc2", "db9", "t1", "mysql", pipeType, 0, testMultiTableResult2, []uint64{3, 0}, t)
			if log.E(err) {
				return
			}
		}
		log.Debugf("Finished consumers")
	}()

	log.Debugf("Waiting pipe consumers to initialize")
	<-initCh
	log.Debugf("Starting workload")

	for _, s := range queries {
		execSQL(dbc, t, s)
		if strings.Contains(strings.ToLower(s), "alterch_wait") {
			log.Debugf("alterch_wait %v", s)
			<-alterCh
			log.Debugf("alterch_afterwait %v", s)
		}
	}

	log.Debugf("Waiting consumer to consume all events and finish")
	err := <-initCh
	log.Debugf("Finishing test")

	shutdown.Initiate()
	shutdown.Wait()

	checkPoolControl(pipeType, t)

	*cfg = saveCfg

	log.Debugf("Finished test")

	if err != nil {
		log.E(err)
		t.FailNow()
	}
}

func runTests(pipeType string, format string, t *testing.T) {
	var tests = []struct {
		name    string
		prepare []string
		test    []string
		result  []types.CommonFormatEvent
	}{
		{"basic", testBasicPrepare, testBasic, testBasicResult},
		{"use_db", testBasicPrepare, testUseDB, testUseDBResult},
		{"multi_column", testMultiColumnPrepare, testMultiColumn, testMultiColumnResult},
		{"multi_row", testMultiColumnPrepare, testMultiRow, testMultiRowResult},
		{"compound_key", testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult},
		{"test_ddl", testDDLPrepare, testDDL, testDDLResult},
		{"multi_table", testMultiTablePrepare, testMultiTable, testMultiTableResult1},
		{"rename_table", testRenameTablePrepare, testRenameTable, testRenameTableResult},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CheckQueries(pipeType, tt.prepare, tt.test, tt.result, format, true, t)
		})
	}
}

func TestLocalJson(t *testing.T) {
	runTests("local", "json", t)
}

func TestMsgPack(t *testing.T) {
	runTests("local", "msgpack", t)
}

func TestMsgPackKafka(t *testing.T) {
	runTests("kafka", "msgpack", t)
}

func TestKafka(t *testing.T) {
	runTests("kafka", "json", t)
}

func TestGracefulRestart(t *testing.T) {
	CheckQueries("local", testBasicPrepare, testBasic, testBasicResult, "json", true, t)
	CheckQueries("local", nil, testRestart, testRestartResult, "json", false, t)
}

func TestAlterFailureRestart(t *testing.T) {
	injectAlterFailure = true

	dbc, _, _ := Prepare("local", testBasicPrepare, "json", true, t)

	for _, s := range testAlterFailure {
		execSQL(dbc, t, s)
	}

	test.CheckFail(dbc.Close(), t)

	if !test.WaitForNumProc(3, 16*time.Second) {
		t.Fatalf("Number of workers didn't decrease in %v secs. NumProcs: %v", 16*time.Second, shutdown.NumProcs())
	}

	shutdown.Initiate()
	shutdown.Wait()
	test.CheckFail(state.Close(), t)

	injectAlterFailure = false
	CheckQueries("local", nil, testAlterFailureRestarted, testAlterFailureResult, "json", false, t)
}

func inVersionArray(a []*table, output string, version int) bool {
	return findInVersionArray(a, output, version) < len(a)
}

func TestMultiVersions_multi_table(t *testing.T) {
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

	saved := cfg.StateUpdateInterval
	cfg.StateUpdateInterval = 1 * time.Second

	dbc, _, _ := Prepare("local", testMultiTablePrepare, "json", true, t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	// Wait till changelog reader pickup the tables
	if !test.WaitForNumProcGreater(5+3, 80*200) {
		t.Fatalf("Number of workers didn't decrease in %v secs. NumProcs: %v", 80*50/1000, shutdown.NumProcs())
	}

	if !state.DeregisterTableFromState(&db.Loc{Service: "test_svc2", Cluster: "test_cluster1", Name: "db9"}, "t1", "mysql", "local", 0, 0) {
		t.Fatalf("Failed to deregister table")
	}

	n := 0
	for i := 0; i < len(tests); i++ {
		if !test.WaitForNumProc(int32(len(tests)-n+2), 16*time.Second) {
			t.Fatalf("Number of workers didn't decrease in %v secs. NumProcs: %v", 80*50/1000, shutdown.NumProcs())
		}

		var dlen, tlen = 0, 0
		if tests[i].real != 0 {
			test.Assert(t, testReader.tables[tests[i].db] != nil, "Db not found in map: %v", tests[i].db)
			dlen = len(testReader.tables[tests[i].db])
			a := testReader.tables[tests[i].db][tests[i].t]["test_svc1"]
			test.Assert(t, a != nil, "Table not found in map: %v", tests[i].t)
			tlen = len(a)
			test.Assert(t, inVersionArray(a, tests[i].output, tests[i].version), "Not found in map %v %v %v %v", tests[i].db, tests[i].t, tests[i].output, tests[i].version)
		}

		n += tests[i].real

		if !state.DeregisterTableFromState(&db.Loc{Service: "test_svc1", Cluster: "test_cluster1", Name: tests[i].db}, tests[i].t, tests[i].input, tests[i].output, tests[i].version, 0) {
			t.Fatalf("Failed to deregister table")
		}

		if !test.WaitForNumProc(int32(len(tests)-n+2), 16*time.Second) {
			t.Fatalf("Number of workers didn't decrease in %v secs. NumProcs: %v", 80*50/1000, shutdown.NumProcs())
		}

		//Check that table has been properly removed from the map and version array.
		if tests[i].real != 0 {
			if tlen > 1 {
				test.Assert(t, testReader.tables[tests[i].db] != nil, "Db not found in map: %v", tests[i].db)
				a := testReader.tables[tests[i].db][tests[i].t]["test_svc1"]
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
	if !test.WaitForNumProc(2, 16*time.Second) { // 1 signal handler + 1 state reg routine
		t.Fatalf("Binlog reader didn't finish int %v secs. NumProcs: %v", 16*time.Second, shutdown.NumProcs())
	}

	shutdown.Initiate()
	shutdown.Wait()

	cfg.StateUpdateInterval = saved
}

func TestDataTypes(t *testing.T) {
	var err error
	(*testDataTypesResult[0].Fields)[4].Value, err = time.ParseInLocation("2006-01-02T15:04:05", (*testDataTypesResult[0].Fields)[4].Value.(string), time.Local)
	test.CheckFail(err, t)
	(*testDataTypesResult[0].Fields)[15].Value, err = time.ParseInLocation("2006-01-02T15:04:05", (*testDataTypesResult[0].Fields)[15].Value.(string), time.UTC)
	test.CheckFail(err, t)
	CheckQueries("local", testDataTypesPrepare, testDataTypes, testDataTypesResult, "json", true, t)
}

func TestDirectOutput(t *testing.T) {
	cfg.ChangelogBuffer = false
	outputFormat = "msgpack" //set to different from "json" to check that reader output in final format and not in buffer format
	CheckQueries("kafka", testBasicPrepare, testBasic, testBasicResult, "json", true, t)
}

func TestReaderShutdown(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	saved := cfg.StateUpdateInterval
	cfg.StateUpdateInterval = 1 * time.Second

	dbc, _, _ := Prepare("local", testShutdownPrepare, "json", true, t)
	defer func() { test.CheckFail(state.Close(), t) }()
	defer func() { test.CheckFail(dbc.Close(), t) }()

	if !state.DeregisterTableFromState(&db.Loc{Service: "test_svc1", Cluster: "test_cluster1", Name: "db1"}, "t1", "mysql", "local", 0, 0) {
		t.Fatalf("Failed to deregister table")
	}

	if !test.WaitForNumProc(2, 16*time.Second) {
		t.Fatalf("Binlog reader didn't finish int %v secs", 16*time.Second)
	}

	fakePool.Adjust(0)
	log.Debugf("adjusted pool to 0")

	if !test.WaitForNumProc(1, 16*time.Second) {
		t.Fatalf("Binlog reader didn't finish int %v secs. NumProcs: %v", 16*time.Second, shutdown.NumProcs())
	}

	cfg.StateUpdateInterval = saved

	shutdown.Initiate()
	shutdown.Wait()
}

func TestQueryRegexpNegative(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		matched interface{}
	}{
		{"empty", "", nil},
		{"other query", "OPTIMIZE TABLE", nil},
		{"no_column", "ALTER TABLE", nil},
		{"no_rename", "aaa TO bbb", nil},
		{"mixed_quotes_rename", "RENAME TABLE aaa TO `bbb`.ccc", nil},
		{"incomplete_rename", "RENAME TABLE aaa", nil},
	}

	for i := 0; i < len(queryHandlers); i++ {
		queryHandlers[i].compiled = regexp.MustCompile(queryHandlers[i].regexp)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noMatch := true
			for _, v := range queryHandlers {
				m := v.compiled.FindAllStringSubmatch(tt.input, -1)
				if len(m) > 0 && (!strings.Contains(tt.name, "_rename") || len(m) >= 2) {
					log.Debugf("Match result: %q", m)
					require.Equal(t, tt.matched, m)
					noMatch = false

				}
			}
			require.True(t, noMatch)
		})
	}
}

func TestQueryRegexp(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		matched interface{}
	}{
		{"comment_alter", "/* ALTER TABLE aaa.bbb */ ALTER TABLE aaa.bbb ADD f1 int", [][]string{{"/* ALTER TABLE aaa.bbb */ ALTER TABLE aaa.bbb ADD f1 int", "aaa", "bbb", "ADD f1 int"}}},
		{"simple_alter", "ALTER TABLE aaa.bbb ADD f1 int", [][]string{{"ALTER TABLE aaa.bbb ADD f1 int", "aaa", "bbb", "ADD f1 int"}}},
		{"simple_alter_no_db", "ALTER TABLE bbb ADD f1 int", [][]string{{"ALTER TABLE bbb ADD f1 int", "", "bbb", "ADD f1 int"}}},
		{"mixed_case_alter", "AlTeR tAbLe aaa.bbb ADd f1 iNt", [][]string{{"AlTeR tAbLe aaa.bbb ADd f1 iNt", "aaa", "bbb", "ADd f1 iNt"}}},
		{"multi_column_alter", "ALTER TABLE aaa.bbb ADD f2 int, ADD f3 timestamp, DROP f1", [][]string{{"ALTER TABLE aaa.bbb ADD f2 int, ADD f3 timestamp, DROP f1", "aaa", "bbb", "ADD f2 int, ADD f3 timestamp, DROP f1"}}},
		{"whitespace_alter", " \t ALTER\t  TABLE \t aaa.bbb\t  ADD\t  f1\t  int\t", [][]string{{" \t ALTER\t  TABLE \t aaa.bbb\t  ADD\t  f1\t  int\t", "aaa", "bbb", "ADD\t  f1\t  int\t"}}},
		{"newline_alter", "\nALTER\nTABLE\naaa.bbb\nADD\nf1\nint\n", [][]string{{"\nALTER\nTABLE\naaa.bbb\nADD\nf1\nint\n", "aaa", "bbb", "ADD\nf1\nint\n"}}},
		{"quoted_alter", "ALTER TABLE `aaa`.`bbb` ADD f1 int", [][]string{{"ALTER TABLE `aaa`.`bbb` ADD f1 int", "aaa", "bbb", "ADD f1 int"}}},
		{"quoted_with_space_alter", "ALTER TABLE `a aa`.`b bb` ADD f1 int", [][]string{{"ALTER TABLE `a aa`.`b bb` ADD f1 int", "a aa", "b bb", "ADD f1 int"}}},
		{"quoted_db_alter", "ALTER TABLE `aaa`.bbb ADD f1 int", [][]string{{"ALTER TABLE `aaa`.bbb ADD f1 int", "aaa", "bbb", "ADD f1 int"}}},
		{"quoted_table_alter", "ALTER TABLE aaa.`bbb` ADD f1 int", [][]string{{"ALTER TABLE aaa.`bbb` ADD f1 int", "aaa", "bbb", "ADD f1 int"}}},
		{"everything_alter", "\n/* comment */\n AlTeR\t TaBlE aaa.`bb b`  ADD  f2\t int, aDd f3 timestamp, DrOP f1", [][]string{{"\n/* comment */\n AlTeR\t TaBlE aaa.`bb b`  ADD  f2\t int, aDd f3 timestamp, DrOP f1", "aaa", "bb b", "ADD  f2\t int, aDd f3 timestamp, DrOP f1"}}},
		{"bug1_alter", "ALTER TABLE t1\nADD COLUMN `col1` VARCHAR(128) GENERATED ALWAYS AS (`p`-\u003e\"$.i1\") AFTER `properties`,\nADD INDEX `pi1` (`pi1`)", [][]string{{"ALTER TABLE t1\nADD COLUMN `col1` VARCHAR(128) GENERATED ALWAYS AS (`p`->\"$.i1\") AFTER `properties`,\nADD INDEX `pi1` (`pi1`)", "", "t1", "ADD COLUMN `col1` VARCHAR(128) GENERATED ALWAYS AS (`p`->\"$.i1\") AFTER `properties`,\nADD INDEX `pi1` (`pi1`)"}}},
		{"bug2_alter", "-- +goose Up\n-- SQL in section 'Up' is executed when this migration is applied\nALTER TABLE `t1` ADD COLUMN `f1` timestamp NULL DEFAULT NULL", [][]string{{"ALTER TABLE `t1` ADD COLUMN `f1` timestamp NULL DEFAULT NULL", "", "t1", "ADD COLUMN `f1` timestamp NULL DEFAULT NULL"}}},

		{"comment_rename", "/* RENAME TABLE */ RENAME TABLE aaa.bbb TO ccc.ddd", [][]string{{"/* RENAME TABLE */ RENAME TABLE ", "/* RENAME TABLE */ RENAME TABLE ", "", "", "", ""}, {"aaa.bbb TO ccc.ddd", "", "aaa", "bbb", "ccc", "ddd"}}},
		{"simple_rename", "RENAME TABLE aaa.bbb TO ccc.ddd", [][]string{{"RENAME TABLE ", "RENAME TABLE ", "", "", "", ""}, {"aaa.bbb TO ccc.ddd", "", "aaa", "bbb", "ccc", "ddd"}}},
		{"db_no_db_rename", "RENAME TABLE aaa TO ccc.ddd", [][]string{{"RENAME TABLE ", "RENAME TABLE ", "", "", "", ""}, {"aaa TO ccc.ddd", "", "", "aaa", "ccc", "ddd"}}},
		{"dot_in_name_rename", "RENAME TABLE `a.aa `.`.bb.b` TO `cc.c`.`dd.d`", [][]string{{"RENAME TABLE ", "RENAME TABLE ", "", "", "", ""}, {"`a.aa `.`.bb.b` TO `cc.c`.`dd.d`", "", "a.aa ", ".bb.b", "cc.c", "dd.d"}}},
		{"simple_rename_no_db", "RENAME TABLE bbb TO ddd", [][]string{{"RENAME TABLE ", "RENAME TABLE ", "", "", "", ""}, {"bbb TO ddd", "", "", "bbb", "", "ddd"}}},
		{"mixed_case_rename", "rEnAMe TAbLE aaa.bbb To ccc.ddd", [][]string{{"rEnAMe TAbLE ", "rEnAMe TAbLE ", "", "", "", ""}, {"aaa.bbb To ccc.ddd", "", "aaa", "bbb", "ccc", "ddd"}}},
		{"multi_table_rename", "RENAME TABLE aaa.bbb TO ccc.ddd, eee.fff TO ggg.hhh, jjj.kkk TO lll.mmm", [][]string{{"RENAME TABLE ", "RENAME TABLE ", "", "", "", ""}, {"aaa.bbb TO ccc.ddd", "", "aaa", "bbb", "ccc", "ddd"}, {", eee.fff TO ggg.hhh", "", "eee", "fff", "ggg", "hhh"}, {", jjj.kkk TO lll.mmm", "", "jjj", "kkk", "lll", "mmm"}}},
		{"whitespace_rename", " \tRENAME  TABLE  aaa.bbb  TO\t  ccc.ddd ,eee.fff  TO ggg.hhh", [][]string{{" \tRENAME  TABLE  ", " \tRENAME  TABLE  ", "", "", "", ""}, {"aaa.bbb  TO\t  ccc.ddd", "", "aaa", "bbb", "ccc", "ddd"}, {" ,eee.fff  TO ggg.hhh", "", "eee", "fff", "ggg", "hhh"}}},
		{"newline_rename", " \nRENAME  TABLE  aaa.bbb  TO\n  ccc.ddd ,eee.fff \n TO ggg.hhh", [][]string{{" \nRENAME  TABLE  ", " \nRENAME  TABLE  ", "", "", "", ""}, {"aaa.bbb  TO\n  ccc.ddd", "", "aaa", "bbb", "ccc", "ddd"}, {" ,eee.fff \n TO ggg.hhh", "", "eee", "fff", "ggg", "hhh"}}},
		{"quoted_rename", "RENAME TABLE `aaa`.`bbb` TO `ccc`.`ddd`, `eee`.`fff` TO `ggg`.`hhh`", [][]string{{"RENAME TABLE ", "RENAME TABLE ", "", "", "", ""}, {"`aaa`.`bbb` TO `ccc`.`ddd`", "", "aaa", "bbb", "ccc", "ddd"}, {", `eee`.`fff` TO `ggg`.`hhh`", "", "eee", "fff", "ggg", "hhh"}}},
		{"quoted_with_whitespce_rename", "RENAME TABLE `aa a`.`b bb` TO `c cc`.`d dd`", [][]string{{"RENAME TABLE ", "RENAME TABLE ", "", "", "", ""}, {"`aa a`.`b bb` TO `c cc`.`d dd`", "", "aa a", "b bb", "c cc", "d dd"}}},

		{"everything_rename", " \nReNaME \t TAbLE  `a aa`.`bb.b` tO\n `ccc`.`ddd`,  `eee`  TO `ggg`.`hhh`, `jjj`.`kkk` TO `mmm`", [][]string{{" \nReNaME \t TAbLE  ", " \nReNaME \t TAbLE  ", "", "", "", ""}, {"`a aa`.`bb.b` tO\n `ccc`.`ddd`", "", "a aa", "bb.b", "ccc", "ddd"}, {",  `eee`  TO `ggg`.`hhh`", "", "", "eee", "ggg", "hhh"}, {", `jjj`.`kkk` TO `mmm`", "", "jjj", "kkk", "", "mmm"}}},
	}

	for i := 0; i < len(queryHandlers); i++ {
		queryHandlers[i].compiled = regexp.MustCompile(queryHandlers[i].regexp)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := false
			for _, v := range queryHandlers {
				m := v.compiled.FindAllStringSubmatch(tt.input, -1)
				if len(m) > 0 && (!strings.Contains(tt.name, "_rename") || len(m) >= 2) {
					require.Equal(t, tt.matched, m)
					matched = true
				}
			}
			require.True(t, matched)
		})
	}
}

func TestReloadState(t *testing.T) {
	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	err := state.InitManager(shutdown.Context, cfg)
	require.NoError(t, err)
	require.True(t, state.Reset())

	ndbs := 3
	ntbls := 3
	nvers := 3
	for i := 0; i < ndbs; i++ {
		execSQL(state.GetDB(), t, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS d%d", i))
		for j := 0; j < ndbs; j++ {
			execSQL(state.GetDB(), t, fmt.Sprintf("CREATE TABLE IF NOT EXISTS d%d.t%d (f1 int primary key)", i, j))
		}
	}

	b := &mysqlReader{}
	b.tables = make(map[string]map[string]map[string][]*table)

	b.bufPipe, err = pipe.Create("local", &config.Get().TableParams.Pipe, state.GetDB())
	require.NoError(t, err)

	b.metrics = metrics.NewChangelogReaderMetrics(getTags("cluster1", "mysql"))
	b.log = log.WithFields(log.Fields{"test": "test"})

	b.dbl.Cluster = "c1"

	res := b.reloadState()
	require.False(t, res)

	// Register ndbs DBs with ntbls tables and nvers versions
	for i := 0; i < ndbs; i++ {
		for j := 0; j < ntbls; j++ {
			for k := 0; k < nvers; k++ {
				r := state.RegisterTableInState(&db.Loc{Cluster: "c1", Service: "s1", Name: fmt.Sprintf("d%d", i)}, fmt.Sprintf("t%d", j), "mysql", "local", k, "json", "", 0)
				require.True(t, r)
			}
			for k := 0; k < nvers; k++ {
				r := state.RegisterTableInState(&db.Loc{Cluster: "c1", Service: "s2", Name: fmt.Sprintf("d%d", i)}, fmt.Sprintf("t%d", j), "mysql", "local", k, "json", "", 0)
				require.True(t, r)
			}
		}
	}

	res = b.reloadState()
	require.True(t, res)

	// Check that changelog reader sees registered tables
	require.Equal(t, ndbs, len(b.tables))
	for i := 0; i < ndbs; i++ {
		dbn := fmt.Sprintf("d%d", i)
		require.Equal(t, ntbls, len(b.tables[dbn]))
		for j := 0; j < ntbls; j++ {
			tbl := fmt.Sprintf("t%d", j)
			require.Equal(t, nvers, len(b.tables[dbn][tbl]["s1"]))
			for i := 0; i < len(b.tables[dbn][tbl]["s1"]); i++ {
				tver := b.tables[dbn][tbl]["s1"][i]
				require.Equal(t, true, tver.dead)
				require.Equal(t, i, tver.version)
			}
			require.Equal(t, nvers, len(b.tables[dbn][tbl]["s2"]))
			for i := 0; i < len(b.tables[dbn][tbl]["s2"]); i++ {
				tver := b.tables[dbn][tbl]["s2"][i]
				require.Equal(t, true, tver.dead)
				require.Equal(t, i, tver.version)
			}
		}
	}

	// Remove all the tables and versions from d0 database
	for j := 0; j < ntbls; j++ {
		tbl := fmt.Sprintf("t%d", j)
		for k := 0; k < nvers; k++ {
			r := state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s1", Name: "d0"}, tbl, "mysql", "local", k, 0)
			require.True(t, r)
		}
		for k := 0; k < nvers; k++ {
			r := state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s2", Name: "d0"}, tbl, "mysql", "local", k, 0)
			require.True(t, r)
		}
	}

	// Remove one versioin from t0, two from t1, and three from t2 versions in d1 database
	for j := 0; j < ntbls; j++ {
		tbl := fmt.Sprintf("t%d", j)
		for k := 0; k < j+1; k++ {
			r := state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s1", Name: "d1"}, tbl, "mysql", "local", k, 0)
			require.True(t, r)
		}
		for k := 0; k < j+1; k++ {
			r := state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s2", Name: "d1"}, tbl, "mysql", "local", k, 0)
			require.True(t, r)
		}
	}

	// Remove version 0 from tble t0 of d2 database
	r := state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s1", Name: "d2"}, "t0", "mysql", "local", 0, 0)
	require.True(t, r)
	r = state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s2", Name: "d2"}, "t0", "mysql", "local", 0, 0)
	require.True(t, r)
	// Remove version 1 from tble t1 of d2 database
	r = state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s1", Name: "d2"}, "t1", "mysql", "local", 1, 0)
	require.True(t, r)
	r = state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s2", Name: "d2"}, "t1", "mysql", "local", 1, 0)
	require.True(t, r)
	// Remove version 2 from tble t2 of d2 database
	r = state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s1", Name: "d2"}, "t2", "mysql", "local", 2, 0)
	require.True(t, r)
	r = state.DeregisterTableFromState(&db.Loc{Cluster: "c1", Service: "s2", Name: "d2"}, "t2", "mysql", "local", 2, 0)
	require.True(t, r)

	res = b.reloadState()
	require.True(t, res)

	// Check that changelog reader sees the changes
	require.Equal(t, 2, len(b.tables))
	for i := 0; i < ndbs; i++ {
		dbn := fmt.Sprintf("d%d", i)
		tbls := ntbls
		if i == 0 {
			tbls = 0
		} else if i == 1 {
			tbls = 2
		}
		require.Equal(t, tbls, len(b.tables[dbn]))
		for j := 0; j < tbls; j++ {
			tbl := fmt.Sprintf("t%d", j)
			vers := nvers - 1
			if i == 1 {
				vers = nvers - j - 1
			}
			require.Equal(t, vers, len(b.tables[dbn][tbl]["s1"]))
			require.Equal(t, vers, len(b.tables[dbn][tbl]["s2"]))
			tt := j
			for k := j; k < len(b.tables[dbn][tbl]["s1"]); k++ {
				tver := b.tables[dbn][tbl]["s1"][k]
				require.Equal(t, true, tver.dead)
				require.NotEqual(t, tt, tver.version)
			}
			for k := j; k < len(b.tables[dbn][tbl]["s2"]); k++ {
				tver := b.tables[dbn][tbl]["s2"][k]
				require.Equal(t, true, tver.dead)
				require.NotEqual(t, tt, tver.version)
			}
		}
	}
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	cfg.MaxNumProcs = 1

	saveCfg = *cfg

	//MySQL 5.6 default, to test zero dates
	db.SQLMode = "NO_ENGINE_SUBSTITUTION"

	pipe.InitialOffset = sarama.OffsetNewest
	pipe.KafkaConfig = sarama.NewConfig()
	pipe.KafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	pipe.KafkaConfig.Producer.Return.Successes = true
	pipe.KafkaConfig.Consumer.MaxWaitTime = 10 * time.Millisecond

	log.Debugf("Config loaded %v", cfg)
	os.Exit(m.Run())
}
