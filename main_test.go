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

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/uber/storagetapper/changelog"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var inserts = []string{
	"INSERT INTO e2e_test_table1(f1) VALUES (?)",
	"INSERT INTO e2e_test_table1(f1,f2) VALUES (?, CONCAT('bbb', ?))",
	"INSERT INTO e2e_test_table2(f1) VALUES (?)",
	"INSERT INTO e2e_test_table1(f1) VALUES (?)",
}

var insertsResultJSON = []string{
	`{"Type":"insert","Key":[%d],"SeqNo":%d,"Timestamp":0,"Fields":[{"Name":"f1","Value":%d},{"Name":"f3","Value":0},{"Name":"f4","Value":null}]}`,
	`{"Type":"insert","Key":[%d],"SeqNo":%d,"Timestamp":0,"Fields":[{"Name":"f1","Value":%d},{"Name":"f3","Value":0},{"Name":"f4","Value":null},{"Name":"f2","Value":"bbb%d"}]}`,
	`{"Type":"insert","Key":[%d],"SeqNo":%d,"Timestamp":0,"Fields":[{"Name":"f1","Value":%d},{"Name":"f3","Value":0},{"Name":"f4","Value":null}]}`,
	`{"Type":"insert","Key":[%d],"SeqNo":%d,"Timestamp":0,"Fields":[{"Name":"f1","Value":%d},{"Name":"f3","Value":0},{"Name":"f4","Value":null}]}`,
}

/*SQL command and expected test result, both for Avro and JSON formats*/
var insertsResultSQL = []string{
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4") VALUES (%d,%d,0,NULL);`,
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4","f2") VALUES (%d,%d,0,NULL,'bbb%d');`,
	`INSERT INTO "e2e_test_table2" ("seqno","f1","f3","f4") VALUES (%d,%d,0,NULL);`,
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4") VALUES (%d,%d,0,NULL);`,
}

var insertsResultSQLIdempotent = []string{
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4") VALUES (%d,%d,0,NULL) ON DUPLICATE KEY UPDATE "f3"= IF(seqno < VALUES(seqno), VALUES("f3"),"f3"),"f4"= IF(seqno < VALUES(seqno), VALUES("f4"),"f4"), seqno = IF(seqno < VALUES(seqno), VALUES(seqno), seqno);`,
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4","f2") VALUES (%d,%d,0,NULL,'bbb%d') ON DUPLICATE KEY UPDATE "f3"= IF(seqno < VALUES(seqno), VALUES("f3"),"f3"),"f4"= IF(seqno < VALUES(seqno), VALUES("f4"),"f4"),"f2"= IF(seqno < VALUES(seqno), VALUES("f2"),"f2"), seqno = IF(seqno < VALUES(seqno), VALUES(seqno), seqno);`,
	`INSERT INTO "e2e_test_table2" ("seqno","f1","f3","f4") VALUES (%d,%d,0,NULL) ON DUPLICATE KEY UPDATE "f3"= IF(seqno < VALUES(seqno), VALUES("f3"),"f3"),"f4"= IF(seqno < VALUES(seqno), VALUES("f4"),"f4"), seqno = IF(seqno < VALUES(seqno), VALUES(seqno), seqno);`,
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4") VALUES (%d,%d,0,NULL) ON DUPLICATE KEY UPDATE "f3"= IF(seqno < VALUES(seqno), VALUES("f3"),"f3"),"f4"= IF(seqno < VALUES(seqno), VALUES("f4"),"f4"), seqno = IF(seqno < VALUES(seqno), VALUES(seqno), seqno);`,
}

var insertsResult = map[string][]string{
	"avro":               insertsResultJSON,
	"json":               insertsResultJSON,
	"msgpack":            insertsResultJSON,
	"ansisql":            insertsResultSQL,
	"mysql":              insertsResultSQL, //quotes replaced in the code
	"ansisql_idempotent": insertsResultSQLIdempotent,
	"mysql_idempotent":   insertsResultSQLIdempotent, //quotes replaced in the code
}

//Test updates, which generated records pair: delete/insert
var updateStmt = []string{
	"UPDATE e2e_test_table1 SET f3=f3+20 WHERE f1>=? AND f1<?",
}

var updateResultJSON = []string{
	`{"Type":"delete","Key":[%v],"SeqNo":%d,"Timestamp":0}`,
	`{"Type":"insert","Key":[%d],"SeqNo":%d,"Timestamp":0,"Fields":[{"Name":"f1","Value":%d},{"Name":"f3","Value":20},{"Name":"f4","Value":null}]}`,
}

//Test updates, which generated records pair: delete/insert
var updateResultSQL = []string{
	`DELETE FROM "e2e_test_table1" WHERE "seqno"=%d AND "f1"=%d;`,
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4") VALUES (%d,%d,20,NULL);`,
}

var updateResultSQLIdempotent = []string{
	`DELETE FROM "e2e_test_table1" WHERE "seqno"=%d AND "f1"=%d;`,
	`INSERT INTO "e2e_test_table1" ("seqno","f1","f3","f4") VALUES (%d,%d,20,NULL) ON DUPLICATE KEY UPDATE "f3"= IF(seqno < VALUES(seqno), VALUES("f3"),"f3"),"f4"= IF(seqno < VALUES(seqno), VALUES("f4"),"f4"), seqno = IF(seqno < VALUES(seqno), VALUES(seqno), seqno);`,
}

var updateResult = map[string][]string{
	"avro":               updateResultJSON,
	"json":               updateResultJSON,
	"msgpack":            updateResultJSON,
	"mysql":              updateResultSQL,
	"ansisql":            updateResultSQL,
	"mysql_idempotent":   updateResultSQLIdempotent,
	"ansisql_idempotent": updateResultSQLIdempotent,
}

func formatSQL(s string) bool {
	return strings.HasPrefix(s, "ansisql") || strings.HasPrefix(s, "mysql")
}

func convertQuotes(s *[]string) {
	for i := 0; i < len(*s); i++ {
		(*s)[i] = strings.Replace((*s)[i], `"`, "`", -1)
	}
}

func waitSnapshotToFinish(table string, output string, t *testing.T) {
	for !shutdown.Initiated() {
		row, err := state.GetTable("e2e_test_svc1", "e2e_test_cluster1", "e2e_test_db1", table, "mysql", output, 0)
		test.CheckFail(err, t)
		if !row.SnapshottedAt.IsZero() && !row.NeedSnapshot {
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
	time.Sleep(time.Millisecond * 50)

	log.Debugf("Detected snapshot finished for %v", table)
}

func consumeEvents(c pipe.Consumer, format string, result []string, outEncoder encoder.Encoder, tableNum string, t *testing.T) {
	for _, v := range result {
		log.Debugf("Waiting event: %+v", v)
		//We can't notify "avro" encoder about schema changed by "ALTER"
		//so remove new "f2" field "avro" encoded doesn't know of
		if format == "avro" {
			var r types.CommonFormatEvent
			err := json.Unmarshal([]byte(v), &r)
			test.CheckFail(err, t)

			if r.Fields != nil {
				k := 0
				for k < len(*r.Fields) && (*r.Fields)[k].Name != "f2" {
					k++
				}
				if k < len(*r.Fields) {
					//"f2" field, if present, should be last field in the fields array
					*r.Fields = (*r.Fields)[:k]
				}
			}

			/*
				if format == "avro" && r.Type == "delete" {
					key := encoder.GetCommonFormatKey(&r)
					r.Key = make([]interface{}, 0)
					r.Key = append(r.Key, key)
				}
			*/

			b, err := json.Marshal(r)
			v = string(b)
			test.CheckFail(err, t)
		}
		if !c.FetchNext() {
			return
		}
		msg, err := c.Pop()
		test.CheckFail(err, t)

		var b []byte
		if format == "msgpack" || format == "avro" {
			r, err := outEncoder.DecodeEvent(msg.([]byte))
			test.CheckFail(err, t)
			b, err = json.Marshal(r)
			test.CheckFail(err, t)
		} else {
			b = msg.([]byte)
		}

		conv := string(b)
		if v != conv {
			log.Errorf("Received : %+v %v", conv, len(b))
			log.Errorf("Reference: %+v %v", v, len(v))
			t.FailNow()
		}
		log.Debugf("Successfully matched: %+v %v", conv, len(b))
	}
}

func waitMainInitialized() {
	for shutdown.NumProcs() < 2 {
		time.Sleep(time.Millisecond * 50)
	}
}

func waitAllEventsStreamed(format string, c pipe.Consumer, sseqno int, seqno int) int {
	//Only json and msgpack push schema to stream, skip schema events for others
	if format != "json" && format != "msgpack" && !formatSQL(format) {
		sseqno++
	}

	for ; sseqno < seqno; sseqno++ {
		_ = c.FetchNext()
	}

	return sseqno
}

func initTestDB(t *testing.T) {
	conn, err := db.Open(&db.Addr{Host: "localhost", Port: 3306, User: types.TestMySQLUser, Pwd: types.TestMySQLPassword, DB: ""})
	require.NoError(t, err)

	test.ExecSQL(conn, t, "DROP DATABASE IF EXISTS "+types.MyDBName)
	test.ExecSQL(conn, t, "DROP DATABASE IF EXISTS e2e_test_db1")
	test.ExecSQL(conn, t, "RESET MASTER")

	test.ExecSQL(conn, t, "CREATE DATABASE e2e_test_db1")
	test.ExecSQL(conn, t, "CREATE TABLE e2e_test_db1.e2e_test_table1(f1 int not null primary key, f3 int not null default 0, f4 int)")

	err = conn.Close()
	require.NoError(t, err)
}

func addTable(format string, tableNum string, output string, t *testing.T) {
	var err error

	table := "e2e_test_table" + tableNum
	log.Debugf("Adding table: %v, output: %v, format: %v", table, output, format)

	//Insert some test cluster connection info
	if tableNum == "1" {
		err := util.HTTPPostJSON("http://localhost:7836/cluster",
			`{"cmd" : "add", "name" : "e2e_test_cluster1", "host" : "localhost", "port" : 3306,
				"user" : "`+types.TestMySQLUser+`", "pw" : "`+types.TestMySQLPassword+`"}`)
		test.CheckFail(err, t)
	}
	//Insert output Avro schema
	dst := `, "dst" : "state"`
	err = util.HTTPPostJSON("http://localhost:7836/schema",
		`{"cmd" : "register", "service" : "e2e_test_svc1", "inputtype": "mysql", "output":"`+output+`", "db" : "e2e_test_db1",
				"type" : "`+format+`", "table" : "`+table+`"`+dst+` }`)
	test.CheckFail(err, t)

	if tableNum == "1" {
		conn, err := db.Open(&db.Addr{Host: "localhost", Port: 3306, User: types.TestMySQLUser, Pwd: types.TestMySQLPassword, DB: ""})
		test.CheckFail(err, t)
		test.ExecSQL(conn, t, "UPDATE "+types.MyDBName+".cluster_state SET seqno=0 WHERE cluster='e2e_test_cluster1'")
		err = conn.Close()
		test.CheckFail(err, t)
	}

	//Register test table for ingestion
	err = util.HTTPPostJSON("http://localhost:7836/table",
		`{"cmd": "add", "cluster": "e2e_test_cluster1", "service": "e2e_test_svc1", "db": "e2e_test_db1",
			"table": "`+table+`", "input": "mysql", "output":"`+output+`","OutputFormat":"`+format+`"}`)
	test.CheckFail(err, t)

	//Force sync state with table registrations
	require.True(t, state.SyncRegisteredTables())
}

func prepareReferenceArray(conn *sql.DB, format string, init bool, startingNum, seqno, numCols int, testQuery string, resfmt string, result *[]string, t *testing.T) int {
	for i := startingNum; i < startingNum+10; i++ {
		vals1 := make([]interface{}, numCols)
		for j := 0; j < numCols; j++ {
			vals1[j] = i
		}

		test.ExecSQL(conn, t, testQuery, vals1...)

		sq := ^uint64(0)
		if !init {
			seqno++
			sq = uint64(seqno)
		}

		var vals2 []interface{}

		if formatSQL(format) {
			vals2 = []interface{}{sq}
		} else {
			vals2 = []interface{}{i, sq}
		}
		for j := 0; j < numCols; j++ {
			vals2 = append(vals2, i)
		}

		if formatSQL(format) {
			s := fmt.Sprintf(resfmt, vals2...)
			*result = append(*result, s)
		} else {
			s := fmt.Sprintf(resfmt, vals2...)
			*result = append(*result, s)
		}
	}

	return seqno
}

func prepareUpdateReferenceArray(conn *sql.DB, format string, seqno int, result *[]string, t *testing.T) int {
	test.ExecSQL(conn, t, updateStmt[0], 101, 111)
	for i := 101; i < 111; i++ {
		if !strings.HasSuffix(format, "_idempotent") {
			seqno++
			if format == "avro" {
				s := fmt.Sprintf(updateResult[format][0], fmt.Sprintf("\"3%v\"", i), seqno)
				*result = append(*result, s)
			} else if formatSQL(format) {
				s := fmt.Sprintf(updateResult[format][0], seqno, i)
				*result = append(*result, s)
			} else {
				s := fmt.Sprintf(updateResult[format][0], i, seqno)
				*result = append(*result, s)
			}
		}

		seqno++
		if formatSQL(format) {
			s := fmt.Sprintf(updateResult[format][1], seqno, i)
			*result = append(*result, s)
		} else {
			s := fmt.Sprintf(updateResult[format][1], i, seqno, i)
			*result = append(*result, s)
		}
	}
	return seqno
}

func prepareSchemaResult(format string, seqno int, tableNum int, f2 bool, result *[]string) {
	var f2sql, f2json string
	if f2 {
		f2sql = `"f2" varchar(32), `
		f2json = `,{"Name":"f2","Value":"varchar(32)"}`
	}
	if formatSQL(format) {
		*result = append(*result, fmt.Sprintf(`CREATE TABLE "e2e_test_table%d" ("seqno" BIGINT NOT NULL, "f1" int NOT NULL, "f3" int NOT NULL, "f4" int, %sUNIQUE KEY("seqno"), PRIMARY KEY ("f1"));`, tableNum, f2sql))
	} else if format != "avro" {
		*result = append(*result, fmt.Sprintf(`{"Type":"schema","Key":["f1"],"SeqNo":%d,"Timestamp":0,"Fields":[{"Name":"f1","Value":"int"},{"Name":"f3","Value":"int"},{"Name":"f4","Value":"int"}%s]}`, seqno, f2json))
	}
}

func testStep(inPipeType string, bufferFormat string, outPipeType string, outPipeFormat string, t *testing.T) {
	cfg := config.Get()

	cfg.Pipe.BaseDir = "/tmp/storagetapper/main_test"
	cfg.Pipe.MaxFileSize = 1 // file per message
	cfg.Pipe.FileDelimited = true
	cfg.ChangelogPipeType = inPipeType
	cfg.InternalEncoding = bufferFormat

	//json and msgpack pushes schema not wrapped into transport format, so
	//streamer won't be able to decode it unless outPipeFormat = InternalEncoding = bufferFormat
	if outPipeFormat == "json" || outPipeFormat == "msgpack" {
		cfg.InternalEncoding = outPipeFormat
		var err error
		encoder.Internal, err = encoder.InitEncoder(cfg.InternalEncoding, "", "", "", "", "", 0)
		require.NoError(t, err)
	}

	cfg.StateUpdateInterval = 50 * time.Millisecond
	cfg.LockExpireTimeout = 10 * time.Second
	cfg.WorkerIdleInterval = 1 * time.Second
	cfg.MaxNumProcs = 3

	var seqno, sseqno = changelog.SeqnoSaveInterval, changelog.SeqnoSaveInterval
	var result = make([]string, 0)
	var result2 = make([]string, 0)

	log.Debugf("STARTED STEP: inPipeType=%v, bufferFormat=%v, outPipeType=%v, outPipeFormat=%v, seqno=%v", inPipeType, bufferFormat, outPipeType, outPipeFormat, seqno)

	trackingPipe, err := pipe.Create(outPipeType, &cfg.Pipe, nil)
	require.NoError(t, err)
	trackingConsumer, err := trackingPipe.NewConsumer("hp-tap-e2e_test_svc1-e2e_test_db1-e2e_test_table1")
	require.NoError(t, err)

	initTestDB(t)

	//First message of non avro stream is schema
	prepareSchemaResult(outPipeFormat, 0, 1, false, &result)

	conn, err := db.Open(&db.Addr{Host: "localhost", Port: 3306, User: types.TestMySQLUser,
		Pwd: types.TestMySQLPassword, DB: "e2e_test_db1"})
	require.NoError(t, err)

	seqno = prepareReferenceArray(conn, outPipeFormat, true, 101, seqno, 1, inserts[0], insertsResult[outPipeFormat][0], &result, t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		mainLow(cfg)
		wg.Done()
	}()

	waitMainInitialized()

	defer func() {
		shutdown.Initiate()
		shutdown.Wait() //There is no guarantee that this wait returns after main's Wait that's why below wait for local wg
		wg.Wait()
		log.Debugf("FINISHED STEP: inPipeType=%v, bufferFormat=%v, outPipeType=%v, outPipeFormat=%v, seqno=%v", inPipeType, bufferFormat, outPipeType, outPipeFormat, seqno)
	}()

	// New consumer sees only new events, so register it before any events produced
	p, err := pipe.Create(outPipeType, &cfg.Pipe, nil)
	require.NoError(t, err)
	c, err := p.NewConsumer("hp-tap-e2e_test_svc1-e2e_test_db1-e2e_test_table1")
	require.NoError(t, err)
	c2, err := p.NewConsumer("hp-tap-e2e_test_svc1-e2e_test_db1-e2e_test_table2")
	require.NoError(t, err)

	addTable(outPipeFormat, "1", outPipeType, t)

	// Encoder/decoder for the table added above
	outEncoder, err := encoder.Create(outPipeFormat, "e2e_test_svc1", "e2e_test_db1", "e2e_test_table1", "mysql", outPipeType, 0)
	require.NoError(t, err)

	// Wait snapshot to finish before sending more data otherwise everything even following events will be read
	// from snapshot and we want them to be read from binlog
	waitSnapshotToFinish("e2e_test_table1", outPipeType, t)

	// Let's insert more rows after the snapshot finishes and wait for all rows to be consumed
	seqno = prepareReferenceArray(conn, outPipeFormat, false, 1, seqno, 1, inserts[0], insertsResult[outPipeFormat][0], &result, t)
	sseqno = waitAllEventsStreamed(outPipeFormat, trackingConsumer, sseqno, seqno)

	// Now let's test with a new column added to the test table
	test.ExecSQL(conn, t, "ALTER TABLE e2e_test_table1 ADD f2 varchar(32)")
	seqno++
	prepareSchemaResult(outPipeFormat, seqno, 1, true, &result)

	seqno = prepareReferenceArray(conn, outPipeFormat, false, 11, seqno, 2, inserts[1], insertsResult[outPipeFormat][1], &result, t)
	sseqno = waitAllEventsStreamed(outPipeFormat, trackingConsumer, sseqno, seqno)

	// Now let's drop the new column that was added above
	test.ExecSQL(conn, t, "ALTER TABLE e2e_test_table1 DROP f2")
	seqno++
	prepareSchemaResult(outPipeFormat, seqno, 1, false, &result)

	// Now let's update rows and check for existence of delete+insert messages
	seqno = prepareUpdateReferenceArray(conn, outPipeFormat, seqno, &result, t)

	// Now let's create the second table and insert some data
	test.ExecSQL(conn, t, "CREATE TABLE e2e_test_db1.e2e_test_table2(f1 int not null primary key, f3 int not null default 0, f4 int)")
	prepareSchemaResult(outPipeFormat, 0, 2, false, &result2)

	prepareReferenceArray(conn, outPipeFormat, true, 101, seqno, 1, inserts[2], insertsResult[outPipeFormat][2], &result2, t)

	seqno = prepareReferenceArray(conn, outPipeFormat, false, 222, seqno, 1, inserts[3], insertsResult[outPipeFormat][3], &result, t)
	//This also guarantees that inserts for t2 skipped by changelog reader
	waitAllEventsStreamed(outPipeFormat, trackingConsumer, sseqno, seqno)

	addTable(outPipeFormat, "2", outPipeType, t)

	// Encoder/decoder for the table added above
	outEncoder2, err := encoder.Create(outPipeFormat, "e2e_test_svc1", "e2e_test_db1", "e2e_test_table2", "mysql", outPipeType, 0)
	require.NoError(t, err)

	// Wait snapshot to finish before sending more data otherwise everything even following events will be read
	// from snapshot and we want them to be read from binlog
	waitSnapshotToFinish("e2e_test_table2", outPipeType, t)

	// Now let's insert some data in the second table
	prepareReferenceArray(conn, outPipeFormat, false, 0, seqno, 1, inserts[2], insertsResult[outPipeFormat][2], &result2, t)

	if strings.HasPrefix(outPipeFormat, "mysql") {
		convertQuotes(&result)
		convertQuotes(&result2)
	}

	log.Debugf("Start consuming events from %v", "hp-tap-e2e_test_svc1-e2e_test_db1-e2e_test_table1")
	consumeEvents(c, outPipeFormat, result, outEncoder, "1", t)

	log.Debugf("Start consuming events from %v", "hp-tap-e2e_test_svc1-e2e_test_db1-e2e_test_table2")
	consumeEvents(c2, outPipeFormat, result2, outEncoder2, "2", t)

	require.NoError(t, c.Close())
	require.NoError(t, c2.Close())

	log.Debugf("FINISHING STEP: %v, %v, %v, %v", inPipeType, bufferFormat, outPipeType, outPipeFormat)
}

func TestBasic(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)
	test.SkipIfNoKafkaAvailable(t)

	encoder.GenTime = func() int64 { return 0 }

	//FIXME: Rewrite test so it doesn't require events to come out in order

	//Configure producer so as everything will go to one partition
	pipe.InitialOffset = pipe.OffsetNewest
	pipe.KafkaConfig = sarama.NewConfig()
	pipe.KafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	pipe.KafkaConfig.Producer.Return.Successes = true
	pipe.KafkaConfig.Consumer.MaxWaitTime = 15 * time.Millisecond
	pipe.KafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	pipe.KafkaConfig.Producer.Retry.Max = 20
	pipe.KafkaConfig.Metadata.Retry.Max = 20

	for _, out := range []string{"kafka"} {
		//for out := range pipe.Pipes { //FIXME: file and hdfs fail some time
		if out == "local" {
			continue
		}
		for _, in := range []string{"local", "kafka"} {
			for _, buf := range []string{"json", "msgpack"} { //possible buffer formats
				for _, enc := range encoder.Encoders() {
					t.Run("buf_"+in+"_buffmt_"+buf+"_out_"+out+"_fmt_"+enc, func(t *testing.T) {
						if strings.Contains(enc, "sql") {
							t.Skip("Skipping sql tests")
						}
						testStep(in, buf, out, enc, t)
					})
				}
			}
		}
	}
}

func TestMain(m *testing.M) {
	_ = test.LoadConfig()

	os.Exit(m.Run())
}
