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

package streamer

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"

	_ "github.com/go-sql-driver/mysql"
)

var (
	cfg              *config.AppConfig
	testSvc          = types.MySvcName
	testDb           = types.MyDbName
	testTbl          = "test_snapstream"
	testTbl1         = "test_snapstream1"
	testPipeType     = "kafka"
	testOutputFormat = "avro"
)

func EventWorker(cfg *config.AppConfig, inP pipe.Pipe, outP map[string]pipe.Pipe, t *testing.T) {
	defer shutdown.Done()
	log.Debugf("Starting Event streamer reader")
	_ = Worker(cfg, inP, &outP)
	log.Debugf("Finished Event streamer reader")
}

func execSQL(db *sql.DB, t *testing.T, query string, param ...interface{}) {
	test.CheckFail(util.ExecSQL(db, query, param...), t)
}

func getDBConn(ci db.Addr, t *testing.T) *sql.DB {
	dbConn, err := db.Open(&ci)
	test.CheckFail(err, t)
	return dbConn
}

func createTestDB(dbConn *sql.DB, t *testing.T) {
	execSQL(dbConn, t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", types.MyDbName))
	execSQL(dbConn, t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDb))
	execSQL(dbConn, t, fmt.Sprintf("CREATE DATABASE %s", testDb))
	execSQL(dbConn, t, fmt.Sprintf("CREATE TABLE %s.%s "+
		"( f1 BIGINT NOT NULL PRIMARY KEY, f2 VARCHAR(32) )", testDb, testTbl))
	execSQL(dbConn, t, fmt.Sprintf("CREATE TABLE %s.%s "+
		"( f1 BIGINT NOT NULL PRIMARY KEY, f2 VARCHAR(32) )", testDb, testTbl1))
}

func setupDB(t *testing.T) *sql.DB {
	ci := db.GetInfo(&db.Loc{Service: testSvc, Name: testDb}, db.Slave)
	dbConn := getDBConn(*ci, t)
	createTestDB(dbConn, t)
	if !state.Init(cfg) {
		t.FailNow()
	}
	dbl := &db.Loc{Service: testSvc, Name: testDb}

	if !state.RegisterTable(dbl, testTbl, "mysql", testPipeType, 0, testOutputFormat) {
		log.Fatalf("Failed to register table")
	}
	//Output of this table should not be affected by global OutputFormat
	if !state.RegisterTable(dbl, testTbl1, "mysql", testPipeType, 0, "json") {
		log.Fatalf("Failed to register table")
	}

	gtid, err := state.GetCurrentGTIDForDB(dbl)
	test.CheckFail(err, t)

	execSQL(state.GetDB(), t, "UPDATE state SET gtid=? WHERE tableName=? AND service=? AND db=?", gtid, testTbl, testSvc, testDb)
	execSQL(state.GetDB(), t, "UPDATE state SET gtid=? WHERE tableName=? AND service=? AND db=?", gtid, testTbl1, testSvc, testDb)

	// Store Avro schema in local db
	avSch, err := schema.ConvertToAvro(&db.Loc{Service: testSvc, Name: testDb}, testTbl, "avro")
	test.Assert(t, err == nil, "Error converting testTbl to its Avro schema: %v", err)
	tn, err := cfg.GetOutputTopicName(testSvc, testDb, testTbl, "mysql", testPipeType, 0)
	test.CheckFail(err, t)

	err = state.InsertSchema(tn, "avro", util.BytesToString(avSch))
	test.CheckFail(err, t)
	return dbConn
}

func setupDataLow(dbConn *sql.DB, shiftKey int, table string, t *testing.T) {
	for i := shiftKey; i < 100+shiftKey; i++ {
		execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(%v,'%v')", testDb, table, i, strconv.Itoa(i)))
	}
}

func setupData(dbConn *sql.DB, shiftKey int, t *testing.T) {
	setupDataLow(dbConn, shiftKey, testTbl, t)
	setupDataLow(dbConn, shiftKey, testTbl1, t)
}

func wrapEvent(enc encoder.Encoder, typ string, key string, bd []byte, seqno uint64) ([]byte, error) {
	akey := make([]interface{}, 1)
	akey[0] = key

	cfw := types.CommonFormatEvent{
		Type:      typ,
		Key:       akey,
		SeqNo:     seqno,
		Timestamp: time.Now().UnixNano(),
		Fields:    nil,
	}

	cfb, err := enc.CommonFormat(&cfw)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(cfb)
	buf.Write(bd)

	return buf.Bytes(), nil
}

func setupBufferData(producer pipe.Producer, bufFormat string, outFormat string, wrap int, shiftKey int, t *testing.T) {
	enc, err := encoder.Create(bufFormat, testSvc, testDb, testTbl)
	test.CheckFail(err, t)
	outEnc, err := encoder.Create(outFormat, testSvc, testDb, testTbl)
	test.CheckFail(err, t)
	for i := shiftKey; i < 100+shiftKey; i++ {
		var bd []byte
		var err error
		if wrap == 2 {
			bd, err = enc.Row(types.Insert, &[]interface{}{int64(i), strconv.Itoa(i)}, uint64(i))
		} else {
			bd, err = outEnc.Row(types.Insert, &[]interface{}{int64(i), strconv.Itoa(i)}, uint64(i))
		}
		test.CheckFail(err, t)
		if wrap == 1 {
			bd, err = wrapEvent(enc, outEnc.Type(), "key", bd, uint64(i))
			test.CheckFail(err, t)
		} else if wrap == 2 {
			bd, err = wrapEvent(enc, enc.Type(), "key", bd, uint64(i))
			test.CheckFail(err, t)
		}
		err = producer.Push(bd)
		test.CheckFail(err, t)
	}

	bd, err := enc.EncodeSchema(10000)
	test.CheckFail(err, t)
	err = producer.Push(bd)
	test.CheckFail(err, t)
}

func setupWorker(bufPipe pipe.Pipe, outPipeType string, t *testing.T) (pipe.Consumer, pipe.Consumer) {
	var err error
	outPipe := make(map[string]pipe.Pipe)
	for p := range pipe.Pipes {
		outPipe[p], err = pipe.Create(shutdown.Context, p, cfg.PipeBatchSize, cfg, state.GetDB())
		log.F(err)
	}

	tn, err := cfg.GetOutputTopicName(testSvc, testDb, testTbl, "mysql", testPipeType, 0)
	test.CheckFail(err, t)

	outConsumer, err := outPipe[outPipeType].NewConsumer(tn)
	test.CheckFail(err, t)

	tn, err = cfg.GetOutputTopicName(testSvc, testDb, testTbl1, "mysql", testPipeType, 0)
	test.CheckFail(err, t)

	outConsumer1, err := outPipe[outPipeType].NewConsumer(tn)
	test.CheckFail(err, t)

	shutdown.Register(2)
	go EventWorker(cfg, bufPipe, outPipe, t)
	go EventWorker(cfg, bufPipe, outPipe, t)

	return outConsumer, outConsumer1
}

func verifyFromOutputKafka(shiftKey int, outPConsumer pipe.Consumer, outFormat string, table string, t *testing.T) {
	log.Debugf("verification started %v table=%v outFormat=%v", shiftKey, table, outFormat)

	enc, err := encoder.Create(outFormat, testSvc, testDb, table)
	test.CheckFail(err, t)

	lastRKey := uint64(shiftKey)
	lastFKey := uint64(shiftKey)
	for lastFKey < uint64(100+shiftKey) && outPConsumer.FetchNext() {
		b, err := outPConsumer.Pop()
		encodedMsg := b.([]byte)
		test.CheckFail(err, t)
		if encodedMsg == nil {
			t.FailNow()
		}
		data, err := enc.DecodeEvent(encodedMsg)
		test.Assert(t, err == nil, "Error decoding message: err=%v msg=%v", err, string(encodedMsg))
		if (shiftKey != 0 && data.SeqNo != lastRKey) || (shiftKey == 0 && data.SeqNo != 0) {
			t.Fatalf("Events out of order when consuming from output Kafka! "+
				"Current event ref key(%v) != expected (%v) shiftKey=%v",
				data.SeqNo, lastRKey, shiftKey)
		}
		if data.Type == "schema" {
			lastRKey++
			continue
		}
		test.Assert(t, len(*data.Fields) == 2, "should be two fields in the table")
		r, ok := (*data.Fields)[0].Value.(int64)
		test.Assert(t, ok, "should be int64")
		if uint64(r) != lastFKey /*|| (*data.Fields)[0].Name != "f1"*/ {
			t.Fatalf("Events out of order when consuming from output Kafka! "+
				"Current event f1 field(%v) != expected (%v) %v",
				(*data.Fields)[0].Value, lastFKey, (*data.Fields)[0].Name)
		}
		lastRKey++
		lastFKey++
	}
	log.Debugf("verification finished %v", shiftKey)
}

// getSchemaTest: for testing purposes fetches avro schema from local db
func getSchemaTest(namespace string, schemaName string, typ string) (*types.AvroSchema, error) {
	var err error
	var a *types.AvroSchema

	s := state.GetOutputSchema(schemaName, typ)
	if s == "" {
		return nil, fmt.Errorf("schema not found")
	}

	a = &types.AvroSchema{}
	err = json.Unmarshal([]byte(s), a)

	return a, err
}

func testStreamerStreamFromConsistentSnapshot(pipeType string, wrap int, outFormat string, t *testing.T) {
	log.Debugf("TestStreamer_StreamFromConsistentSnapshot %v", outFormat)
	test.SkipIfNoMySQLAvailable(t)
	test.SkipIfNoKafkaAvailable(t)

	testPipeType = pipeType
	testOutputFormat = outFormat

	shutdown.Setup()

	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	dbConn := setupDB(t)
	defer func() { test.CheckFail(dbConn.Close(), t) }()

	bufPipe, err := pipe.Create(shutdown.Context, pipeType, 16, cfg, nil)
	test.CheckFail(err, t)
	tn, err := config.Get().GetChangelogTopicName(testSvc, testDb, testTbl, "mysql", testPipeType, 0)
	test.CheckFail(err, t)
	producer, err := bufPipe.NewProducer(tn)
	test.CheckFail(err, t)

	setupData(dbConn, 0, t)

	consumer, consumer1 := setupWorker(bufPipe, pipeType, t)

	verifyFromOutputKafka(0, consumer, outFormat, testTbl, t)
	verifyFromOutputKafka(0, consumer1, "json", testTbl1, t)

	setupBufferData(producer, "json", outFormat, wrap, 100, t)

	verifyFromOutputKafka(100, consumer, outFormat, testTbl, t)

	test.CheckFail(consumer.Close(), t)
	test.CheckFail(producer.Close(), t)

}

func TestStreamer_StreamFromConsistentSnapshotKafka(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot("kafka", 1, "avro", t)
}

func TestStreamer_StreamFromConsistentSnapshotJson(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot("kafka", 0, "json", t)
}

func TestStreamer_StreamFromConsistentSnapshotJsonWrapped(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot("kafka", 1, "json", t)
}

func TestStreamer_StreamFromConsistentSnapshotJsonWrappedWithAvroOutput(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot("kafka", 2, "avro", t)
}

func TestStreamer_BadOutputPipe(t *testing.T) {
}

func TestStreamerShutdown(t *testing.T) {
	log.Debugf("TestStreamerShutdown")
	test.SkipIfNoMySQLAvailable(t)
	test.SkipIfNoKafkaAvailable(t)

	shutdown.Setup()
	save := cfg.StateUpdateTimeout
	cfg.StateUpdateTimeout = 1
	dbConn := setupDB(t)
	defer func() { test.CheckFail(dbConn.Close(), t) }()

	bufPipe, err := pipe.Create(shutdown.Context, "local", 16, cfg, nil)
	test.CheckFail(err, t)

	outConsumer, outConsumer1 := setupWorker(bufPipe, "kafka", t)

	execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(0,'0')", testDb, testTbl))
	execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(0,'0')", testDb, testTbl1))

	if outConsumer.FetchNext() {
		_, err := outConsumer.Pop()
		test.CheckFail(err, t)
	} else {
		t.Fatalf("FetchNext failed")
	}

	if outConsumer1.FetchNext() {
		_, err := outConsumer1.Pop()
		test.CheckFail(err, t)
	} else {
		t.Fatalf("FetchNext failed")
	}

	if !test.WaitForNumProc(3, 80*200) {
		t.Fatalf("Streamer worker didn't finish %v", shutdown.NumProcs())
	}

	if !state.DeregisterTable("storagetapper", "storagetapper", "test_snapstream", "mysql", testPipeType, 0) {
		t.Fatalf("Failed to deregister table")
	}

	if !state.DeregisterTable("storagetapper", "storagetapper", "test_snapstream1", "mysql", testPipeType, 0) {
		t.Fatalf("Failed to deregister table 1")
	}

	if !test.WaitForNumProc(1, 80*200) {
		t.Fatalf("Streamer worker didn't finish %v", shutdown.NumProcs())
	}

	test.CheckFail(outConsumer.Close(), t)
	test.CheckFail(outConsumer1.Close(), t)

	if !test.WaitForNumProc(1, 80*200) {
		t.Fatalf("Streamer worker didn't finish")
	}

	cfg.StateUpdateTimeout = save

	shutdown.Initiate()
	shutdown.Wait()
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()

	encoder.GetLatestSchema = getSchemaTest

	pipe.KafkaConfig = sarama.NewConfig()
	pipe.KafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	pipe.KafkaConfig.Producer.Return.Successes = true
	pipe.KafkaConfig.Consumer.MaxWaitTime = 10 * time.Millisecond

	cfg.ClusterConcurrency = 100
	cfg.ThrottleTargetIOPS = 5
	cfg.DataDir = "/tmp/storagetapper/streamer_test"
	cfg.StateUpdateTimeout = 1

	DbConn, err := db.OpenService(&db.Loc{Service: testSvc, Name: testDb}, "")
	if err != nil {
		log.Warnf("MySQL is not available")
		os.Exit(0)
	}
	defer func() {
		err := DbConn.Close()
		if err != nil {
			log.Fatalf("Close failed")
		}
	}()
	os.Exit(m.Run())
}
