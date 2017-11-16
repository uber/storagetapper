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
	"github.com/linkedin/goavro"
)

var (
	cfg          *config.AppConfig
	TestSvc      = types.MySvcName
	TestDb       = types.MyDbName
	SchemaSvcTbl = "schemasvc"
	TestTbl      = "test_snapstream"
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
	execSQL(dbConn, t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", TestDb))
	execSQL(dbConn, t, fmt.Sprintf("CREATE DATABASE %s", TestDb))
	execSQL(dbConn, t, fmt.Sprintf("CREATE TABLE %s.%s "+
		"( f1 INT NOT NULL PRIMARY KEY, f2 VARCHAR(32) )", TestDb, TestTbl))
	execSQL(dbConn, t, fmt.Sprintf("CREATE TABLE %s.%s "+
		"( topic VARCHAR(96) NOT NULL PRIMARY KEY, avroschema TEXT )", TestDb, SchemaSvcTbl))
}

func setupDB(t *testing.T) *sql.DB {
	ci := db.GetInfo(&db.Loc{Service: TestSvc, Name: TestDb}, db.Slave)
	dbConn := getDBConn(*ci, t)
	createTestDB(dbConn, t)
	if !state.Init(cfg) {
		t.FailNow()
	}

	if !state.RegisterTable(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl, "mysql", cfg.OutputPipeType, 0) {
		log.Fatalf("Failed to register table")
	}
	execSQL(state.GetDB(), t, "UPDATE state SET gtid='fake-gtid' WHERE tableName=? AND service=? AND db=?", TestTbl, TestSvc, TestDb)

	// Store Avro schema in local db
	avSch, err := schema.ConvertToAvro(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl, "avro")
	test.Assert(t, err == nil, "Error converting TestTbl to its Avro schema: %v", err)
	err = state.InsertSchema(cfg.GetOutputTopicName(TestSvc, TestDb, TestTbl, 0), "avro", util.BytesToString(avSch))
	test.CheckFail(err, t)
	return dbConn
}

func setupData(dbConn *sql.DB, shiftKey int, t *testing.T) {
	for i := shiftKey; i < 1000+shiftKey; i++ {
		execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(%v,'%v')", TestDb, TestTbl, i, strconv.Itoa(i)))
	}
}

func setupBufferData(producer pipe.Producer, shiftKey int, t *testing.T) {
	enc, err := encoder.Create("json", TestSvc, TestDb, TestTbl)
	test.CheckFail(err, t)
	for i := shiftKey; i < 1000+shiftKey; i++ {
		bd, err := enc.Row(types.Insert, &[]interface{}{i, strconv.Itoa(i)}, uint64(i))
		test.CheckFail(err, t)
		err = producer.Push(bd)
		test.CheckFail(err, t)
	}
}

func setupWorker(bufPipe pipe.Pipe, t *testing.T) pipe.Consumer {
	var err error
	outPipe := make(map[string]pipe.Pipe)
	for p := range pipe.Pipes {
		outPipe[p], err = pipe.Create(shutdown.Context, p, cfg.PipeBatchSize, cfg, state.GetDB())
		log.F(err)
	}

	outConsumer, err := outPipe["kafka"].NewConsumer(cfg.GetOutputTopicName(TestSvc, TestDb, TestTbl, 0))

	test.CheckFail(err, t)
	shutdown.Register(1)
	go EventWorker(cfg, bufPipe, outPipe, t)
	return outConsumer
}

func verifyFromOutputKafka(shiftKey int, outPConsumer pipe.Consumer, t *testing.T) {
	log.Debugf("verification started %v", shiftKey)
	codec, _, err := encoder.GetLatestSchemaCodec(TestSvc, TestDb, TestTbl, "avro")
	test.Assert(t, err == nil, "Error getting codec for avro decoding: %v", err)

	lastRKey := int64(shiftKey)
	for i := shiftKey; i < 1000+shiftKey && outPConsumer.FetchNext(); i++ {
		b, err := outPConsumer.Pop()
		encodedMsg := b.([]byte)
		test.CheckFail(err, t)
		if encodedMsg == nil {
			t.FailNow()
		}
		data, err := codec.Decode(bytes.NewBuffer(encodedMsg))
		test.Assert(t, err == nil, "Error decoding Avro message using schema: %v", err)
		record := data.(*goavro.Record)
		rKey, err := record.Get("ref_key")
		test.Assert(t, err == nil, "Error fetching ref key from decoded Avro record: %v", err)
		r := rKey.(int64)
		if (shiftKey != 0 && r != lastRKey) || (shiftKey == 0 && r != 0) {
			t.Errorf("Events out of order when consuming from output Kafka! "+
				"Current event ref key(%v) != expected (%v)",
				r, lastRKey)
		}
		f1, err := record.Get("f1")
		test.Assert(t, err == nil, "Error fetching f1 key from decoded Avro record: %v", err)
		f := int64(f1.(int32))
		if f != lastRKey {
			t.Errorf("Events out of order when consuming from output Kafka! "+
				"Current event f1 field(%v) != expected (%v)",
				f, lastRKey)
		}
		lastRKey++
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

func TestStreamer_StreamFromConsistentSnapshot(t *testing.T) {
	log.Debugf("TestStreamer_StreamFromConsistentSnapshot")
	test.SkipIfNoMySQLAvailable(t)
	test.SkipIfNoKafkaAvailable(t)

	shutdown.Setup()

	dbConn := setupDB(t)
	defer func() { test.CheckFail(dbConn.Close(), t) }()

	bufPipe, err := pipe.Create(shutdown.Context, "kafka", 16, cfg, nil)
	test.CheckFail(err, t)
	producer, err := bufPipe.NewProducer(config.GetTopicName(cfg.ChangelogTopicNameFormat, TestSvc, TestDb, TestTbl, 0))
	test.CheckFail(err, t)

	setupData(dbConn, 0, t)

	consumer := setupWorker(bufPipe, t)

	verifyFromOutputKafka(0, consumer, t)

	setupBufferData(producer, 1000, t)

	verifyFromOutputKafka(1000, consumer, t)

	test.CheckFail(consumer.Close(), t)
	test.CheckFail(producer.Close(), t)

	shutdown.Initiate()
	shutdown.Wait()
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

	outConsumer := setupWorker(bufPipe, t)

	execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(0,'0')", TestDb, TestTbl))

	if outConsumer.FetchNext() {
		_, err := outConsumer.Pop()
		test.CheckFail(err, t)
	} else {
		t.Fatalf("FetchNext failed")
	}

	if !state.DeregisterTable("storagetapper", "storagetapper", "test_snapstream", "mysql", cfg.OutputPipeType, 0) {
		t.Fatalf("Failed to deregister table")
	}

	if !test.WaitForNumProc(1, 80*200) {
		t.Fatalf("Streamer worker didn't finish %v", shutdown.NumProcs())
	}

	test.CheckFail(outConsumer.Close(), t)

	if !test.WaitForNumProc(1, 80*200) {
		t.Fatalf("Streamer worker didn't finish")
	}

	cfg.StateUpdateTimeout = save

	shutdown.Initiate()
	shutdown.Wait()
}

func TestMain(m *testing.M) {
	encoder.GetLatestSchema = getSchemaTest

	pipe.KafkaConfig = sarama.NewConfig()
	pipe.KafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	pipe.KafkaConfig.Producer.Return.Successes = true
	pipe.KafkaConfig.Consumer.MaxWaitTime = 10 * time.Millisecond

	cfg = test.LoadConfig()
	DbConn, err := db.OpenService(&db.Loc{Service: TestSvc, Name: TestDb}, "")
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
