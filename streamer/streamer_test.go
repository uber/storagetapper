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

const (
	//Testing purposes
	schemaSvcTbl = "schemasvc"
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

	if !state.RegisterTable(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl, "mysql", cfg.OutputPipeType) {
		log.Fatalf("Failed to register table")
	}
	execSQL(state.GetDB(), t, "UPDATE state SET gtid='fake-gtid' WHERE tableName=? AND service=? AND db=?", TestTbl, TestSvc, TestDb)

	// Store Avro schema in local db
	avSch, err := schema.ConvertToAvro(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl)
	test.Assert(t, err == nil, "Error converting TestTbl to its Avro schema: %v", err)
	execSQL(dbConn, t, fmt.Sprintf("INSERT INTO %s.%s (topic, avroschema) VALUES ('%v', '%v')",
		TestDb, SchemaSvcTbl, cfg.GetOutputTopicName(TestSvc, TestDb, TestTbl), util.BytesToString(avSch)))
	test.CheckFail(err, t)
	return dbConn
}

func setupData(dbConn *sql.DB, t *testing.T) {
	for i := 0; i < 1000; i++ {
		execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(%v,'%v')", TestDb, TestTbl, i, strconv.Itoa(i)))
	}
}

func setupWorker(t *testing.T) pipe.Consumer {
	inP, err := pipe.Create(shutdown.Context, "local", 16, cfg, nil)
	test.CheckFail(err, t)

	outP := make(map[string]pipe.Pipe)
	for p := range pipe.Pipes {
		outP[p], err = pipe.Create(shutdown.Context, p, cfg.PipeBatchSize, cfg, state.GetDB())
		log.F(err)
	}

	pipe.KafkaConfig = sarama.NewConfig()
	pipe.KafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	pipe.KafkaConfig.Producer.Return.Successes = true
	pipe.KafkaConfig.Consumer.MaxWaitTime = 10 * time.Millisecond

	outPConsumer, err := outP["kafka"].NewConsumer(cfg.GetOutputTopicName(TestSvc, TestDb, TestTbl))

	test.CheckFail(err, t)
	shutdown.Register(1)
	go EventWorker(cfg, inP, outP, t)
	return outPConsumer
}

func verifyFromOutputKafka(outPConsumer pipe.Consumer, t *testing.T) {
	codec, _, err := encoder.GetLatestSchemaCodec(TestSvc, TestDb, TestTbl)
	test.Assert(t, err == nil, "Error getting codec for avro decoding: %v", err)
	defer func() { test.CheckFail(outPConsumer.Close(), t) }()

	lastRKey := int64(0)
	for i := 0; i < 1000 && outPConsumer.FetchNext(); i++ {
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
		if r >= lastRKey {
			lastRKey = r
		} else {
			t.Errorf("Events out of order when consuming from output Kafka! "+
				"Current event ref key(%v) lower than previous highest ref key (%v)",
				r, lastRKey)
		}
	}
}

// getSchemaTest: for testing purposes fetches avro schema from local db
func getSchemaTest(namespace string, schemaName string) (*types.AvroSchema, error) {
	var avSchStr string
	var avSch types.AvroSchema
	dbConn, err := db.OpenService(&db.Loc{Service: types.MySvcName, Name: types.MyDbName}, "")
	if err != nil {
		return nil, err
	}
	resRows, err := util.QuerySQL(dbConn, fmt.Sprintf("SELECT AVROSCHEMA FROM %s.%s WHERE TOPIC='%s'",
		types.MySvcName, schemaSvcTbl, schemaName))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resRows.Close() }()
	if !resRows.Next() {
		return nil, fmt.Errorf("No result for schema fetch for %v from local test db", schemaName)
	}
	err = resRows.Scan(&avSchStr)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(avSchStr), &avSch)
	if err != nil {
		return nil, err
	}
	return &avSch, nil
}

func TestStreamer_StreamFromConsistentSnapshot(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)
	test.SkipIfNoKafkaAvailable(t)

	shutdown.Setup()
	dbConn := setupDB(t)
	defer func() { test.CheckFail(dbConn.Close(), t) }()
	setupData(dbConn, t)
	pc := setupWorker(t)

	log.Debugf("Finishing")
	verifyFromOutputKafka(pc, t)
	shutdown.Initiate()
	shutdown.Wait()
}

func TestStreamerShutdown(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)
	test.SkipIfNoKafkaAvailable(t)

	shutdown.Setup()
	save := cfg.StateUpdateTimeout
	cfg.StateUpdateTimeout = 1
	dbConn := setupDB(t)
	defer func() { test.CheckFail(dbConn.Close(), t) }()

	outConsumer := setupWorker(t)

	execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(0,'0')", TestDb, TestTbl))

	if outConsumer.FetchNext() {
		_, err := outConsumer.Pop()
		test.CheckFail(err, t)
	} else {
		t.Fatalf("FetchNext failed")
	}

	if !state.DeregisterTable("storagetapper", "storagetapper", "test_snapstream") {
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
	pipe.InitialOffset = pipe.OffsetNewest
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
