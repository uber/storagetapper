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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
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
)

var (
	cfg              *config.AppConfig
	testCluster      = "test_cluster"
	testSvc          = types.MySvcName
	testDB           = "test_db_streamer"
	testTbl          = "test_snapstream"
	testTbl1         = "test_snapstream1"
	testInput        = "mysql"
	testPipeType     = "kafka"
	testOutputFormat = "avro"
	numTestEvents    = 30

	dbl = &db.Loc{Cluster: testCluster, Service: testSvc, Name: testDB}
)

func EventWorker(cfg *config.AppConfig, inP pipe.Pipe, _ *testing.T) {
	defer shutdown.Done()
	log.Debugf("Starting Event streamer reader")
	_ = Worker(cfg, inP)
	log.Debugf("Finished Event streamer reader")
}

func execSQL(db *sql.DB, t *testing.T, query string, param ...interface{}) {
	test.CheckFail(util.ExecSQL(db, query, param...), t)
}

func createTestDB(t *testing.T) {
	ci, err := db.GetConnInfo(&db.Loc{Cluster: testCluster, Service: testSvc}, db.Slave, types.InputMySQL)
	require.NoError(t, err)

	dbConn, err := db.Open(ci)
	require.NoError(t, err)

	execSQL(dbConn, t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", types.MyDBName))
	execSQL(dbConn, t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
	execSQL(dbConn, t, fmt.Sprintf("CREATE DATABASE %s DEFAULT CHARACTER SET latin1", testDB))
	execSQL(dbConn, t, fmt.Sprintf("CREATE TABLE %s.%s (f1 BIGINT NOT NULL, f2 BIGINT NOT NULL, PRIMARY KEY(f1, f2))", testDB, testTbl))
	execSQL(dbConn, t, fmt.Sprintf("CREATE TABLE %s.%s (f1 BIGINT NOT NULL, f2 BIGINT NOT NULL, PRIMARY KEY(f1, f2))", testDB, testTbl1))
}

func setupDB(ctx context.Context, t *testing.T) *sql.DB {
	createTestDB(t)

	err := state.InitManager(ctx, cfg)
	require.NoError(t, err)

	if !state.RegisterTableInState(dbl, testTbl, "mysql", testPipeType, 0, testOutputFormat, "", 0) {
		log.Fatalf("Failed to register table")
	}

	//Output of this table should not be affected by global OutputFormat
	if !state.RegisterTableInState(dbl, testTbl1, "mysql", testPipeType, 0, "json", "", 0) {
		log.Fatalf("Failed to register table 1")
	}

	// Store Avro schema in local db
	avSch, err := schema.ConvertToAvro(&db.Loc{Service: testSvc, Name: testDB}, testTbl, testInput, "avro")
	test.Assert(t, err == nil, "Error converting TestTbl to its Avro schema: %v", err)
	tn, err := cfg.GetOutputTopicName(testSvc, testDB, testTbl, "mysql", testPipeType, 0, time.Now())
	test.CheckFail(err, t)

	err = state.InsertSchema(tn, "avro", util.BytesToString(avSch))
	test.CheckFail(err, t)
	return state.GetDB()
}

func setupDataLow(dbConn *sql.DB, shiftKey int, table string, t *testing.T) {
	for i := shiftKey; i < numTestEvents/10+shiftKey; i++ {
		for j := 0; j < 10; j++ {
			execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(%v, %v)", testDB, table, i, j))
		}
	}
}

func setupData(dbConn *sql.DB, shiftKey int, t *testing.T) {
	setupDataLow(dbConn, shiftKey, testTbl, t)
	setupDataLow(dbConn, shiftKey, testTbl1, t)
}

func wrapEvent(enc encoder.Encoder, typ string, key []interface{}, bd []byte, seqno uint64) ([]byte, error) {
	cfw := types.CommonFormatEvent{
		Type:      typ,
		Key:       key,
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

func setupBufferData(pipeType string, bufPipe pipe.Pipe, bufFormat string, outFormat string, wrap int, shiftKey int, t *testing.T) {
	for {
		row, err := state.GetTable(testSvc, testCluster, testDB, testTbl, types.InputMySQL, testPipeType, 0)
		test.CheckFail(err, t)
		if !row.NeedSnapshot {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	enc, err := encoder.Create(bufFormat, testSvc, testDB, testTbl, testInput, testPipeType, 0)
	test.CheckFail(err, t)
	outEnc, err := encoder.Create(outFormat, testSvc, testDB, testTbl, testInput, testPipeType, 0)
	test.CheckFail(err, t)

	tn, err := config.Get().GetChangelogTopicName(testSvc, testDB, testTbl, testInput, pipeType, 0, time.Now())
	test.CheckFail(err, t)
	log.Debugf("Creating buffer pipe producer using topic: %v", tn)
	producer, err := bufPipe.NewProducer(tn)
	test.CheckFail(err, t)

	seqNo := uint64(shiftKey)
	for i := shiftKey; i < numTestEvents/10+shiftKey; i++ {
		for j := 0; j < 10; j++ {
			var bd []byte

			row := &[]interface{}{int64(i), int64(j)}
			seqNo++

			if wrap == 2 {
				bd, err = enc.Row(types.Insert, row, seqNo, time.Time{})
			} else {
				bd, err = outEnc.Row(types.Insert, row, seqNo, time.Time{})
			}
			test.CheckFail(err, t)

			key := fmt.Sprintf("key.%s-%s", strconv.Itoa(i), strconv.Itoa(j))

			if wrap == 1 {
				bd, err = wrapEvent(enc, outEnc.Type(), []interface{}{key}, bd, seqNo)
				test.CheckFail(err, t)
			} else if wrap == 2 {
				bd, err = wrapEvent(enc, enc.Type(), []interface{}{key}, bd, seqNo)
				test.CheckFail(err, t)
			}

			log.Debugf("Pushing row: %+v, key: %v", row, key)
			err = producer.PushK(key, bd)
			require.NoError(t, err)
		}
	}

	bd, err := enc.EncodeSchema(10000)
	require.NoError(t, err)

	err = producer.Push(bd)
	require.NoError(t, err)

	test.CheckFail(producer.Close(), t)
}

// Set test cluster gtid and update timestamp in loop to satisfy streamer start
// conditions
// This loop is not needed once we get first messages from streamer
// It'll be stopped by writing to emulateBinlogCh in verifyFromOutputKafka
func startEmulateBinlogProgress(dbl *db.Loc, t *testing.T) chan bool {
	gtid, err := db.GetCurrentGTIDForDB(dbl, types.InputMySQL)
	test.CheckFail(err, t)

	execSQL(state.GetDB(), t, "UPDATE cluster_state SET gtid=? WHERE cluster=?", gtid, testCluster)

	ch := make(chan bool)
	shutdown.Register(1)
	go func() {
		defer shutdown.Done()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		i := 0
		for seqno := 1; !shutdown.Initiated(); seqno++ {
			select {
			case <-tick.C:
				execSQL(state.GetDB(), t, "UPDATE cluster_state SET gtid=?,seqno=? WHERE cluster=?", gtid, seqno, testCluster)
			case <-ch:
				//Wait till both streamer workers started
				i++
				if i == 2 {
					log.Debugf("Emulate binlog stopped")
					return
				}
			}
		}
	}()

	return ch
}

func setupWorker(bufPipe pipe.Pipe, outPipeType string, t *testing.T) (pipe.Consumer, pipe.Consumer) {
	outPipe, err := pipe.CacheGet(outPipeType, &cfg.Pipe, state.GetDB())
	log.F(err)

	tn, err := cfg.GetOutputTopicName(testSvc, testDB, testTbl, "mysql", outPipeType, 0, time.Now())
	test.CheckFail(err, t)

	outConsumer, err := outPipe.NewConsumer(tn)
	test.CheckFail(err, t)

	tn, err = cfg.GetOutputTopicName(testSvc, testDB, testTbl1, "mysql", outPipeType, 0, time.Now())
	test.CheckFail(err, t)

	outConsumer1, err := outPipe.NewConsumer(tn)
	test.CheckFail(err, t)

	shutdown.Register(2)
	go EventWorker(cfg, bufPipe, t)
	go EventWorker(cfg, bufPipe, t)

	return outConsumer, outConsumer1
}

func verifyFromOutputKafka(shiftKey int, outPConsumer pipe.Consumer, outFormat string, table string, ch chan bool, t *testing.T) {
	log.Debugf("verification started %v table=%v outFormat=%v", shiftKey, table, outFormat)

	enc, err := encoder.Create(outFormat, testSvc, testDB, table, testInput, testPipeType, 0)
	test.CheckFail(err, t)

	lastVal := make([]int64, 10+shiftKey)

	i := shiftKey
	for i < numTestEvents+shiftKey {
		b, err := outPConsumer.FetchNext()
		require.NoError(t, err)
		if b == nil {
			break
		}

		encodedMsg := b.([]byte)
		require.NotNil(t, encodedMsg)

		data, err := enc.DecodeEvent(encodedMsg)
		require.NoError(t, err, fmt.Sprintf("Error decoding message: err=%v msg=%v", err, string(encodedMsg)))

		log.Debugf("Read event data: %+v", data)
		require.Equal(t, 2, len(*data.Fields), "should be two fields in the table")

		if data.Type == "schema" {
			continue
		}

		if shiftKey == 0 && data.SeqNo != ^uint64(0) {
			t.Fatalf("Events out of order for data %+v when consuming %v.%v from output Kafka. "+
				"Current event seqNo(%v) != expected(0) shiftKey=%v", data, testDB, table, data.SeqNo, shiftKey)
		}

		v1, ok := (*data.Fields)[0].Value.(int64)
		require.True(t, ok, fmt.Sprintf("Field 0 in %+v should be int64", data.Fields))

		v2, ok := (*data.Fields)[1].Value.(int64)
		require.True(t, ok, fmt.Sprintf("Field 1 in %+v should be int64", data.Fields))

		prev := lastVal[int(v1)]
		if prev != 0 && prev != v2 && prev != (v2-1) {
			t.Fatalf("Event with seqNo %v received out of order when consuming from %v.%v from output kafka. "+
				"Current event f1(%v), f2(%v) != expected f1(%v), f2(%v or %v)", data.SeqNo, testDB, table, v1, v2, v1, prev, prev+1)
		}

		lastVal[int(v1)] = v2

		if i == shiftKey && ch != nil {
			log.Debugf("Stop emulate binlog loop")
			ch <- true
		}

		// increment the counter after skipping the schema message
		i++
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

	cfg.ChangelogBuffer = true
	testPipeType = pipeType
	testOutputFormat = outFormat
	cfg.Pipe.MaxBatchSize = 16

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	dbConn := setupDB(shutdown.Context, t)
	defer func() { test.CheckFail(dbConn.Close(), t) }()

	bufPipe, err := pipe.Create(pipeType, &cfg.Pipe, state.GetDB())
	test.CheckFail(err, t)

	setupData(dbConn, 0, t)

	ch := startEmulateBinlogProgress(dbl, t)

	consumer, consumer1 := setupWorker(bufPipe, pipeType, t)

	verifyFromOutputKafka(0, consumer, outFormat, testTbl, ch, t)
	verifyFromOutputKafka(0, consumer1, "json", testTbl1, ch, t)

	setupBufferData(pipeType, bufPipe, "json", outFormat, wrap, numTestEvents/10, t)

	verifyFromOutputKafka(numTestEvents/10, consumer, outFormat, testTbl, nil, t)

	test.CheckFail(consumer.Close(), t)

	log.Debugf("TestStreamer_StreamFromConsistentSnapshot %v finished", outFormat)
}

func TestStreamer_StreamFromConsistentSnapshotKafka(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot(testPipeType, 1, "avro", t)
}

func TestStreamer_StreamFromConsistentSnapshotJson(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot(testPipeType, 0, "json", t)
}

func TestStreamer_StreamFromConsistentSnapshotJsonWrapped(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot(testPipeType, 1, "json", t)
}

func TestStreamer_StreamFromConsistentSnapshotJsonWrappedWithAvroOutput(t *testing.T) {
	testStreamerStreamFromConsistentSnapshot(testPipeType, 2, "avro", t)
}

func TestStreamerShutdown(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)
	test.SkipIfNoKafkaAvailable(t)

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	saved := cfg.StateUpdateInterval
	cfg.StateUpdateInterval = 1 * time.Second

	dbConn := setupDB(shutdown.Context, t)
	defer func() { test.CheckFail(dbConn.Close(), t) }()

	bufPipe, err := pipe.Create("local", &cfg.Pipe, nil)
	require.NoError(t, err)

	execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(1, 1)", testDB, testTbl))
	execSQL(dbConn, t, fmt.Sprintf("insert into %s.%s values(1, 1)", testDB, testTbl1))

	ch := startEmulateBinlogProgress(dbl, t)

	consumer, consumer1 := setupWorker(bufPipe, testPipeType, t)

	b, err := consumer.FetchNext()
	test.CheckFail(err, t)
	require.NotNil(t, b)
	ch <- true

	b1, err := consumer1.FetchNext()
	test.CheckFail(err, t)
	require.NotNil(t, b1)
	ch <- true

	if !test.WaitForNumProc(3, 16*time.Second) {
		t.Fatalf("Streamer worker didn't finish. Num procs %v", shutdown.NumProcs())
	}

	if !state.DeregisterTableFromState(&db.Loc{Service: testSvc, Cluster: testCluster, Name: testDB}, testTbl, "mysql", testPipeType, 0, 1) {
		t.Fatalf("Failed to deregister table")
	}

	if !state.DeregisterTableFromState(&db.Loc{Service: testSvc, Cluster: testCluster, Name: testDB}, testTbl1, "mysql", testPipeType, 0, 1) {
		t.Fatalf("Failed to deregister table 1")
	}

	if !test.WaitForNumProc(1, 16*time.Second) {
		t.Fatalf("Streamer worker didn't finish. Num procs %v", shutdown.NumProcs())
	}

	test.CheckFail(consumer.Close(), t)
	test.CheckFail(consumer1.Close(), t)

	if !test.WaitForNumProc(1, 16*time.Second) {
		t.Fatalf("Streamer worker didn't finish")
	}

	cfg.StateUpdateInterval = saved

	shutdown.Initiate()
	shutdown.Wait()
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()

	encoder.GetLatestSchema = getSchemaTest
	cancelCheckInterval = 1 * time.Second

	pipe.KafkaConfig = sarama.NewConfig()
	pipe.KafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	pipe.KafkaConfig.Producer.Return.Successes = true
	pipe.KafkaConfig.Consumer.MaxWaitTime = 15 * time.Millisecond
	pipe.KafkaConfig.Producer.RequiredAcks = sarama.WaitForAll

	cfg.ClusterConcurrency = 100
	cfg.ThrottleTargetIOPS = 5
	cfg.Pipe.BaseDir = "/tmp/storagetapper/streamer_test"
	cfg.StateUpdateInterval = 1 * time.Second

	if !test.MySQLAvailable() {
		log.Warnf("MySQL is not available")
		os.Exit(0)
	}

	os.Exit(m.Run())
}
