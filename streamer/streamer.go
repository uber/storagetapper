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
	"fmt"
	"golang.org/x/net/context" //"context"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/uber/storagetapper/binlog"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/lock"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
)

//Streamer struct defines common properties of Event streamer worker
type Streamer struct {
	//TODO: Convert to db.Loc
	cluster string
	svc     string
	db      string
	table   string

	topic       string
	id          int64
	inPipe      pipe.Pipe
	outPipe     pipe.Pipe
	outProducer pipe.Producer
	encoder     encoder.Encoder
	log         log.Logger

	metrics      *metrics.Streamer
	BytesWritten int64
	BytesRead    int64

	outputFormat       string
	stateUpdateTimeout int
	batchSize          int
	lock               lock.Lock
}

// ensureBinlogReaderStart ensures that Binlog reader worker has started publishing to Kafka buffer
// This is important in the case of bootstrap where we start streaming from the consistent snapshot
// and need to be sure that binlog reader has started producing events for this table.
func (s *Streamer) ensureBinlogReaderStart() (string, error) {
	tblStr := fmt.Sprintf("svc: %s, db: %s, tbl: %s", s.svc, s.db, s.table)
	log.Debugf("Waiting for Binlog reader to start publishing for %s", tblStr)
	tickChan := time.NewTicker(time.Millisecond * 1000).C
	for {
		select {
		case <-tickChan:
			sRows, err := state.GetTable(s.id)
			if len(sRows) == 0 {
				return "", fmt.Errorf("State DB has no rows for %s", tblStr)
			} else if err != nil {
				return "", err
			}
			if sRows[0].Gtid != "" {
				log.Debugf("Binlog reader confirmed started for %s from %v", tblStr, sRows[0].Gtid)
				return sRows[0].Gtid, nil
			}
		case <-shutdown.InitiatedCh():
			return "", fmt.Errorf("Shutdown in progress")
		default:
		}
	}
}

func (s *Streamer) waitForGtid(svc string, sdb string, gtid string) bool {
	var current mysql.GTIDSet

	log.Debugf("Waiting for snapshot server to catch up to: %v", gtid)

	target, err := mysql.ParseGTIDSet("mysql", gtid)
	if log.EL(s.log, err) {
		return false
	}

	conn, err := db.OpenService(&db.Loc{Service: svc, Name: sdb}, "")
	if log.EL(s.log, err) {
		return false
	}
	defer func() { log.EL(s.log, conn.Close()) }()

	tickChan := time.NewTicker(time.Millisecond * 1000).C
	for {
		err = conn.QueryRow("SELECT @@global.gtid_executed").Scan(&gtid)
		if log.EL(s.log, err) {
			return false
		}
		current, err = mysql.ParseGTIDSet("mysql", gtid)
		if log.EL(s.log, err) {
			return false
		}
		if current.Contain(target) {
			break
		}
		select {
		case <-tickChan:
		case <-shutdown.InitiatedCh():
			return false
		default:
		}
	}

	log.Debugf("Snapshot server at: %v", current)

	return true
}

func (s *Streamer) startBootstrap(needsBootstrap bool, bootstrapCh chan bool, cfg *config.AppConfig) bool {
	if needsBootstrap {
		if cfg.ConcurrentBootstrap {
			s.log.Debugf("Starting concurrent snapshot")
			shutdown.Register(1)
			go func() {
				defer shutdown.Done()
				bootstrapCh <- s.streamFromConsistentSnapshot(true, cfg.ThrottleTargetMB, cfg.ThrottleTargetIOPS)
			}()
		} else {
			if !s.streamFromConsistentSnapshot(false, cfg.ThrottleTargetMB, cfg.ThrottleTargetIOPS) {
				return false
			}
		}
	}

	return true
}

func (s *Streamer) start(cfg *config.AppConfig, outPipes map[string]pipe.Pipe) bool {
	// Fetch Lock on a service-db-table entry in State.
	// Currently, each event streamer worker handles a single table.
	//TODO: Handle multiple tables per event streamer worker in future
	var st state.Type
	var err error

	log.Debugf("Started streamer thread")

	if cfg.ReaderPipeType == "local" {
		st, err = state.GetForCluster(binlog.ThisInstanceCluster())
	} else {
		st, err = state.Get()
	}
	if err != nil {
		log.Errorf("Error reading state: %v", err.Error())
	}

	s.lock = lock.Create(state.GetDbAddr(), cfg.OutputPipeConcurrency)

	for _, row := range st {
		if s.lock.Lock("service." + row.Service + ".db." + row.Db + ".table." + row.Table) {
			s.cluster = row.Cluster
			s.svc = row.Service
			s.db = row.Db
			s.table = row.Table
			s.id = row.ID
			s.outPipe = outPipes[row.Output]
			if s.outPipe == nil {
				log.Errorf("Unknown output pipe type: %v", row.Output)
				return true
			}
			defer s.lock.Unlock()
			break
		}
	}

	//If unable to take a lock, return back
	if s.table == "" {
		log.Debugf("Finished streamer: No free tables to work on")
		return false
	}

	sTag := s.getTag()
	s.metrics = metrics.GetStreamerMetrics(sTag)
	log.Debugf("Initializing metrics for streamer: Cluster: %s, DB: %s, Table: %s -- Tags: %v",
		s.cluster, s.db, s.table, sTag)

	s.metrics.NumWorkers.Inc()
	defer s.metrics.NumWorkers.Dec()

	s.log = log.WithFields(log.Fields{"service": s.svc, "db": s.db, "table": s.table})

	// Event Streamer worker has successfully acquired a lock on a table. Proceed further
	// Each Event Streamer handles events from all partitions from Input buffer for a table
	s.topic = cfg.GetOutputTopicName(s.svc, s.db, s.table)
	s.batchSize = cfg.PipeBatchSize

	log.Debugf("Will be streaming to topic: %v", s.topic)

	s.outProducer, err = s.outPipe.RegisterProducer(s.topic)
	if log.E(err) {
		return false
	}
	defer func() { log.EL(s.log, s.outPipe.CloseProducer(s.outProducer)) }()

	// Ensures that some binlog reader worker has started reading log events for the cluster on
	// which the table resides.
	gtid, err := s.ensureBinlogReaderStart()
	if err != nil {
		return false
	}

	s.waitForGtid(s.svc, s.db, gtid)

	s.outputFormat = cfg.OutputFormat
	s.stateUpdateTimeout = cfg.StateUpdateTimeout

	s.encoder, err = encoder.Create(s.outputFormat, s.svc, s.db, s.table)
	if log.EL(s.log, err) {
		return false
	}

	// Checks whether table is new and needs bootstrapping.
	// Stream events by invoking Consistent Snapshot Reader and allowing it to complete
	needsBootstrap, err := state.GetTableNewFlag(s.svc, s.db, s.table)
	if log.EL(s.log, err) {
		return false
	}

	//Consumer should registered before snapshot started, so it sees all the
	//event during the snapshot
	ctx, cancel := context.WithCancel(context.Background())
	consumer, err := s.inPipe.RegisterConsumerCtx(ctx, config.GetTopicName(cfg.BufferTopicNameFormat, s.svc, s.db, s.table))
	if log.EL(s.log, err) {
		cancel()
		return false
	}

	bootstrapCh := make(chan bool)
	if !s.startBootstrap(needsBootstrap, bootstrapCh, cfg) {
		log.E(s.inPipe.CloseConsumer(consumer, false))
		return false
	}

	s.StreamTable(consumer, cancel, bootstrapCh)

	log.Debugf("Finished streamer")

	return true
}

// Worker : Initializer function
func Worker(cfg *config.AppConfig, inP pipe.Pipe, outPipes map[string]pipe.Pipe) bool {
	s := &Streamer{inPipe: inP}
	return s.start(cfg, outPipes)
}
