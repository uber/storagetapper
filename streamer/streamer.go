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
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/uber/storagetapper/changelog"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/lock"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/util"
)

const waitForGtidTimeout = 3600 * time.Second

//Streamer struct defines common properties of Event streamer worker
type Streamer struct {
	row *state.Row

	topic      string
	inPipe     pipe.Pipe
	outPipe    pipe.Pipe
	outEncoder encoder.Encoder
	envEncoder encoder.Encoder
	log        log.Logger

	metrics      *metrics.Streamer
	BytesWritten int64
	BytesRead    int64

	stateUpdateInterval time.Duration
	clusterLock         lock.Lock

	workerID string
}

func (s *Streamer) ensureChangelogReaderStartLow(prevGTID string, ts time.Time) (string, time.Time, error) {
	s.log.WithFields(log.Fields{"prev_gtid": prevGTID}).Debugf("Waiting for Changelog reader to start publishing event")
	ticker := time.NewTicker(time.Millisecond * 1000)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			tblInfo, err := state.GetTableByID(s.row.ID)
			if err != nil {
				return "", time.Time{}, err
			}
			if tblInfo == nil {
				return "", time.Time{}, fmt.Errorf("table not found. id=%v", s.row.ID)
			}
			if tblInfo.Gtid != prevGTID || (prevGTID != "" && ts != tblInfo.GtidUpdatedAt) {
				s.log.Debugf("Changelog reader confirmed started from %v, updated at %v", tblInfo.Gtid, tblInfo.GtidUpdatedAt)
				return tblInfo.Gtid, tblInfo.GtidUpdatedAt, nil
			}
		case <-shutdown.InitiatedCh():
			return "", time.Time{}, fmt.Errorf("shutdown in progress")
		}
	}
}

// ensureChangelogReaderStart ensures that Changelog reader worker has started
// publishing to Kafka buffer for this cluster in order to avoid a gap in the
// event stream
func (s *Streamer) ensureChangelogReaderStart() (string, time.Time, error) {
	//ensure that changelog started for this cluster
	gtid, ts, err := s.ensureChangelogReaderStartLow("", time.Time{})
	if err != nil {
		return "", time.Time{}, err
	}
	//ensure that changelog reader sees our possible change of snapshotted_at in
	//lockTable
	return s.ensureChangelogReaderStartLow(gtid, ts)
}

func (s *Streamer) waitForGtid(svc string, cluster string, sdb string, inputType string, gtid string) bool {
	var current mysql.GTIDSet

	s.log.Debugf("Waiting for snapshot server to catch up to: %v", gtid)

	target, err := mysql.ParseGTIDSet("mysql", gtid)
	if log.EL(s.log, err) {
		return false
	}

	conn, err := db.OpenService(&db.Loc{Service: svc, Cluster: cluster, Name: sdb}, "", inputType)
	if log.EL(s.log, err) {
		return false
	}
	defer func() { log.EL(s.log, conn.Close()) }()

	tickCheck := time.NewTicker(5 * time.Second)
	defer tickCheck.Stop()
	tickLock := time.NewTicker(s.stateUpdateInterval)
	defer tickLock.Stop()
	toCh := time.After(waitForGtidTimeout)
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
		case <-toCh:
			s.log.WithFields(log.Fields{"server has": current, "we need": target.String()}).Errorf("Timeout waiting snapshot server to catch up")
			return false
		case <-tickLock.C:
			if !state.RefreshTableLock(s.row.ID, s.workerID) || !s.clusterLock.Refresh() {
				s.log.Debugf("Lost the lock while waiting for gtid")
				return false
			}
			s.log.WithFields(log.Fields{"server has": current, "we need": target.String()}).Errorf("Still waiting snapshot server to catch up")
		case <-shutdown.InitiatedCh():
			return false
		case <-tickCheck.C:
		}
	}

	s.log.Debugf("Snapshot server at: %v", current)

	return true
}

func (s *Streamer) lockTable() bool {
	var err error

	//Table not found or snapshot is not needed and input doesn't need
	//changelog tailing
	if s.row == nil || ((!s.row.NeedSnapshot || s.row.Params.NoSnapshot) && changelog.Plugins[s.row.Input] == nil) {
		return false
	}

	//If cluster concurrency is limited try to get our ticket
	cc := s.row.Params.ClusterConcurrency
	//MySQL 5.7 lock names are limited to 64 character, so we hash possible long
	//cluster names here
	h := md5.New()
	_, err = io.WriteString(h, fmt.Sprintf("%v.%v", s.row.Service, s.row.Cluster))
	log.E(err)
	if cc != 0 && s.row.NeedSnapshot && !s.clusterLock.TryLockShared(fmt.Sprintf("%x", h.Sum(nil)), cc) {
		return false
	}

	s.outPipe, err = pipe.CacheGet(s.row.Output, &s.row.Params.Pipe, state.GetDB())
	return !log.E(err)
}

func (s *Streamer) setupChangelogConsumer(cfg *config.AppConfig) (pipe.Consumer, bool) {
	var err error
	var consumer pipe.Consumer

	if cfg.ChangelogBuffer {
		var tn string
		tn, err = config.Get().GetChangelogTopicName(s.row.Service, s.row.Db, s.row.Table, s.row.Input, s.row.Output, s.row.Version, s.row.SnapshottedAt)
		if log.EL(s.log, err) {
			return nil, false
		}

		s.log.Debugf("Setting up consumer for buffer topic: %v", tn)

		//Consumer MUST be created before snapshotting the table.
		//Creating it after may leave a gap in events stream.
		consumer, err = s.inPipe.NewConsumer(tn)
		if log.EL(s.log, err) {
			return nil, false
		}

	}

	var gtid string
	gtid, _, err = s.ensureChangelogReaderStart()
	if log.E(err) {
		if consumer != nil {
			log.E(consumer.CloseOnFailure())
		}
		return nil, false
	}

	if !s.waitForGtid(s.row.Service, s.row.Cluster, s.row.Db, s.row.Input, gtid) {
		if consumer != nil {
			log.E(consumer.CloseOnFailure())
		}
		return nil, false
	}

	return consumer, true
}

func (s *Streamer) start(cfg *config.AppConfig) bool {
	var err error

	s.clusterLock = lock.Create(state.GetDbAddr())
	defer s.clusterLock.Close()

	h, _ := os.Hostname()
	w := log.GenWorkerID()
	s.workerID = h + "." + w

	s.row, err = state.GetTableTask(s.workerID, cfg.LockExpireTimeout)
	if err != nil {
		if util.MySQLError(err, mysql.ER_LOCK_WAIT_TIMEOUT) {
			return true
		}
		log.E(err)
		return false
	}

	if !s.lockTable() {
		log.Debugf("Finished streamer: No free tables to work on")
		return false
	}

	s.log = log.WithFields(log.Fields{"worker_id": w, "service": s.row.Service, "db": s.row.Db, "table": s.row.Table, "version": s.row.Version})
	s.metrics = metrics.NewStreamerMetrics(s.getTag())

	s.log.Debugf("Started event streamer")

	s.metrics.NumWorkers.Inc()
	defer func() {
		if err != nil {
			s.metrics.Errors.Inc(1)
		}
		s.metrics.NumWorkers.Dec()
	}()

	// Event Streamer worker has successfully acquired a lock on a table. Proceed further
	// Each Event Streamer handles events from all partitions from Input buffer for a table
	s.topic, err = cfg.GetOutputTopicName(s.row.Service, s.row.Db, s.row.Table, s.row.Input, s.row.Output, s.row.Version, s.row.SnapshottedAt)
	if log.E(err) {
		return false
	}
	s.stateUpdateInterval = cfg.StateUpdateInterval

	s.log.Debugf("Will be streaming to output topic: %v", s.topic)

	consumer, res := s.setupChangelogConsumer(cfg)
	if !res {
		return false
	}

	s.outEncoder, err = encoder.Create(s.row.OutputFormat, s.row.Service, s.row.Db, s.row.Table, s.row.Input, s.row.Output, s.row.Version)
	if log.EL(s.log, err) {
		if consumer != nil {
			log.E(consumer.CloseOnFailure())
		}
		return false
	}

	if s.row.NeedSnapshot && !s.row.Params.NoSnapshot && !s.streamFromConsistentSnapshot(cfg.ThrottleTargetMB, cfg.ThrottleTargetIOPS) {
		if consumer != nil {
			log.E(consumer.CloseOnFailure())
		}
		return false
	}

	s.clusterLock.Close() // ClusterConcurrency is limited for the duration of snapshot only

	if cfg.ChangelogBuffer {
		//Transit format encoder, aka envelope encoder
		//It must be per table to be able to decode schematized events
		s.envEncoder, err = encoder.Create(encoder.Internal.Type(), s.row.Service, s.row.Db, s.row.Table, s.row.Input, s.row.Output, s.row.Version)
		if log.EL(s.log, err) {
			log.E(consumer.CloseOnFailure())
			return false
		}

		if !s.streamTable(consumer) {
			s.metrics.Errors.Inc(1)
		}
	}

	s.log.Debugf("Finished streamer")

	return true
}

// Worker : Initializer function
func Worker(cfg *config.AppConfig, inP pipe.Pipe) bool {
	s := &Streamer{inPipe: inP}
	return s.start(cfg)
}
