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
	"time"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/snapshot"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/throttle"
)

const numRetries = 5

var cancelCheckInterval = 180 * time.Second

//The function may skip producing last fetched message, because of the partition
//key change, so we need to return it and use as a first message on subsequent
//call to streamBatch
func (s *Streamer) streamBatch(snReader snapshot.Reader, outProducer pipe.Producer, key string, pKey string, outMsg []byte, snapshotMetrics *metrics.Snapshot) (bool, int64, int64, string, string, []byte, error) {
	var i, b int
	var prevKey string
	var err error
	defer snapshotMetrics.ReadLatency.Start().Stop()
	cfg := s.outPipe.Config()
	for outMsg != nil || snReader.FetchNext() { //use last message from prev batch or fetch next one
		if outMsg == nil {
			key, pKey, outMsg, err = snReader.Pop()
			if log.EL(s.log, err) {
				return false, 0, 0, "", "", nil, err
			}
		}

		//Commit when batch full and partition key different from previous is fetched
		//This means that batch can be a little bigger than MaxBatchSize
		if (i >= cfg.MaxBatchSize || b >= cfg.MaxBatchSizeBytes) && pKey != prevKey {
			break
		}

		if len(outMsg) == 0 {
			outMsg = nil
			continue
		}
		b += len(outMsg)

		key = outProducer.PartitionKey("snapshot", key)
		err = outProducer.PushBatch(key, outMsg)

		if log.EL(s.log, err) {
			return false, 0, 0, "", "", nil, err
		}

		i++
		outMsg = nil
		prevKey = pKey
	}

	snapshotMetrics.BatchSize.Record(time.Duration(i) * time.Millisecond)

	if i == 0 {
		return false, 0, 0, "", "", nil, nil
	}

	snapshotMetrics.BytesWritten.Inc(int64(b))
	snapshotMetrics.EventsWritten.Inc(int64(i))

	return true, int64(b), int64(i), key, pKey, outMsg, nil
}

func (s *Streamer) commitWithRetry(outProducer pipe.Producer, snapshotMetrics *metrics.Snapshot) bool {
	var err error
	defer snapshotMetrics.ProduceLatency.Start().Stop()
	for i := 0; i < numRetries; i++ {
		if err = outProducer.PushBatchCommit(); err == nil {
			return true
		}
		log.Warnf("Retrying...Attempt %v", i+1)
	}
	log.EL(s.log, err)
	return false
}

func (s *Streamer) pushSchema(outProducer pipe.Producer) bool {
	outMsg, err := s.outEncoder.EncodeSchema(0)
	if log.EL(s.log, err) {
		return false
	}
	if outMsg == nil {
		return true
	}
	key := outProducer.PartitionKey("snapshot", "schema")
	err = outProducer.PushSchema(key, outMsg)
	return !log.EL(s.log, err)
}

func (s *Streamer) snapshotTickHandler(snReader snapshot.Reader) bool {
	reg, _ := state.TableRegisteredInState(s.row.ID)
	if !reg {
		s.log.Warnf("Table removed from ingestion. Snapshot cancelled.")
		return false
	}

	if !state.RefreshTableLock(s.row.ID, s.workerID) {
		s.log.Warnf("Lost the table lock. Snapshot cancelled.")
		return false
	}
	if !s.clusterLock.Refresh() {
		s.log.Warnf("Lost the cluster lock. Snapshot cancelled.")
		return false
	}
	return true
}

func (s *Streamer) streamLoop(snReader snapshot.Reader, outProducer pipe.Producer, iopsThrottler, mbThrottler *throttle.Throttle, ticker *time.Ticker, snapshotMetrics *metrics.Snapshot) bool {
	var err error
	var next bool
	var nBytes, nEvents int64
	var key, pKey string
	var outMsg []byte

	for !shutdown.Initiated() {
		next, nBytes, nEvents, key, pKey, outMsg, err = s.streamBatch(snReader, outProducer, key, pKey, outMsg, snapshotMetrics)
		if err != nil {
			return false
		}
		if !next {
			break
		}

		if !s.commitWithRetry(outProducer, snapshotMetrics) {
			return false
		}

		c := iopsThrottler.Advice(nEvents)
		m := mbThrottler.Advice(nBytes)
		if m > c {
			c = m
		}

		if c != 0 {
			time.Sleep(time.Microsecond * time.Duration(c))
			snapshotMetrics.ThrottledUs.Inc(c)
		}

		select {
		case <-ticker.C:
			if !s.snapshotTickHandler(snReader) {
				return false
			}
		default:
		}
	}

	return true
}

// StreamFromConsistentSnapshot initializes and pulls event from the Snapshot reader, serializes
// them in Avro format and publishes to output.
func (s *Streamer) streamFromConsistentSnapshot(throttleMB int64, throttleIOPS int64) bool {
	success := false
	snapshotMetrics := metrics.NewSnapshotMetrics("", s.getTag())

	snapshotMetrics.NumWorkers.Inc()
	startTime := time.Now()

	outProducer, err := s.outPipe.NewProducer(s.topic)
	if log.E(err) {
		return false
	}
	outProducer.SetFormat(s.row.OutputFormat)

	defer func() {
		if !success {
			snapshotMetrics.Errors.Inc(1)
			log.EL(s.log, outProducer.CloseOnFailure())
		}
		snapshotMetrics.NumWorkers.Dec()
	}()

	s.log.Infof("Starting consistent snapshot streamer for: %v, %v", s.topic, s.outEncoder.Type())

	//For JSON format push schema as a first message of the stream
	if !s.pushSchema(outProducer) {
		return false
	}

	snReader, err := snapshot.Start(s.row.Input, s.row.Service, s.row.Cluster, s.row.Db, s.row.Table, s.row.Params, s.outEncoder, snapshotMetrics)
	if log.EL(s.log, err) {
		return false
	}
	defer snReader.End()

	iopsThrottler := throttle.New(throttleIOPS, 1000000, 3)
	defer iopsThrottler.Close()
	mbThrottler := throttle.New(throttleMB*1024*1024, 1000000, 3)
	defer mbThrottler.Close()
	if throttleIOPS != 0 || throttleMB != 0 {
		s.log.Debugf("Snapshot throttle enabled: %v IOPS, %v MBs", throttleIOPS, throttleMB)
	}

	ticker := time.NewTicker(cancelCheckInterval)
	defer ticker.Stop()

	if !s.streamLoop(snReader, outProducer, iopsThrottler, mbThrottler, ticker, snapshotMetrics) {
		return false
	}

	if shutdown.Initiated() {
		return false
	}

	if err = outProducer.Close(); err == nil {
		err = state.ClearNeedSnapshot(s.row.ID, s.row.SnapshottedAt)
		snapshotMetrics.Duration.Record(time.Since(startTime))
		snapshotMetrics.SizeRead.Set(snapshotMetrics.BytesRead.Get())
		snapshotMetrics.SizeWritten.Set(snapshotMetrics.BytesWritten.Get())
		if err == nil {
			success = true
		}
	}

	return !log.EL(s.log, err)
}
