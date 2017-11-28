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

var numRetries = 5

func (s *Streamer) streamBatch(snReader snapshot.Reader, outProducer pipe.Producer, batchSize int, snapshotMetrics *metrics.Snapshot) (bool, int64, int64, error) {
	var i, b int
	for i < batchSize && snReader.HasNext() {
		key, outMsg, err := snReader.GetNext()
		if log.EL(s.log, err) {
			return false, 0, 0, err
		}

		b += len(outMsg)

		if s.outPipe.Type() == "file" {
			key = "snapshot"
		}
		err = outProducer.PushBatch(key, outMsg)

		if log.EL(s.log, err) {
			return false, 0, 0, err
		}

		i++
	}

	snapshotMetrics.BatchSize.Record(time.Duration(i * 1000000))

	if i == 0 {
		return false, 0, 0, nil
	}

	snapshotMetrics.BytesRead.Inc(int64(b))
	snapshotMetrics.BytesWritten.Inc(int64(b))
	snapshotMetrics.EventsRead.Inc(int64(i))
	snapshotMetrics.EventsWritten.Inc(int64(i))

	return true, int64(b), int64(i), nil
}

func (s *Streamer) commitWithRetry(snapshotMetrics *metrics.Snapshot) bool {
	var err error
	for i := 0; i < numRetries; i++ {
		w := snapshotMetrics.ProduceLatency
		w.Start()
		err = s.outProducer.PushBatchCommit()
		w.Stop()
		if err == nil {
			return true
		}
		log.Warnf("Retrying...Attempt %v", i+1)
	}
	log.EL(s.log, err)
	return false
}

func (s *Streamer) pushSchema() bool {
	outMsg, err := s.outEncoder.EncodeSchema(0)
	if log.EL(s.log, err) {
		return false
	}
	if outMsg == nil {
		return true
	}
	err = s.outProducer.PushSchema("", outMsg)
	return !log.EL(s.log, err)
}

// StreamFromConsistentSnapshot initializes and pulls event from the Snapshot reader, serializes
// them in Avro format and publishes to output Kafka topic.
func (s *Streamer) streamFromConsistentSnapshot(input string, throttleMB int64, throttleIOPS int64) bool {
	snReader, err := snapshot.InitReader(input)
	if log.EL(s.log, err) {
		return false
	}
	snapshotMetrics := metrics.GetSnapshotMetrics(s.getTag())

	outProducer := s.outProducer

	s.log.Infof("Starting consistent snapshot streamer for: %v, %v", s.topic, s.outEncoder.Type())

	//For JSON format push schema as a first message of the stream
	if !s.pushSchema() {
		return false
	}

	_, err = snReader.Start(s.cluster, s.svc, s.db, s.table, s.outEncoder)
	if log.EL(s.log, err) {
		return false
	}
	defer snReader.End()

	snapshotMetrics.NumWorkers.Inc()
	defer snapshotMetrics.NumWorkers.Dec()

	iopsThrottler := throttle.New(throttleIOPS, 1000000, 3)
	mbThrottler := throttle.New(throttleMB*1024*1024, 1000000, 3)

	if throttleIOPS != 0 || throttleMB != 0 {
		s.log.Debugf("Snapshot throttle enabled: %v IOPS, %v MBs", throttleIOPS, throttleMB)
	}

	var t int64
	for !shutdown.Initiated() {
		next, nBytes, nEvents, err := s.streamBatch(snReader, outProducer, s.batchSize, snapshotMetrics)

		if log.EL(s.log, err) {
			return false
		}
		if !next {
			break
		}

		if !s.commitWithRetry(snapshotMetrics) {
			return false
		}

		c := iopsThrottler.Advice(nEvents)
		m := mbThrottler.Advice(nBytes)
		if m > c {
			c = m
		}

		if c != 0 {
			t += c
			time.Sleep(time.Microsecond * time.Duration(c))
		}
	}

	log.Debugf("Total throttled %v us", t)

	if shutdown.Initiated() {
		return false
	}

	err = state.SetTableNewFlag(s.svc, s.db, s.table, false)
	return !log.EL(s.log, err)
}
