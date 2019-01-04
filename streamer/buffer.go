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
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
)

var batchCommitInterval = 1 * time.Second

func (s *Streamer) getTag() map[string]string {
	return map[string]string{
		"table":   s.row.Table,
		"db":      s.row.Db,
		"cluster": s.row.Cluster,
		"input":   s.row.Input,
		"version": strconv.Itoa(s.row.Version),
	}
}

func (s *Streamer) encodeCommonFormat(outProducer pipe.Producer, data []byte) (key string, outMsg []byte, err error) {
	cfEvent := &types.CommonFormatEvent{}
	payload, err := s.envEncoder.UnwrapEvent(data, cfEvent)
	if log.EL(s.log, err) {
		log.Errorf("broken event: %v %v", data, len(data))
		return
	}

	s.metrics.TimeInBuffer.Record(time.Since(time.Unix(0, cfEvent.Timestamp)))

	//  log.Debugf("commont format received %v %v", cfEvent, cfEvent.Fields)

	if cfEvent.Type == s.row.OutputFormat {
		outMsg = payload
		key = cfEvent.Key[0].(string)
		if key == "" {
			err = s.outEncoder.UpdateCodec()
			if log.EL(s.log, err) {
				return
			}
		}
		//    log.Debugf("Data in final format already. Forwarding. Key=%v, SeqNo=%v", key, cfEvent.SeqNo)
	} else if cfEvent.Type == "insert" || cfEvent.Type == "delete" || cfEvent.Type == "schema" {
		outMsg, err = s.outEncoder.CommonFormat(cfEvent)
		if log.EL(s.log, err) {
			return
		}

		key = encoder.GetCommonFormatKey(cfEvent)

		if cfEvent.Type == "schema" && outMsg != nil {
			key = outProducer.PartitionKey("log", key)

			err = outProducer.PushSchema(key, outMsg)
			log.EL(s.log, err)

			outMsg = nil
			return
		}
	} else if cfEvent.Type == s.envEncoder.Type() {
		var ev *types.CommonFormatEvent
		ev, err = s.envEncoder.DecodeEvent(payload)
		if log.EL(s.log, err) {
			return
		}
		outMsg, err = s.outEncoder.CommonFormat(ev)
		if log.EL(s.log, err) {
			return
		}

		key = encoder.GetCommonFormatKey(ev)
	} else {
		err = fmt.Errorf("unsupported conversion from: %v to %v", cfEvent.Type, s.row.OutputFormat)
	}

	return
}

func (s *Streamer) produceEvent(outProducer pipe.Producer, data interface{}) error {
	var err error
	var outMsg []byte
	var key string

	// FIXME: We currently support only raw messages from local pipe or CommonFormat messages
	switch m := data.(type) {
	case *types.RowMessage:
		key = m.Key
		outMsg, err = s.outEncoder.Row(m.Type, m.Data, m.SeqNo, m.Timestamp)
	case []byte:
		if len(m) == 0 { // Kafka may return empty messages, skip them
			return nil
		}
		s.BytesRead += int64(len(m))
		key, outMsg, err = s.encodeCommonFormat(outProducer, m)
	}

	if err != nil {
		return err
	}

	/* Schema events skipped */
	if outMsg == nil {
		return nil
	}

	key = outProducer.PartitionKey("log", key)
	err = outProducer.PushBatch(key, outMsg)
	log.EL(s.log, err)

	if err == nil {
		s.BytesWritten += int64(len(outMsg))
	}

	return err
}

//message passed from fetcher to the main loop
type result struct {
	data    interface{}
	err     error
	hasNext bool
}

func (s *Streamer) commitBatch(outProducer pipe.Producer, numEvents int64) bool {
	s.metrics.EventsRead.Inc(numEvents)
	s.metrics.EventsWritten.Inc(numEvents)
	s.metrics.BatchSize.Record(time.Duration(numEvents) * time.Millisecond)
	s.metrics.BytesWritten.Set(s.BytesWritten)
	s.metrics.BytesRead.Set(s.BytesRead)

	s.metrics.ProduceLatency.Start()
	err := outProducer.PushBatchCommit()
	s.metrics.ProduceLatency.Stop()

	if err != nil {
		s.log.Errorf(errors.Wrap(err, "Failed to commit batch").Error())
		return false
	}

	return true
}

//eventFetcher is blocking call to buffered channel converter
func (s *Streamer) eventFetcher(c pipe.Consumer, wg *sync.WaitGroup, msgCh chan *result, exitCh chan bool) {
	defer wg.Done()
L:
	for {
		msg := &result{}
		if msg.hasNext = c.FetchNext(); msg.hasNext {
			msg.data, msg.err = c.Pop()
		}

		select {
		case msgCh <- msg:
			if !msg.hasNext || msg.err != nil {
				break L
			}
		case <-exitCh:
			break L
		}
	}
	log.Debugf("Finished streamer eventFetcher goroutine")
}

//returns false if worker should exit
func (s *Streamer) bufferTickHandler(consumer pipe.Consumer) bool {
	s.metrics.NumWorkers.Emit()

	if !state.RefreshTableLock(s.row.ID, s.workerID) {
		s.metrics.LockLost.Inc(1)
		s.log.Debugf("Lost table lock")
		return false
	}

	tblInfo, err := state.GetTableByID(s.row.ID)
	if log.EL(s.log, err) {
		return false
	}

	if tblInfo == nil {
		s.log.Debugf("Table removed from ingestion")
		return false
	}

	if (tblInfo.NeedSnapshot || tblInfo.SnapshotTimeChanged(s.row.SnapshottedAt) || tblInfo.TimeForSnapshot(time.Now())) && !s.row.Params.NoSnapshot {
		s.log.Debugf("Table needs snapshot")
		return false
	}

	//Guarantee that we can loose no more than state_update_interval seconds
	if err := consumer.SaveOffset(); err != nil {
		s.log.Errorf("Error persisting pipe position")
		return false
	}

	return true
}

func closeConsumer(consumer pipe.Consumer, saveOffsets bool, l log.Logger) {
	if saveOffsets {
		log.EL(l, consumer.Close())
	} else {
		log.EL(l, consumer.CloseOnFailure())
	}
}

// streamTable attempts to acquire a lock on a table partition and streams events from
// that table partition while periodically updating its state of the last kafka offset consumed
func (s *Streamer) streamTable(consumer pipe.Consumer) bool {
	var saveOffsets = true
	var wg sync.WaitGroup

	msgCh, exitCh := make(chan *result, s.outPipe.Config().MaxBatchSize), make(chan bool)

	outProducer, err := s.outPipe.NewProducer(s.topic)
	if log.E(err) {
		return false
	}
	outProducer.SetFormat(s.row.OutputFormat)

	defer func() {
		closeConsumer(consumer, saveOffsets, s.log)
		log.EL(s.log, outProducer.Close())
		close(exitCh)
		wg.Wait()
	}()

	// This goroutine is to multiplex blocking FetchNext and tickChan
	wg.Add(1)
	go s.eventFetcher(consumer, &wg, msgCh, exitCh)

	stateUpdateTick := time.NewTicker(s.stateUpdateInterval)
	defer stateUpdateTick.Stop()

	batchCommitTick := time.NewTicker(batchCommitInterval)
	defer batchCommitTick.Stop()

	s.log.Debugf("Beginning to stream from buffer")

	var numEvents int64
	var prevBytesWritten = s.BytesWritten
	for !shutdown.Initiated() {
		select {
		case <-stateUpdateTick.C:
			if !s.bufferTickHandler(consumer) {
				return true
			}
		case next := <-msgCh:
			if !next.hasNext {
				s.log.Debugf("End of message stream")
				return true
			}

			if next.err != nil {
				s.log.Errorf(errors.Wrap(next.err, "Failed to fetch next message").Error())
				return false
			}

			if err := s.produceEvent(outProducer, next.data); err != nil {
				s.log.Errorf(errors.Wrap(err, "Failed to produce message").Error())
				saveOffsets = false
				return false
			}
			numEvents++
			numBytes := s.BytesWritten - prevBytesWritten

			// Commit the batch if we have reached batch size
			if numEvents >= int64(s.outPipe.Config().MaxBatchSize) ||
				numBytes >= int64(s.outPipe.Config().MaxBatchSizeBytes) {
				if !s.commitBatch(outProducer, numEvents) {
					saveOffsets = false
					return false
				}
				numEvents = 0
				prevBytesWritten = s.BytesWritten
			}
		case <-batchCommitTick.C:
			// Its time to commit the batch, let's emit metrics as well
			if !s.commitBatch(outProducer, numEvents) {
				saveOffsets = false
				return false
			}
			numEvents = 0
			prevBytesWritten = s.BytesWritten
		case <-shutdown.InitiatedCh():
		}
	}

	return true
}
