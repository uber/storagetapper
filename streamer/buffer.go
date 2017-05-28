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
	"encoding/json"
	"fmt"
	"golang.org/x/net/context" //"context"
	"sync"
	"time"

	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
)

func (s *Streamer) getTag() map[string]string {
	return map[string]string{
		"table":   s.table,
		"db":      s.db,
		"cluster": s.cluster,
	}
}

func (s *Streamer) encodeCommonFormat(data []byte) (key string, outMsg []byte, err error) {
	buf := bytes.NewBuffer(data)

	cfEvent := &types.CommonFormatEvent{}
	dec := json.NewDecoder(buf)
	err = dec.Decode(&cfEvent)
	if log.EL(s.log, err) {
		log.Errorf("broken event: %v %v", data, len(data))
		return
	}

	s.metrics.TimeInBuffer.Record(time.Since(time.Unix(0, cfEvent.Timestamp)))

	//	log.Debugf("commont format received %v %v", cfEvent, cfEvent.Fields)

	if cfEvent.Type == "insert" || cfEvent.Type == "delete" || cfEvent.Type == "schema" {
		outMsg, err = s.encoder.CommonFormat(cfEvent)
		if log.EL(s.log, err) {
			return
		}

		key = encoder.GetCommonFormatKey(cfEvent)
	} else if cfEvent.Type == s.outputFormat {
		_, err = buf.ReadFrom(dec.Buffered())
		if log.EL(s.log, err) {
			return
		}
		outMsg = buf.Bytes()
		key = cfEvent.Key[0].(string)
		//		log.Debugf("Data in final format already. Forwarding. Key=%v, SeqNo=%v", key, cfEvent.SeqNo)
	} else if cfEvent.Type == "json" {
		_, err = buf.ReadFrom(dec.Buffered())
		if log.EL(s.log, err) {
			return
		}
		var ev *types.CommonFormatEvent
		ev, err = encoder.CommonFormatDecode(buf.Bytes())
		if log.EL(s.log, err) {
			return
		}

		outMsg, err = s.encoder.CommonFormat(ev)
		if log.EL(s.log, err) {
			return
		}

		key = encoder.GetCommonFormatKey(ev)
	} else {
		err = fmt.Errorf("Unsupported conversion from: %v to %v", cfEvent.Type, s.outputFormat)
	}

	return
}

func (s *Streamer) produceEvent(data interface{}) error {
	var err error
	var outMsg []byte
	var key string

	//FIXME: We currently support only raw messages from local pipe or
	//CommonFormat messages
	switch m := data.(type) {
	case *types.RowMessage:
		//log.Debugf("Received raw message %v %v %v %v", m.Type, m.SeqNo, m.Data, m.Key)
		key = m.Key
		outMsg, err = s.encoder.Row(m.Type, m.Data, m.SeqNo)
	case []byte:
		s.BytesRead += int64(len(m))
		key, outMsg, err = s.encodeCommonFormat(m)
	}

	if err != nil {
		return err
	}

	/* Schema events skipped */
	if outMsg == nil {
		return nil
	}

	err = s.outProducer.PushBatch(key, outMsg)

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

func (s *Streamer) processBatch(c pipe.Consumer, next *result, msgCh chan *result) bool {
	var b int64
L:
	for next.hasNext {
		if log.EL(s.log, next.err) {
			return false
		}
		if err := s.produceEvent(next.data); err != nil {
			return false
		}
		b++
		if b >= int64(s.batchSize) {
			break
		}
		//Break if we would block
		select {
		case next = <-msgCh:
		default:
			break L
		}
	}

	s.metrics.EventsRead.Inc(b)
	s.metrics.EventsWritten.Inc(b)
	s.metrics.BatchSize.Record(time.Duration(b * 1000000))
	s.metrics.BytesWritten.Set(s.BytesWritten)
	s.metrics.BytesRead.Set(s.BytesRead)

	w := s.metrics.ProduceLatency
	w.Start()
	err := s.outProducer.PushBatchCommit()
	w.Stop()
	return !log.EL(s.log, err)
}

//eventFetcher is blocking call to buffered channel converter
func (s *Streamer) eventFetcher(c pipe.Consumer, wg *sync.WaitGroup, msgCh chan *result, exitCh chan bool) {
	defer wg.Done()
L:
	for {
		//		w := s.metrics.ReadLatency.Start()
		msg := &result{}
		//FetchNextBatch doesn't persist offsets, PopAck does
		if msg.hasNext = c.FetchNext(); msg.hasNext {
			msg.data, msg.err = c.Pop()
		}
		//		w.Stop()

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

// StreamTable attempts to acquire a lock on a table partition and streams events from
// that table partition while periodically updating its state of the last kafka offset consumed
func (s *Streamer) StreamTable(consumer pipe.Consumer, cancel context.CancelFunc, bootstrapCh chan bool) bool {
	var saveOffsets = true

	msgCh, exitCh := make(chan *result, s.batchSize), make(chan bool)

	var wg sync.WaitGroup
	wg.Add(1)

	defer func() {
		cancel()
		log.EL(s.log, s.inPipe.CloseConsumer(consumer, saveOffsets))
		close(exitCh)
		wg.Wait()
	}()

	/*This goroutine is to multiplex blocking FetchNext and tickChan*/
	go s.eventFetcher(consumer, &wg, msgCh, exitCh)

	tickCh := time.NewTicker(time.Second * time.Duration(s.stateUpdateTimeout)).C

	for !shutdown.Initiated() {
		select {
		case <-tickCh:
			s.metrics.NumWorkers.Emit()

			if !s.lock.Refresh() {
				return true
			}
			reg, _ := state.TableRegistered(s.id)
			if !reg {
				s.log.Warnf("Table removed from ingestion")
				return true
			}
		case err := <-bootstrapCh:
			if err {
				return false
			}
		case next := <-msgCh:
			if !next.hasNext {
				return true
			}
			if !s.processBatch(consumer, next, msgCh) {
				saveOffsets = false
				return false
			}
		case <-shutdown.InitiatedCh():
		}
	}

	return true
}
