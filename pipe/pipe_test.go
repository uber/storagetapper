// Copyright (c) 2018 Uber Technologies, Inc.
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

package pipe

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/util"
)

var startCh chan bool
var numProcs = 5
var numRecs = 31
var numPartitions = 8
var wg sync.WaitGroup

func msgGenDef(topic string, i int) string {
	return strings.Replace(topic, "/", ".", -1) + "key" + "." + fmt.Sprintf("%03d", i)
}

func schemaGenDef(topic string) string {
	return ""
}

var msgGenFn = msgGenDef
var schemaGenFn = schemaGenDef

func consumeMessage(c Consumer, t *testing.T) string {
	if !c.FetchNext() {
		return ""
	}
	in, err := c.Pop()
	b := in.([]byte)
	test.CheckFail(err, t)
	return util.BytesToString(b)
}

func testConsumerWorker(p Pipe, topic string, i int, n int, graceful bool, t *testing.T) {
	log.Debugf("Consumer from %v start=%v count=%v", topic, i, n)
	c, err := p.NewConsumer(topic)
	if err != nil {
		log.Errorf("NewConsumer failed: %v", err.Error())
		t.Fatalf("NewConsumer failed: %v", err.Error())
	}
	c.SetFormat("text")
	defer func() {
		if graceful {
			log.E(c.Close())
		} else {
			log.E(c.CloseOnFailure())
		}
	}()

	startCh <- true
	log.Debugf("Pushed to start channel nrecs=%v startfrom=%v", n, i)

	res := make(map[string]bool)

	for j := 0; j < n; j++ {
		s := consumeMessage(c, t)
		if s == "" {
			break
		}
		if res[s] {
			log.Fatalf("Duplicate key received: %v", s)
		}
		res[s] = true
		log.Debugf("%v. %v consumed, %v to go", s, j+1, n-j-1)
	}

	if len(res) != n {
		t.Fatalf("Not all message received, got %v, want %v", len(res), n)
	}

	for j := 0; j < n; j++ {
		s := msgGenFn(topic, i+j)
		if !res[s] {
			log.Fatalf("Missing: %v", s)
		}
	}

	log.Debugf("Consumer finished %v", topic)
}

func testProducerWorker(p Pipe, topic string, startFrom int, ptype int, nrecs int, t *testing.T) {
	c, err := p.NewProducer(topic)
	test.CheckFail(err, t)

	c.SetFormat("text")
	s := schemaGenFn(topic)
	if s != "" {
		err = c.PushSchema("", []byte(s))
		test.CheckFail(err, t)
	}

	log.Debugf("testProducerWorker started: %v", topic)

	for i := 0; i < nrecs; i++ {
		log.Debugf("testProducerWorker %v %v", i, topic)
		msg := msgGenFn(topic, i+startFrom)
		//msg := topic + "key" + "." + strconv.Itoa(i+startFrom)
		b := []byte(msg)
		if ptype == KEY {
			err = c.PushK(msg, b)
		} else if ptype == NOKEY {
			err = c.Push(b)
		} else if ptype == BATCH {
			err = c.PushBatch(msg, b)
		} else {
			err = c.(*kafkaProducer).pushPartition(msg, ptype, b)
		}
		log.Debugf("Pushed: %v", msg)
		test.CheckFail(err, t)
	}

	if ptype == BATCH {
		err = c.PushBatchCommit()
		test.CheckFail(err, t)
	}

	err = c.Close()
	test.CheckFail(err, t)

	log.Debugf("Producer finished %v", topic)
}

func startConsumers(p Pipe, nprocs int, startFrom int, n int, t *testing.T) {
	startConsumersLow(p, nprocs, startFrom, n, "topic%03d", t)
}

func startConsumersLow(p Pipe, nprocs int, startFrom int, n int, topicFmt string, t *testing.T) {
	log.Debugf("Starting %v consumers", nprocs)
	wg.Add(nprocs)
	for i := 0; i < nprocs; i++ {
		go func(i int) { defer wg.Done(); testConsumerWorker(p, fmt.Sprintf(topicFmt, i), startFrom, n, true, t) }(i)
	}
}

func startProducers(p Pipe, startFrom int, t *testing.T, pt int) {
	startProducersLow(p, startFrom, t, pt, "topic%03d")
}

func startProducersLow(p Pipe, startFrom int, t *testing.T, pt int, topicFmt string) {
	log.Debugf("Starting %v producers", numProcs)
	wg.Add(numProcs)
	for i := 0; i < numProcs; i++ {
		go func(i int) {
			defer wg.Done()
			testProducerWorker(p, fmt.Sprintf(topicFmt, i), startFrom, pt, numRecs, t)
		}(i)
	}
}

func testLoop(p Pipe, t *testing.T, pt int) {
	testLoopLow(p, t, pt, "topic%03d")
}

func testLoopLow(p Pipe, t *testing.T, pt int, topicFmt string) {
	log.Debugf("STARTED %v type=%v, file_size=%v", p.Type(), pt, p.Config().MaxFileSize)
	startConsumersLow(p, numProcs, 0, numRecs, topicFmt, t)

	for i := 0; i < numProcs; i++ {
		<-startCh
		log.Debugf("started %v consumers", i+1)
	}
	log.Debugf("all consumers have started")

	startProducersLow(p, 0, t, pt, topicFmt)

	wg.Wait()
	log.Debugf("FINISHED %v %v", p.Type(), pt)
}

func testLoopReversed(p Pipe, t *testing.T, pt int) {
	startProducers(p, 1111, t, pt)

	wg.Wait()

	startConsumers(p, numProcs, 1111, numRecs, t)

	for i := 0; i < numProcs; i++ {
		log.Debugf("waiting for %v", i)
		<-startCh
	}
	log.Debugf("all consumers have started")

	wg.Wait()
	log.Debugf("finished")
}
