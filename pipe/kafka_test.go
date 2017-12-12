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

package pipe

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/util"
)

var startCh chan bool
var numProcs = 5
var numRecs = 31
var numPartitions = 8
var wg sync.WaitGroup

//Push types. 0... - used for push to specific partition
const (
	KEY   = -3
	NOKEY = -2
	BATCH = -1
)

func createPipe(batchSize int) *KafkaPipe {
	p := &KafkaPipe{ctx: shutdown.Context, kafkaAddrs: cfg.KafkaAddrs, conn: state.GetDB(), batchSize: batchSize}
	//Allow us to manually assign partitions to outgoing messages
	p.Config = sarama.NewConfig()
	p.Config.Producer.Partitioner = sarama.NewManualPartitioner
	p.Config.Producer.Return.Successes = true
	p.Config.Consumer.MaxWaitTime = 10 * time.Millisecond

	//Test the cases when offsetPersistInterval smaller then batch size
	offsetPersistInterval = 1
	InitialOffset = sarama.OffsetNewest

	return p
}

func consumeMessage(c Consumer, t *testing.T) string {
	if !c.FetchNext() {
		return ""
	}
	in, err := c.Pop()
	b := in.([]byte)
	test.CheckFail(err, t)
	return util.BytesToString(b)
	/*
		slen := bytes.IndexByte(b, 0) //find strlen
		if slen == -1 {
			slen = len(b)
		}
		return string(b[:slen])
	*/
}

func kafkaConsumerWorker(p Pipe, key string, i int, n int, graceful bool, t *testing.T) {
	log.Debugf("Consumer from %v start=%v count=%v", key, i, n)
	c, err := p.NewConsumer(key)
	if err != nil {
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
		s := key + "key" + "." + strconv.Itoa(i+j)
		if !res[s] {
			log.Fatalf("Missing: %v", s)
		}
	}

	log.Debugf("Consumer finished %v", key)
}

//pushPartition sends a keyed message to specific partition
func (p *kafkaProducer) pushPartition(key string, partition int, in interface{}) error {
	var bytes []byte
	switch in.(type) {
	case []byte:
		bytes = in.([]byte)
	default:
		return fmt.Errorf("Kafka pipe can handle binary arrays only")
	}
	msg := &sarama.ProducerMessage{Topic: p.topic, Partition: int32(partition), Key: sarama.StringEncoder(key), Value: sarama.ByteEncoder(bytes)}
	//_, _, err := p.producer.SendMessage(msg)
	log.Debugf("msg: %+v", msg)
	respart, offset, err := p.producer.SendMessage(msg)
	if !log.E(err) {
		log.Debugf("Message has been sent. Partition=%v(%v). Offset=%v key=%v\n", respart, int32(partition), offset, key)
	}

	return err
}

func kafkaProducerWorker(p Pipe, key string, startFrom int, ptype int, nrecs int, t *testing.T) {
	c, err := p.NewProducer(key)
	test.CheckFail(err, t)

	c.SetFormat("text")

	log.Debugf("kafkaProducerWorker started: %v", key)

	for i := 0; i < nrecs; i++ {
		log.Debugf("kafkaProducerWorker %v %v", i, key)
		msg := key + "key" + "." + strconv.Itoa(i+startFrom)
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

	log.Debugf("Producer finished %v", key)
}

func startConsumers(p Pipe, nprocs int, startFrom int, n int, t *testing.T) {
	log.Debugf("Starting %v consumers", nprocs)
	wg.Add(nprocs)
	for i := 0; i < nprocs; i++ {
		go func(i int) { defer wg.Done(); kafkaConsumerWorker(p, "topic"+strconv.Itoa(i), startFrom, n, true, t) }(i)
	}
}

func startProducers(p Pipe, startFrom int, t *testing.T, pt int) {
	log.Debugf("Starting %v producers", numProcs)
	wg.Add(numProcs)
	for i := 0; i < int(numProcs); i++ {
		go func(i int) {
			defer wg.Done()
			kafkaProducerWorker(p, "topic"+strconv.Itoa(i), startFrom, pt, numRecs, t)
		}(i)
	}
}

func testLoop(p Pipe, t *testing.T, pt int) {
	startConsumers(p, numProcs, 0, numRecs, t)

	for i := 0; i < numProcs; i++ {
		<-startCh
		log.Debugf("started %v consumers", i+1)
	}
	log.Debugf("all consumers have started")

	startProducers(p, 0, t, pt)

	wg.Wait()
	log.Debugf("finished")
}

func testLoopReversed(p Pipe, t *testing.T, pt int) {
	log.Debugf("Check saved kafka offsets")

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

func TestKafkaBasic(t *testing.T) {
	test.SkipIfNoKafkaAvailable(t)
	test.SkipIfNoMySQLAvailable(t)

	log.Debugf("Testing basic Kafka pipe functionality")

	startCh = make(chan bool)

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	if !state.Init(cfg) {
		t.Fatalf("Failed to init State")
	}

	//Don't check returned error because table might not exist
	_ = util.ExecSQL(state.GetDB(), "DROP TABLE IF EXISTS kafka_offsets")

	p := createPipe(0)

	log.Debugf("Test keyed push")
	testLoop(p, t, KEY)
	log.Debugf("Check saved offsets by starting producers first")
	testLoopReversed(p, t, KEY)

	err := util.ExecSQL(state.GetDB(), "DROP TABLE IF EXISTS kafka_offsets")
	test.CheckFail(err, t)

	log.Debugf("Test non-keyed push")
	testLoop(p, t, NOKEY)
	log.Debugf("Test offsets after restart")
	testLoopReversed(p, t, NOKEY)
}

func TestKafkaBigMessage(t *testing.T) {
	test.SkipIfNoKafkaAvailable(t)
	test.SkipIfNoMySQLAvailable(t)

	log.Debugf("Testing Big Kafka pipe message")

	startCh = make(chan bool)

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	if !state.Init(cfg) {
		t.Fatalf("Failed to init State")
	}

	//Don't check returned error because table might not exist
	_ = util.ExecSQL(state.GetDB(), "DROP TABLE IF EXISTS kafka_offsets")

	p := createPipe(1)

	consumer, err := p.NewConsumer("topic0")
	test.CheckFail(err, t)
	producer, err := p.NewProducer("topic0")
	test.CheckFail(err, t)

	buf := make([]byte, 16384)
	for i := 0; i < 16384; i++ {
		buf[i] = byte(i)
	}

	for i := 0; i < 512; i++ {
		err = producer.PushBatch(strconv.Itoa(i), buf)
		test.CheckFail(err, t)
	}
	err = producer.PushBatchCommit()
	test.CheckFail(err, t)

	for i := 0; i < 512; i++ {
		if !consumer.FetchNext() {
			t.Fatalf("There should be message")
		}

		res, err := consumer.Pop()
		test.CheckFail(err, t)

		buf = res.([]byte)

		for i := 0; i < 16384; i++ {
			if buf[i] != byte(i) {
				t.Fatalf("Mismatch, pos %v", i)
			}
		}
	}

	err = consumer.Close()
	test.CheckFail(err, t)
	err = producer.Close()
	test.CheckFail(err, t)
}

func TestKafkaOffsets(t *testing.T) {
	test.SkipIfNoKafkaAvailable(t)
	test.SkipIfNoMySQLAvailable(t)

	startCh = make(chan bool, 1)

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	if !state.Init(cfg) {
		t.Fatalf("Failed to init State")
	}
	defer func() { log.E(state.Close()) }()

	//Don't check returned error because table might not exist
	_ = util.ExecSQL(state.GetDB(), "DROP TABLE IF EXISTS kafka_offsets")

	log.Debugf("Testing that pipe can preserve offsets on graceful shutdown")

	var bs = []int{1, 2, 3, 256}
	for _, i := range bs {
		log.Debugf("Testing batch size %v", i)
		p := createPipe(i)

		startConsumers(p, 1, 0, i, t) //this consumer and next producer is to persist current offset
		<-startCh                     //wait consumers to start
		log.Debugf("Started consumers")
		kafkaProducerWorker(p, "topic0", 0, KEY, i, t)
		wg.Wait() // wait consumers to finish

		o, err := p.getOffsets("topic0")
		test.CheckFail(err, t)
		log.Debugf("Produce %v event, Consume %v event and gracefully close. Current offsets: %+v", i, i, o)
		kafkaProducerWorker(p, "topic0", 100, KEY, i, t)
		kafkaConsumerWorker(p, "topic0", 100, i, true, t)
		<-startCh //pop so next kafka consumer doesn't block
		o1, err := p.getOffsets("topic0")
		test.CheckFail(err, t)

		if o[0].offset+int64(i) != o1[0].offset {
			t.Fatalf("Offset for %v consumed message(s) should be persisted. Offset before %+v, offsets after: %v", i, o, o1)
		}

		o, err = p.getOffsets("topic0")
		test.CheckFail(err, t)
		log.Debugf("Produce %v event, Consume %v event and failure close", i, i)
		kafkaProducerWorker(p, "topic0", 1000, KEY, i*2, t)
		kafkaConsumerWorker(p, "topic0", 1000, i*2, false, t) //offsets of last batch should not be persisted
		<-startCh
		o1, err = p.getOffsets("topic0")
		test.CheckFail(err, t)

		if o[0].offset+int64(i) != o1[0].offset {
			t.Fatalf("Offset for %v consumed message(s) should NOT be persisted. Offset before %+v, offsets after: %v", i, o, o1)
		}

		kafkaConsumerWorker(p, "topic0", 1000+i, i, true, t) //offsets should be persisted
		<-startCh
		o1, err = p.getOffsets("topic0")
		test.CheckFail(err, t)

		if o[0].offset+int64(i*2) != o1[0].offset {
			t.Fatalf("Offset for %v consumed message(s) should be persisted. Offset before %+v, offsets after: %v", 2*i, o, o1)
		}

		log.Debugf("Check that we can start consuming messages after graceful shutdown")
		startConsumers(p, 1, 0, i, t) //this consumer and next producer is to persist current offset
		<-startCh
		kafkaProducerWorker(p, "topic0", 0, KEY, i, t)
		wg.Wait() // wait consumers to finish

		log.Debugf("Check that we are not progressing offsets out of bound after graceful shutdown")
		startConsumers(p, 1, 0, 0, t) //this consumer and next producer is to persist current offset
		<-startCh
		wg.Wait() // wait consumers to finish
	}
}

func testSimpleNto1(p Pipe, t *testing.T) {
	wg.Add(1)
	go func() { defer wg.Done(); kafkaConsumerWorker(p, "topic3333", 0, numPartitions, true, t) }()
	<-startCh //wait consumers to start
	for i := 0; i < numPartitions; i++ {
		kafkaProducerWorker(p, "topic3333", i, i, 1, t)
	}
	wg.Wait() // wait consumers to finish
}

func registerConsumers(p Pipe, pc []Consumer, topic string, n int, t *testing.T) {
	var err error
	for j := 0; j < n; j++ {
		pc[j], err = p.NewConsumer(topic)
		test.CheckFail(err, t)
	}
}

func closeConsumers(p Pipe, pc []Consumer, n int, t *testing.T) {
	for j := 0; j < n; j++ {
		err := pc[j].Close()
		test.CheckFail(err, t)
	}
}

func multiTestLoop(p Pipe, i int, t *testing.T) {
	numMsgs := 3

	pc := make([]Consumer, numPartitions)

	registerConsumers(p, pc, "topic3333", i+1, t)

	for j := 0; j < numPartitions; j++ {
		kafkaProducerWorker(p, "topic3333", j*numMsgs, j, numMsgs, t)
	}

	j := 0
	res := make(map[string]bool)
	partsPerConsumer := numPartitions / (i + 1 - j)
	for k := 0; k < numPartitions; k++ {
		log.Debugf("consumer %v msgs k=%v", j, k)
		for l := 0; l < numMsgs; l++ {
			r := consumeMessage(pc[j], t)

			if res[r] {
				t.Fatalf("Duplicate entry for %v", r)
			}

			res[r] = true

			log.Debugf("consumer %v msgs: %v k=%v", j, r, k)
		}
		if (numPartitions-k-1)%partsPerConsumer == 0 {
			j++
			if i+1 != j {
				partsPerConsumer = (numPartitions - k - 1) / (i + 1 - j)
				log.Debugf("s1ss %v", partsPerConsumer)
			}
		}
	}

	closeConsumers(p, pc, i+1, t)

	if len(res) < numMsgs*numPartitions {
		t.Fatalf("Too few messages %v, expected %v", len(res), numMsgs*numPartitions)
	} else if len(res) > numMsgs*numPartitions {
		t.Fatalf("Too many messages %v, expected %v", len(res), numMsgs*numPartitions)
	}

	for j := 0; j < numMsgs*numPartitions; j++ {
		e := "topic3333key." + strconv.Itoa(j)
		log.Debugf("msg %v", e)
		if !res[e] {
			t.Fatalf("Absent: %v", e)
		}
	}
}

func TestKafkaType(t *testing.T) {
	pt := "kafka"
	p, _ := initKafkaPipe(nil, 0, cfg, nil)
	test.Assert(t, p.Type() == pt, "type should be "+pt)
}

//WARN: There is no way to control number of partition programmatically
//so the test relies on the server configuration:
//num.partitions=8
//auto.create.topics.enable=true
func TestKafkaMultiPartition(t *testing.T) {
	test.SkipIfNoKafkaAvailable(t)
	test.SkipIfNoMySQLAvailable(t)

	startCh = make(chan bool, 1)

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	if !state.Init(cfg) {
		t.Fatalf("Failed to init State")
	}
	defer func() { log.E(state.Close()) }()

	_ = util.ExecSQL(state.GetDB(), "DROP TABLE IF EXISTS kafka_offsets")

	p := createPipe(1)

	//Simple test produces 1 message to N partitions, consumes all messages by one consumer
	testSimpleNto1(p, t)

	log.Debugf("Starting multi partition consumers")

	//Vary number of consumer from 0 to numPartitions
	for i := 0; i < numPartitions; i++ {
		multiTestLoop(p, i, t)
	}

	log.Debugf("Check that consumer saves only offsets for partition it consumes from")

	pc := make([]Consumer, numPartitions)
	//Every consumer consumes two partitions
	registerConsumers(p, pc, "topic3333", numPartitions/2, t)

	//Write one message to every partition
	for j := 0; j < numPartitions; j++ {
		kafkaProducerWorker(p, "topic3333", j, j, 1, t)
	}

	for j := 0; j < numPartitions; j++ {
		consumeMessage(pc[j/2], t)
	}

	log.Debugf("Consumed 2 messages from every consumer")

	o1, err := p.getOffsets("topic3333")
	test.CheckFail(err, t)
	for j := 0; j < numPartitions/2; j++ {
		g := j == 1
		log.Debugf("Closing %v %v", j, g)
		if g {
			err = pc[j].Close()
		} else {
			err = pc[j].CloseOnFailure()
		}
		log.Debugf("Closed %v", j)
		test.CheckFail(err, t)
	}
	o2, err := p.getOffsets("topic3333")
	test.CheckFail(err, t)
	for j := range o1 {
		if o1[j].offset != o2[j].offset && ((j != 2 && j != 3 && j != 4) || o1[j].offset+1 != o2[j].offset) {
			t.Fatalf("Offsets should be greater by one for partitions 2 and 3 and 4 the rest should be equal, got before=%v, after=%v", o1, o2)
		}
	}

	log.Debugf("Closed gracefully one of consumers")

	pc[0], err = p.NewConsumer("topic3333")
	test.CheckFail(err, t)

	log.Debugf("Consuming messages for not gracefully closed consumers")

	for j := 0; j < 5; j++ {
		consumeMessage(pc[0], t)
	}

	log.Debugf("Gracefully closing consumer")

	err = pc[0].Close()
	test.CheckFail(err, t)
}
