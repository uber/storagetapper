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
	"database/sql"
	"fmt"
	"golang.org/x/net/context" //"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

//Initial offset type
var (
	OffsetOldest = sarama.OffsetOldest
	OffsetNewest = sarama.OffsetNewest
)

//offsetPersistInterval determines how often offsets will be persisted,
//meaning at most offsetPersistInterval messages will be resent
//after failure restart
//At the moment of when we persist offset, we are pesisting not current offset
//but current offset minus batchSize, to guarantee resending at least batchSize
//last messages after failure restart
var offsetPersistInterval int64 = 10000

//InitialOffset allows to configure global initial offset from which to start
//consuming partitions which doesn't have offsets stored in the kafka_offsets table
var InitialOffset = OffsetNewest

//KafkaConfig global per process Sarama config
var KafkaConfig *sarama.Config

type kafkaPartition struct {
	id            int32
	offset        int64
	savedOffset   int64
	consumer      sarama.PartitionConsumer
	childConsumer chan *sarama.ConsumerMessage
	nextMsg       *sarama.ConsumerMessage
}

type topicConsumer struct {
	partitions []kafkaPartition
	consumers  []chan *sarama.ConsumerMessage
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// KafkaPipe is wrapper on top of Sarama library to produce/consume through kafka
//  * after failure shutdown pipe guarantees to resent last batchSize messages,
//meaning batchSize messages may be inflight, reading (batchSize+1)th message
//automatically acknowledges previous batch.
//  * producer caches and sents maximum batchSize messages at once
type KafkaPipe struct {
	ctx            context.Context
	kafkaAddrs     []string
	conn           *sql.DB
	saramaConsumer sarama.Consumer
	consumers      map[string]*topicConsumer
	lock           sync.RWMutex //protects consumers map, which can be modified by concurrent RegisterConsumer/CloseConsumer
	batchSize      int
	Config         *sarama.Config
}

// kafkaProducer synchronously pushes messages to Kafka using topic specified during producer creation
type kafkaProducer struct {
	topic    string
	ctx      context.Context
	producer sarama.SyncProducer
	batch    []*sarama.ProducerMessage
	batchPtr int
}

// kafkaConsumer consumes messages from Kafka using topic and partition specified during consumer creation
type kafkaConsumer struct {
	pipe   *KafkaPipe
	topic  string
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan *sarama.ConsumerMessage
	msg    *sarama.ConsumerMessage
	err    error
}

func init() {
	registerPlugin("kafka", initKafkaPipe)
}

func initKafkaPipe(pctx context.Context, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error) {
	return &KafkaPipe{ctx: pctx, kafkaAddrs: cfg.KafkaAddrs, conn: db, batchSize: batchSize}, nil
}

// Type returns Pipe type as Kafka
func (p *KafkaPipe) Type() string {
	return "kafka"
}

// Init initializes Kafka pipe creating kafka_offsets table
func (p *KafkaPipe) Init() error {
	var err error
	var config *sarama.Config
	if KafkaConfig != nil {
		config = KafkaConfig
	}
	if p.Config != nil {
		config = p.Config
	}
	p.consumers = make(map[string]*topicConsumer)
	p.saramaConsumer, err = sarama.NewConsumer(p.kafkaAddrs, config)
	if log.E(err) {
		return err
	}
	if p.conn == nil {
		log.Warnf("No DB configured, offset won't be persisted")
		return nil
	}
	err = util.ExecSQL(p.conn, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.kafka_offsets (
		topic VARCHAR(255) CHARACTER SET utf8 NOT NULL,
		partitionId INT NOT NULL DEFAULT 0,
		offset BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY(topic, partitionId))`)
	if log.E(err) {
		return err
	}
	return nil
}

//DeleteKafkaOffsets delete offsets for specified topic
func DeleteKafkaOffsets(conn *sql.DB, topic string) bool {
	err := util.ExecSQL(conn, "DELETE FROM kafka_offsets WHERE topic=?", topic)
	log.E(err)
	return true
}

//RegisterProducer registers a new sync producer
func (p *KafkaPipe) RegisterProducer(topic string) (Producer, error) {
	var config *sarama.Config
	if KafkaConfig != nil {
		config = KafkaConfig
	}
	if p.Config != nil {
		config = p.Config
	}
	if config == nil {
		//p.config = sarama.NewConfig()
		//p.config.Producer.Partitioner = sarama.NewManualPartitioner
		//p.config.Producer.Return.Successes = true
	}
	producer, err := sarama.NewSyncProducer(p.kafkaAddrs, config)
	if log.E(err) {
		return nil, err
	}

	return &kafkaProducer{topic, p.ctx, producer, make([]*sarama.ProducerMessage, p.batchSize), 0}, nil
}

func (p *KafkaPipe) getOffsets(topic string) (map[int32]kafkaPartition, error) {
	res := make(map[int32]kafkaPartition)
	if p.conn == nil {
		return res, nil
	}
	rows, err := util.QuerySQL(p.conn, "SELECT partitionId, offset FROM kafka_offsets WHERE topic=?", topic)
	if err != nil {
		return nil, err
	}
	defer func() { log.E(rows.Close()) }()
	var r kafkaPartition
	for rows.Next() {
		if err := rows.Scan(&r.id, &r.offset); err == nil {
			if r.offset < 0 {
				r.offset = 0
			}
			res[r.id] = r
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

//pushPartitionMsg pushes message to partition consumer, while waiting for
//cancelation event also
func (p *KafkaPipe) pushPartitionMsg(ctx context.Context, ch chan *sarama.ConsumerMessage, t *kafkaPartition) bool {
	select {
	case ch <- t.nextMsg:
		t.nextMsg = nil
		//case <-shutdown.InitiatedCh():
		//	return
	case <-ctx.Done():
		return false
	}
	return true
}

/*Redistribute topic partitions amongs topic consumers */
func (p *KafkaPipe) redistributeConsumers(c *topicConsumer) {
	if len(c.consumers) == 0 {
		return
	}

	/*Stop currently running consumers and wait till they terminate*/
	c.cancel()
	c.wg.Wait()

	var nparts = len(c.partitions)

	//FIXME: Do not use shutdown here, rely on high level levels to call
	//CloseConsumer
	//shutdown.Register(int32(nparts))
	c.wg.Add(nparts)

	log.Debugf("Distributing %v partition(s) onto %v consumer(s)", nparts, len(c.consumers))

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	j := 0
	partsPerConsumer := nparts / (len(c.consumers) - j)
	for i := 0; i < nparts; i++ {
		c.partitions[i].childConsumer = c.consumers[j]
		/* Partition 'i' messages goes to consumer 'j' */
		go func(i int, outCh chan *sarama.ConsumerMessage, ctx context.Context) {
			defer c.wg.Done()
			//			defer shutdown.Done()

			if c.partitions[i].nextMsg != nil && !p.pushPartitionMsg(ctx, outCh, &c.partitions[i]) {
				return
			}

			for {
				select {
				/*FIXME: Config kafka and handle errors. By default errors logged only
				* and not returned by the channel */
				/*
					case msg := <-c.partitions[i].consumer.Errors():
						select {
						case c.consumers[j] <- msg:
						case <-c.ctx.Done():
							return
						}
				*/
				case c.partitions[i].nextMsg = <-c.partitions[i].consumer.Messages():
					//Copy data to our buffer. Key field is not used, so not
					//copied
					b := make([]byte, len(c.partitions[i].nextMsg.Value))
					copy(b, c.partitions[i].nextMsg.Value)
					c.partitions[i].nextMsg.Value = b
					if !p.pushPartitionMsg(ctx, outCh, &c.partitions[i]) {
						return
					}
				//case <-shutdown.InitiatedCh():
				//	return
				case <-ctx.Done():
					return
				}
			}
		}(i, c.consumers[j], ctx)
		log.Debugf("Started consumer, partition=%d, push to channel=%d\n", i, j)
		/*Try our best to equally redistribute work */
		if (nparts-i-1)%partsPerConsumer == 0 {
			j++
			if len(c.consumers) != j {
				partsPerConsumer = (nparts - i - 1) / (len(c.consumers) - j)
			}
		}
	}
}

func (p *KafkaPipe) initTopicConsumer(topic string) error {
	log.Debugf("initTopic consumer %v", topic)
	c := &topicConsumer{}
	_, c.cancel = context.WithCancel(context.Background())

	/*Initialize topic partitions on first registered consumer*/
	parts, err := p.saramaConsumer.Partitions(topic)
	if log.E(err) {
		return err
	}
	offsets, err := p.getOffsets(topic)
	if log.E(err) {
		return err
	}
	for _, i := range parts {
		o := InitialOffset
		if v, ok := offsets[i]; ok {
			o = v.offset
		}
		log.Debugf("start consuming partition %v from offset %v for topic %v", i, o, topic)
		pc, err := p.saramaConsumer.ConsumePartition(topic, i, o)
		if log.E(err) {
			return err
		}
		c.partitions = append(c.partitions, kafkaPartition{i, InitialOffset, InitialOffset, pc, nil, nil})
	}

	p.consumers[topic] = c

	return nil
}

//RegisterConsumer registers a new kafka consumer
func (p *KafkaPipe) RegisterConsumer(topic string) (Consumer, error) {
	log.Debugf("Registering consumer %v", topic)

	p.lock.Lock()
	defer p.lock.Unlock()

	/*First consumer - initialize map, kafka consumer and state*/
	if len(p.consumers) == 0 {
		if err := p.Init(); err != nil {
			return nil, err
		}
	}

	/*First consumer of this topic. Initialize*/
	if c := p.consumers[topic]; c == nil {
		err := p.initTopicConsumer(topic)
		if err != nil {
			return nil, err
		}
	}

	if len(p.consumers[topic].consumers) >= len(p.consumers[topic].partitions) {
		return nil, fmt.Errorf("Number of consumers(%v) should be less or eqaul to the number of partitions(%v)", len(p.consumers[topic].consumers)+1, len(p.consumers[topic].partitions))
	}

	//FIXME: Can't use buffered channel here, because in the case, when one of
	//the consumer closes, messages, which are in the buffer, will be lost
	ch := make(chan *sarama.ConsumerMessage)
	//ch := make(chan *sarama.ConsumerMessage, p.batchSize)
	p.consumers[topic].consumers = append(p.consumers[topic].consumers, ch)

	p.redistributeConsumers(p.consumers[topic])

	ctx, cancel := context.WithCancel(context.Background())

	log.Debugf("Registered consumer %v", topic)
	return &kafkaConsumer{p, topic, ctx, cancel, ch, nil, nil}, nil
}

func (p *KafkaPipe) commitOffset(topic string, partition int32, offset int64, persistInterval int64) error {
	tc := p.consumers[topic]
	if p.conn == nil || tc == nil {
		return nil
	}

	//BUG: Wrong. Partition is id not an index
	tp := &tc.partitions[partition]
	if tp.offset == InitialOffset {
		tp.savedOffset = offset
	}
	tp.offset = offset

	if offset == InitialOffset {
		return nil
	}

	if persistInterval != 0 {
		offset = offset - int64(p.batchSize) + 1
	} else {
		offset++ //graceful shutdown, all messages acked, start from next offset next time
	}

	if tp.offset-tp.savedOffset >= persistInterval {
		err := util.ExecSQL(p.conn, "INSERT INTO kafka_offsets VALUES(?,?,?) ON DUPLICATE KEY UPDATE offset=?", topic, partition, offset, offset)
		if log.E(err) {
			return err
		}
		tp.savedOffset = tp.offset
	}

	return nil
}

// CloseProducer closes Kafka producer
func (p *KafkaPipe) CloseProducer(kc Producer) error {
	return kc.Close()
}

func (p *kafkaConsumer) commitConsumerPartitionOffsets() error {
	var v *kafkaPartition
	for i := 0; i < len(p.pipe.consumers[p.topic].partitions); i++ {
		v = &p.pipe.consumers[p.topic].partitions[i]
		if v.childConsumer != p.ch {
			continue
		}
		err := p.pipe.commitOffset(p.topic, v.id, v.offset, 0)
		if log.E(err) {
			return err
		}
	}

	return p.err
}

// CloseConsumer closes Kafka consumer
func (p *KafkaPipe) CloseConsumer(pc Consumer, graceful bool) error {
	kc := pc.(*kafkaConsumer)

	kc.cancel() //Unblock FetchNext

	p.lock.Lock()
	defer p.lock.Unlock()

	log.Debugf("Closing consumer for topic: %v, graceful %v", kc.topic, graceful)

	if graceful {
		if err := kc.commitConsumerPartitionOffsets(); err != nil {
			return err
		}
	}

	t := p.consumers[kc.topic]
	c := t.consumers

	if len(c) > 1 {
		//FIXME: Avoid linear search below
		var i int
		for i = 0; i < len(c); i++ {
			if c[i] == kc.ch {
				break
			}
		}
		if i >= len(c) {
			return fmt.Errorf("Consumer doesn't belong to the pipe")
		}

		/*Remove consumer by swapping with last element*/
		c[i] = c[len(c)-1]
		t.consumers = c[:len(c)-1]

		p.redistributeConsumers(p.consumers[kc.topic])
	} else {
		t.cancel()
		t.wg.Wait()

		for _, v := range t.partitions {
			err := v.consumer.Close()
			if log.E(err) {
				return err
			}
		}

		delete(p.consumers, kc.topic)

		log.Debugf("Last consumer closed. Topic deleted: %v", kc.topic)
	}
	return nil
}

//Push produces message to Kafka topic
func (p *kafkaProducer) Push(in interface{}) error {
	var bytes []byte
	switch in.(type) {
	case []byte:
		bytes = in.([]byte)
	default:
		return fmt.Errorf("Kafka pipe can handle binary arrays only")
	}
	msg := &sarama.ProducerMessage{Topic: p.topic, Value: sarama.ByteEncoder(bytes)}
	_, _, err := p.producer.SendMessage(msg)
	//partition, offset, err := p.producer.SendMessage(msg)
	if !log.E(err) {
		//log.Debugf("Message has been sent. Partition=%v. Offset=%v\n", partition, offset)
	}

	return err
}

//PushK sends a keyed message to Kafka
func (p *kafkaProducer) PushK(key string, in interface{}) error {
	var bytes []byte
	switch in.(type) {
	case []byte:
		bytes = in.([]byte)
	default:
		return fmt.Errorf("Kafka pipe can handle binary arrays only")
	}
	msg := &sarama.ProducerMessage{Topic: p.topic, Key: sarama.StringEncoder(key), Value: sarama.ByteEncoder(bytes)}
	_, _, err := p.producer.SendMessage(msg)
	//partition, offset, err := p.producer.SendMessage(msg)
	if !log.E(err) {
		//log.Debugf("Message has been sent. Partition=%v. Offset=%v\n", partition, offset)
	}

	return err
}

//PushBatch stashes a keyed message into batch which will be send to Kafka by
//PushBatchCommit
func (p *kafkaProducer) PushBatch(key string, in interface{}) error {
	var bytes []byte
	switch in.(type) {
	case []byte:
		bytes = in.([]byte)
	default:
		return fmt.Errorf("Kafka pipe can handle binary arrays only")
	}

	if p.batch[p.batchPtr] == nil {
		p.batch[p.batchPtr] = new(sarama.ProducerMessage)
	}

	p.batch[p.batchPtr].Topic = p.topic
	p.batch[p.batchPtr].Key = sarama.StringEncoder(key)
	p.batch[p.batchPtr].Value = sarama.ByteEncoder(bytes)

	p.batchPtr++

	if p.batchPtr >= len(p.batch) {
		return p.PushBatchCommit()
	}

	return nil
}

//PushBatchCommit commits currently queued messages in the producer
func (p *kafkaProducer) PushBatchCommit() error {
	if p.batchPtr == 0 {
		return nil
	}

	err := p.producer.SendMessages(p.batch[:p.batchPtr])
	if !log.E(err) {
		//log.Debugf("Message batch has been sent. batchSize=%v\n", p.batchPtr)
		p.batchPtr = 0
	} else {
		for i, m := range err.(sarama.ProducerErrors) {
			log.Errorf("%v: %v", i, m.Error())
		}
	}

	return err
}

// Close Kafka Producer
func (p *kafkaProducer) Close() error {
	err := p.producer.Close()
	log.E(err)
	return err
}

//FetchNext fetches next message from Kafka and commits offset read
func (p *kafkaConsumer) FetchNext() bool {
	select {
	case msg, ok := <-p.ch:
		if !ok {
			return false
		}
		p.msg = msg
		p.pipe.lock.RLock()
		defer p.pipe.lock.RUnlock()
		p.err = p.pipe.commitOffset(p.msg.Topic, p.msg.Partition, p.msg.Offset, offsetPersistInterval)
		log.E(p.err)
		return true
	case <-p.ctx.Done():
	}
	return false
}

//Pop pops pipe message
func (p *kafkaConsumer) Pop() (interface{}, error) {
	return p.msg.Value, p.err
}

//Close closes consumer
func (p *kafkaConsumer) Close() error {
	return nil
}

/*
type saramaLogger struct {
}

var saramalogger saramaLogger

func init() {
	sarama.Logger = &saramalogger
}

func (s *saramaLogger) Print(v ...interface{}) {
	log.Debugf("", v...)
}

func (s *saramaLogger) Printf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func (s *saramaLogger) Println(v ...interface{}) {
	log.Debugf("", v...)
}
*/
