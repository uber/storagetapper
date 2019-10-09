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
	"strings"
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
//meaning batchSize messages may be in flight, reading (batchSize+1)th message
//automatically acknowledges previous batch.
//  * producer caches and sends maximum batchSize messages at once
type KafkaPipe struct {
	cfg            config.PipeConfig
	conn           *sql.DB
	saramaConsumer sarama.Consumer
	consumers      map[string]*topicConsumer
	lock           sync.RWMutex //protects consumers map, which can be modified by concurrent NewConsumer/closeConsumer
}

// kafkaProducer synchronously pushes messages to Kafka using topic specified during producer creation
type kafkaProducer struct {
	topic    string
	producer sarama.SyncProducer
	batch    []*sarama.ProducerMessage
	batchPtr int
	log      log.Logger
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
	log    log.Logger
}

func init() {
	registerPlugin("kafka", initKafkaPipe)
}

func initKafkaPipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	return &KafkaPipe{conn: db, cfg: *cfg}, nil
}

// Type returns Pipe type as Kafka
func (p *KafkaPipe) Type() string {
	return "kafka"
}

// Config returns pipe configuration
func (p *KafkaPipe) Config() *config.PipeConfig {
	return &p.cfg
}

// Close release resources associated with the pipe
func (p *KafkaPipe) Close() error {
	if p.saramaConsumer == nil {
		return nil
	}
	return p.saramaConsumer.Close()
}

// Init initializes Kafka pipe creating kafka_offsets table
func (p *KafkaPipe) Init() error {
	var err error
	var cfg *sarama.Config

	if KafkaConfig != nil {
		cfg = KafkaConfig
	} else {
		cfg = sarama.NewConfig()
		cfg.ClientID = types.MySvcName
	}

	p.consumers = make(map[string]*topicConsumer)
	p.saramaConsumer, err = sarama.NewConsumer(p.cfg.Kafka.Addresses, cfg)
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

//NewProducer registers a new sync producer
func (p *KafkaPipe) NewProducer(topic string) (Producer, error) {
	l := log.WithFields(log.Fields{"topic": topic})
	producer, err := sarama.NewSyncProducer(p.cfg.Kafka.Addresses, p.producerConfig())
	if log.EL(l, err) {
		return nil, err
	}
	return &kafkaProducer{topic, producer, make([]*sarama.ProducerMessage, p.cfg.MaxBatchSize), 0, l}, nil
}

func (p *KafkaPipe) producerConfig() *sarama.Config {
	if KafkaConfig != nil {
		return KafkaConfig
	}

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.MaxMessageBytes = p.cfg.Kafka.MaxMessageBytes
	cfg.ClientID = types.MySvcName

	return cfg
}

//TODO: Think about moving offsets handling SQL to state package
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
//cancellation event also
func (p *KafkaPipe) pushPartitionMsg(ctx context.Context, ch chan *sarama.ConsumerMessage, t *kafkaPartition) bool {
	select {
	case ch <- t.nextMsg:
		t.nextMsg = nil
	case <-ctx.Done():
		return false
	}
	return true
}

/*Redistribute topic partitions amongst topic consumers */
func (p *KafkaPipe) redistributeConsumers(c *topicConsumer) {
	if len(c.consumers) == 0 {
		return
	}

	/*Stop currently running consumers and wait till they terminate*/
	if c.cancel != nil { // can be nil when called for the first time
		c.cancel()
	}
	c.wg.Wait()

	var nparts = len(c.partitions)

	log.Debugf("Distributing %v partition(s) onto %v consumer(s)", nparts, len(c.consumers))

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	j := 0
	partsPerConsumer := nparts / (len(c.consumers) - j)

	c.wg.Add(nparts)
	for i := 0; i < nparts; i++ {
		c.partitions[i].childConsumer = c.consumers[j]
		/* Partition 'i' messages goes to consumer 'j' */
		go func(ctx context.Context, i int, outCh chan *sarama.ConsumerMessage) {
			defer c.wg.Done()

			if c.partitions[i].nextMsg != nil && !p.pushPartitionMsg(ctx, outCh, &c.partitions[i]) {
				return
			}

			for {
				select {
				// FIXME: Config kafka and handle errors. By default errors logged only and not returned by the channel
				/*
					case msg := <-c.partitions[i].consumer.Errors():
						select {
						case c.consumers[j] <- msg:
						case <-c.ctx.Done():
							return
						}
				*/
				case c.partitions[i].nextMsg = <-c.partitions[i].consumer.Messages():
					//Copy data to our buffer. Key field is not used, so not copied
					b := make([]byte, len(c.partitions[i].nextMsg.Value))
					copy(b, c.partitions[i].nextMsg.Value)
					c.partitions[i].nextMsg.Value = b
					if !p.pushPartitionMsg(ctx, outCh, &c.partitions[i]) {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(ctx, i, c.consumers[j])
		log.Debugf("Started consumer, partition=%d, push to channel=%d", i, j)

		/*Try our best to equally redistribute work */
		if (nparts-i-1)%partsPerConsumer == 0 {
			j++
			if len(c.consumers) != j {
				partsPerConsumer = (nparts - i - 1) / (len(c.consumers) - j)
			}
		}
	}
}

func (p *KafkaPipe) initTopicConsumer(topic string, l log.Logger) error {
	log.Debugf("initTopic consumer %v", topic)
	c := &topicConsumer{}

	/*Initialize topic partitions on first registered consumer*/
	parts, err := p.saramaConsumer.Partitions(topic)
	if log.EL(l, err) {
		return err
	}
	offsets, err := p.getOffsets(topic)
	if log.EL(l, err) {
		return err
	}
	for _, i := range parts {
		o := InitialOffset
		if v, ok := offsets[i]; ok {
			o = v.offset
		}
		log.Debugf("start consuming partition %v from offset %v for topic %v", i, o, topic)
		pc, err := p.saramaConsumer.ConsumePartition(topic, i, o)
		if log.EL(l, err) {
			return err
		}
		c.partitions = append(c.partitions, kafkaPartition{i, InitialOffset, InitialOffset, pc, nil, nil})
	}

	p.consumers[topic] = c

	return nil
}

//NewConsumer registers a new kafka consumer
func (p *KafkaPipe) NewConsumer(topic string) (Consumer, error) {
	l := log.WithFields(log.Fields{"topic": topic})

	l.Debugf("Registering consumer")

	p.lock.Lock()
	defer p.lock.Unlock()

	/*First consumer - initialize map, kafka consumer and state*/
	if p.saramaConsumer == nil {
		if err := p.Init(); err != nil {
			return nil, err
		}
	}

	/*First consumer of this topic. Initialize*/
	if c := p.consumers[topic]; c == nil {
		err := p.initTopicConsumer(topic, l)
		if err != nil {
			return nil, err
		}
	}

	if len(p.consumers[topic].consumers) >= len(p.consumers[topic].partitions) {
		return nil, fmt.Errorf("number of consumers(%v) should be less or eqaul to the number of partitions(%v)", len(p.consumers[topic].consumers)+1, len(p.consumers[topic].partitions))
	}

	//FIXME: Can't use buffered channel here, because in the case, when one of
	//the consumer closes, messages, which are in the buffer, will be lost
	ch := make(chan *sarama.ConsumerMessage)
	//ch := make(chan *sarama.ConsumerMessage, p.batchSize)
	p.consumers[topic].consumers = append(p.consumers[topic].consumers, ch)

	p.redistributeConsumers(p.consumers[topic])

	ctx, cancel := context.WithCancel(context.Background())

	l.Debugf("Registered consumer")
	return &kafkaConsumer{p, topic, ctx, cancel, ch, nil, nil, l}, nil
}

func (p *KafkaPipe) commitOffset(topic string, partition int32, offset int64, persistInterval int64, l log.Logger) error {
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
		offset = offset - int64(p.cfg.MaxBatchSize) + 1
	} else {
		offset++ //graceful shutdown, all messages acked, start from next offset next time
	}

	if tp.offset-tp.savedOffset >= persistInterval {
		err := util.ExecSQL(p.conn, "INSERT INTO kafka_offsets VALUES(?,?,?) ON DUPLICATE KEY UPDATE offset=?", topic, partition, offset, offset)
		if log.EL(l, err) {
			return err
		}
		tp.savedOffset = tp.offset
	}

	return nil
}

func (p *kafkaConsumer) commitConsumerPartitionOffsets() error {
	var v *kafkaPartition
	for i := 0; i < len(p.pipe.consumers[p.topic].partitions); i++ {
		v = &p.pipe.consumers[p.topic].partitions[i]
		if v.childConsumer != p.ch {
			continue
		}
		if err := p.pipe.commitOffset(p.topic, v.id, v.offset, 0, p.log); err != nil {
			return err
		}
	}

	return p.err
}

//DeleteKafkaOffsets deletes Kafka offsets of all partitions of specified topic
func DeleteKafkaOffsets(topic string, conn *sql.DB) error {
	log.Debugf("Deleting old Kafka topic offsets: %v", topic)
	if err := util.ExecSQL(conn, "DELETE FROM kafka_offsets WHERE topic=?", topic); err != nil {
		if !strings.Contains(err.Error(), "doesn't exist") {
			return err
		}
	}
	return nil
}

// closeConsumer closes Kafka consumer
func (p *KafkaPipe) closeConsumer(kc *kafkaConsumer, graceful bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	kc.log.Debugf("Closing consumer. graceful %v", graceful)

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
			return fmt.Errorf("consumer doesn't belong to the pipe")
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
			if log.EL(kc.log, err) {
				return err
			}
		}

		delete(p.consumers, kc.topic)

		kc.log.Debugf("Last consumer closed. Topic deleted")
	}
	return nil
}

//Push produces message to Kafka topic
func (p *kafkaProducer) Push(in interface{}) error {
	_, _, err := p.pushLow("", in)
	return err
}

//PushK sends a keyed message to Kafka
func (p *kafkaProducer) PushK(key string, in interface{}) error {
	_, _, err := p.pushLow(key, in)
	return err
}

func (p *kafkaProducer) pushLow(key string, in interface{}) (int32, int64, error) {
	var bytes []byte
	switch b := in.(type) {
	case []byte:
		bytes = b
	default:
		return 0, 0, fmt.Errorf("kafka pipe can handle binary arrays only")
	}
	if len(bytes) == 0 {
		log.WithFields(log.Fields{"topic": p.topic}).Warnf("Producing empty message to kafka")
	}
	msg := &sarama.ProducerMessage{Topic: p.topic, Value: sarama.ByteEncoder(bytes)}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	partition, offset, err := p.producer.SendMessage(msg)
	//partition, offset, err := p.producer.SendMessage(msg)
	log.EL(p.log, err)
	//log.Debugf("Message has been sent. Partition=%v. Offset=%v\n", partition, offset)
	return partition, offset, err
}

//PushBatch stashes a keyed message into batch which will be send to Kafka by
//PushBatchCommit
func (p *kafkaProducer) PushBatch(key string, in interface{}) error {
	var bytes []byte
	switch b := in.(type) {
	case []byte:
		bytes = b
	default:
		return fmt.Errorf("kafka pipe can handle binary arrays only")
	}

	if p.batch[p.batchPtr] == nil {
		p.batch[p.batchPtr] = new(sarama.ProducerMessage)
	}

	if len(bytes) == 0 {
		log.WithFields(log.Fields{"topic": p.topic}).Warnf("Producing empty message to kafka")
	}

	p.batch[p.batchPtr].Topic = p.topic
	p.batch[p.batchPtr].Key = sarama.StringEncoder(key)
	p.batch[p.batchPtr].Value = sarama.ByteEncoder(bytes)

	p.batchPtr++

	if p.batchPtr >= len(p.batch) {
		p.batch = append(p.batch, make([]*sarama.ProducerMessage, 32)...)
	}

	return nil
}

//PushBatchCommit commits currently queued messages in the producer
func (p *kafkaProducer) PushBatchCommit() error {
	if p.batchPtr == 0 {
		return nil
	}

	err := p.producer.SendMessages(p.batch[:p.batchPtr])
	if !log.EL(p.log, err) {
		p.batchPtr = 0
	} else {
		for _, m := range err.(sarama.ProducerErrors) {
			p.log.Errorf("%v", m.Error())
		}
	}

	return err
}

func (p *kafkaProducer) PushSchema(key string, data []byte) error {
	return p.PushBatch(key, data)
}

// Close Kafka Producer
func (p *kafkaProducer) Close() error {
	err := p.producer.Close()
	log.EL(p.log, err)
	return err
}

// CloseOnFailure Kafka Producer
func (p *kafkaProducer) CloseOnFailure() error {
	err := p.producer.Close()
	log.EL(p.log, err)
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
		p.err = p.pipe.commitOffset(p.msg.Topic, p.msg.Partition, p.msg.Offset, offsetPersistInterval, p.log)
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
func (p *kafkaConsumer) close(graceful bool) error {
	p.cancel() //Unblock FetchNext

	return p.pipe.closeConsumer(p, graceful)
}

func (p *kafkaConsumer) Close() error {
	return p.close(true)
}

func (p *kafkaConsumer) CloseOnFailure() error {
	return p.close(false)
}

func (p *kafkaConsumer) SaveOffset() error {
	p.pipe.lock.Lock()
	defer p.pipe.lock.Unlock()

	return p.commitConsumerPartitionOffsets()
}

func (p *kafkaProducer) SetFormat(format string) {
}

//PartitionKey transform input row key into partition key
func (p *kafkaProducer) PartitionKey(_ string, key string) string {
	return key
}

func (p *kafkaConsumer) SetFormat(format string) {
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
