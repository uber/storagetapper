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
	"context"
	"database/sql"

	"github.com/uber/storagetapper/config"
)

//Consumer consumer interface for the pipe
type Consumer interface {
	Pop() (interface{}, error)
	Close() error
	/*FetchNext is a blocking call which receives a message.
	  Message and error can be later retreived by Pop call.
	  If it returns false this means EOF and no more Pops allowed */
	FetchNext() bool
}

//Producer producer interface for pipe
type Producer interface {
	Push(data interface{}) error
	PushK(key string, data interface{}) error
	//PushBatch queues the messages instead of sending immediately
	PushBatch(key string, data interface{}) error
	//PushCommit writes out all the messages queued by PushBatch
	PushBatchCommit() error
	Close() error
}

//Pipe connects named producers and consumers
type Pipe interface {
	RegisterConsumer(key string) (Consumer, error)
	RegisterConsumerCtx(key string, ctx context.Context) (Consumer, error)
	RegisterProducer(key string) (Producer, error)
	CloseConsumer(p Consumer, graceful bool) error
	CloseProducer(p Producer) error
	Type() int
}

//TODO: Get this info from config, where testing config points to local
const (
	Local int = iota
	Kafka int = iota
)

//Create is a pipe factory
//Creates pipe of given type, with given buffer size
//cfg is used by Kafka pipe to get additional configuration
//db is used by Kafka pipe to save state
//pctx is used to be able to cancel blocking calls inside pipe, like during
//shutdown
func Create(tp int, batchSize int, cfg *config.AppConfig, db *sql.DB, pctx context.Context) Pipe {
	switch tp {
	case Local:
		return &LocalPipe{c: make(map[string](chan interface{})), ctx: pctx, batchSize: batchSize}
	default:
		return &KafkaPipe{ctx: pctx, kafkaAddrs: cfg.KafkaAddrs, conn: db, batchSize: batchSize}
	}
}
