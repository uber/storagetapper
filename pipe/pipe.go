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

	"github.com/uber/storagetapper/config"
)

//Consumer consumer interface for the pipe
type Consumer interface {
	Pop() (interface{}, error)
	Close() error
	//CloseOnFailure doesn't save offsets
	CloseOnFailure() error
	/*FetchNext is a blocking call which receives a message.
	  Message and error can be later retreived by Pop call.
	  If it returns false this means EOF and no more Pops allowed */
	FetchNext() bool
	//Allows to explicitly persists current consumer position
	SaveOffset() error
}

//Producer producer interface for pipe
type Producer interface {
	Push(data interface{}) error
	PushK(key string, data interface{}) error
	PushSchema(key string, data []byte) error
	//PushBatch queues the messages instead of sending immediately
	PushBatch(key string, data interface{}) error
	//PushCommit writes out all the messages queued by PushBatch
	PushBatchCommit() error
	Close() error
}

//Pipe connects named producers and consumers
type Pipe interface {
	NewConsumer(topic string) (Consumer, error)
	NewProducer(topic string) (Producer, error)
	Type() string
}

type constructor func(pctx context.Context, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error)

//Pipes is the list of registered pipes
//Plugins insert their constructors into this map
var Pipes map[string]constructor

//registerPlugin should be called from plugin's init
func registerPlugin(name string, init constructor) {
	if Pipes == nil {
		Pipes = make(map[string]constructor)
	}
	Pipes[name] = init
}

//Create is a pipe factory
//Creates pipe of given type, with given buffer size
//cfg is used by Kafka pipe to get additional configuration
//db is used by Kafka pipe to save state
//pctx is used to be able to cancel blocking calls inside pipe, like during
//shutdown
func Create(pctx context.Context, pipeType string, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error) {

	init := Pipes[strings.ToLower(pipeType)]
	if init == nil {
		return nil, fmt.Errorf("Unsupported pipe: %s", strings.ToLower(pipeType))
	}

	pipe, err := init(pctx, batchSize, cfg, db)
	if err != nil {
		return nil, err
	}

	return pipe, nil
}
