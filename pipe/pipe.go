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
	"fmt"
	"strings"
	"sync"

	//"context"

	"github.com/uber/storagetapper/config"
)

//Consumer consumer interface for the pipe
type Consumer interface {
	Close() error
	//CloseOnFailure doesn't save offsets
	CloseOnFailure() error
	Message() chan interface{}
	Error() chan error
	FetchNext() (interface{}, error)
	//Allows to explicitly persists current consumer position
	SaveOffset() error

	//SetFormat allow to tell consumer the format of the file when there is no
	//header
	SetFormat(format string)
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
	CloseOnFailure() error

	SetFormat(format string)

	PartitionKey(source string, key string) string
}

//Pipe connects named producers and consumers
type Pipe interface {
	NewConsumer(topic string) (Consumer, error)
	NewProducer(topic string) (Producer, error)
	Type() string
	Config() *config.PipeConfig
	Close() error
}

type constructor func(cfg *config.PipeConfig, db *sql.DB) (Pipe, error)

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
//pctx is used to be able to cancel blocking calls inside pipe, like during
//shutdown
func Create(pipeType string, cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {

	init := Pipes[strings.ToLower(pipeType)]
	if init == nil {
		return nil, fmt.Errorf("unsupported pipe: %s", strings.ToLower(pipeType))
	}

	pipe, err := init(cfg, db)
	if err != nil {
		return nil, err
	}

	return pipe, nil
}

type baseConsumer struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	msgCh  chan interface{}
	errCh  chan error
}

type fetchFunc func() (interface{}, error)

func (p *baseConsumer) initBaseConsumer(fn fetchFunc) {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.msgCh = make(chan interface{})
	p.errCh = make(chan error)

	p.wg.Add(1)
	go p.fetchLoop(fn)
}

func (p *baseConsumer) Message() chan interface{} {
	return p.msgCh
}
func (p *baseConsumer) Error() chan error {
	return p.errCh
}

func (p *baseConsumer) FetchNext() (interface{}, error) {
	select {
	case msg := <-p.msgCh:
		return msg, nil
	case err := <-p.errCh:
		return nil, err
	case <-p.ctx.Done():
	}
	return nil, nil
}

func (p *baseConsumer) fetchLoop(fn fetchFunc) {
	defer p.wg.Done()
	for {
		msg, err := fn()
		if err != nil {
			p.sendErr(err)
			return
		}
		if !p.sendMsg(msg) || msg == nil {
			return
		}
	}
}

func (p *baseConsumer) sendMsg(msg interface{}) bool {
	select {
	case p.msgCh <- msg:
		return true
	case <-p.ctx.Done():
	}
	return false
}

func (p *baseConsumer) sendErr(err error) {
	select {
	case p.errCh <- err:
	case <-p.ctx.Done():
	}
}
