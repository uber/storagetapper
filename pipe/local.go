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
	"sync"

	"github.com/uber/storagetapper/config"
	"golang.org/x/net/context" //"context"
)

//LocalPipe pipe based on channels
type LocalPipe struct {
	mutex     sync.Mutex
	c         map[string](chan interface{})
	ctx       context.Context
	batchSize int
}

//localProducerConsumer implements both producer and consumer
type localProducerConsumer struct {
	ch      chan interface{}
	ctx     context.Context
	msg     interface{}
	closeCh chan bool
}

func init() {
	registerPlugin("local", initLocalPipe)
}

func initLocalPipe(pctx context.Context, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error) {
	return &LocalPipe{c: make(map[string](chan interface{})), ctx: pctx, batchSize: batchSize}, nil
}

//Type returns type of the type
func (p *LocalPipe) Type() string {
	return "local"
}

func (p *LocalPipe) registerProducerConsumer(ctx context.Context, key string) (*localProducerConsumer, error) {
	p.mutex.Lock()
	ch := p.c[key]
	if ch == nil {
		ch = make(chan interface{}, p.batchSize)
		p.c[key] = ch
	}
	p.mutex.Unlock()
	return &localProducerConsumer{ch, ctx, nil, make(chan bool)}, nil
}

//RegisterConsumer registers consumer with the given pipe name
func (p *LocalPipe) RegisterConsumer(key string) (Consumer, error) {
	return p.registerProducerConsumer(p.ctx, key)
}

//CloseProducer closes given producer
func (p *LocalPipe) CloseProducer(lp Producer) error {
	return lp.Close()
}

//CloseConsumer closes give consumer
func (p *LocalPipe) CloseConsumer(lp Consumer, graceful bool) error {
	return lp.Close()
}

//RegisterProducer registers producer with the given pipe name
func (p *LocalPipe) RegisterProducer(key string) (Producer, error) {
	return p.registerProducerConsumer(p.ctx, key)
}

func (p *localProducerConsumer) pushLow(b interface{}) error {
	select {
	case p.ch <- b:
		return nil
	case <-p.ctx.Done():
	case <-p.closeCh:
	}
	return fmt.Errorf("Context canceled")
}

//Push pushes the given message to pipe
func (p *localProducerConsumer) Push(data interface{}) error {
	return p.pushLow(data)
}

//PushBatch stashes the given message in pipe buffer, all stashed messages will
//be send by subsequent PushBatchCommit call
func (p *localProducerConsumer) PushBatch(key string, data interface{}) error {
	return p.pushLow(data)
}

//PushBatchCommit sends all stashed messages to pipe
func (p *localProducerConsumer) PushBatchCommit() error {
	//TODO: Drain the channel here
	return nil
}

func (p *localProducerConsumer) PushSchema(key string, data []byte) error {
	return p.PushBatch(key, data)
}

//FetchNext receives next message from pipe. It's a blocking call, message can
//later be retreived by Pop call
func (p *localProducerConsumer) FetchNext() bool {
	select {
	case p.msg = <-p.ch:
		if p.msg == nil {
			return false
		}
		return true
	case <-p.ctx.Done():
	case <-p.closeCh:
	}
	return false
}

//Pop retreives message fetched by FetchNext
func (p *localProducerConsumer) Pop() (interface{}, error) {
	return p.msg, nil
}

//PushK pushes keyed message to pipe
func (p *localProducerConsumer) PushK(key string, b interface{}) error {
	return p.Push(b)
}

//Close producer/consumer
func (p *localProducerConsumer) Close() error {
	close(p.closeCh)
	return nil
}

//SaveOffset is not applicable for local pipe
func (p *localProducerConsumer) SaveOffset() error {
	return nil
}
