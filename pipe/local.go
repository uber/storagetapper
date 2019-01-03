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

//localPipe pipe based on channels
type localPipe struct {
	mutex sync.Mutex
	ch    map[string](chan interface{})
	cfg   config.PipeConfig
}

//localProducerConsumer implements both producer and consumer
type localProducerConsumer struct {
	ch     chan interface{}
	ctx    context.Context
	cancel context.CancelFunc
	msg    interface{}
}

func init() {
	registerPlugin("local", initLocalPipe)
}

func initLocalPipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	return &localPipe{ch: make(map[string](chan interface{})), cfg: *cfg}, nil
}

//Type returns type of the type
func (p *localPipe) Type() string {
	return "local"
}

// Config returns pipe configuration
func (p *localPipe) Config() *config.PipeConfig {
	return &p.cfg
}

// Close releases resources associated with the pipe
func (p *localPipe) Close() error {
	return nil
}

func (p *localPipe) registerProducerConsumer(key string) (*localProducerConsumer, error) {
	p.mutex.Lock()
	ch := p.ch[key]
	if ch == nil {
		ch = make(chan interface{}, p.cfg.MaxBatchSize)
		p.ch[key] = ch
	}
	p.mutex.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	return &localProducerConsumer{ch, ctx, cancel, nil}, nil
}

//NewConsumer registers consumer with the given pipe name
func (p *localPipe) NewConsumer(key string) (Consumer, error) {
	return p.registerProducerConsumer(key)
}

//NewProducer registers producer with the given pipe name
func (p *localPipe) NewProducer(key string) (Producer, error) {
	return p.registerProducerConsumer(key)
}

func (p *localProducerConsumer) pushLow(b interface{}) error {
	select {
	case p.ch <- b:
		return nil
	case <-p.ctx.Done():
	}
	return fmt.Errorf("context canceled")
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
func (p *localProducerConsumer) close(graceful bool) error {
	p.cancel()
	return nil
}

func (p *localProducerConsumer) Close() error {
	return p.close(true)
}

func (p *localProducerConsumer) CloseOnFailure() error {
	return p.close(false)
}

//SaveOffset is not applicable for local pipe
func (p *localProducerConsumer) SaveOffset() error {
	return nil
}

//SetFormat specifies format, which pipe can pass down the stack
func (p *localProducerConsumer) SetFormat(format string) {
}

func (p *localProducerConsumer) PartitionKey(_ string, key string) string {
	return key
}
