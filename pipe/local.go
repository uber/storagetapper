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
	"golang.org/x/net/context" //"context"
	"sync"
)

//LocalPipe pipe based on channels
type LocalPipe struct {
	mutex     sync.Mutex
	c         map[string](chan interface{})
	ctx       context.Context
	batchSize int
}

//LocalProducerConsumer implements both producer and consumer
type LocalProducerConsumer struct {
	ch      chan interface{}
	ctx     context.Context
	msg     interface{}
	closeCh chan bool
}

//Type returns type of the type
func (p *LocalPipe) Type() int {
	return Local
}

func (p *LocalPipe) registerProducerConsumer(ctx context.Context, key string) (*LocalProducerConsumer, error) {
	p.mutex.Lock()
	ch := p.c[key]
	if ch == nil {
		ch = make(chan interface{}, p.batchSize)
		p.c[key] = ch
	}
	p.mutex.Unlock()
	return &LocalProducerConsumer{ch, ctx, nil, make(chan bool)}, nil
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

func (p *LocalProducerConsumer) pushLow(b interface{}) error {
	select {
	case p.ch <- b:
		return nil
	case <-p.ctx.Done():
	case <-p.closeCh:
	}
	return fmt.Errorf("Context canceled")
}

//Push pushes the given message to pipe
func (p *LocalProducerConsumer) Push(b interface{}) error {
	return p.pushLow(b)
}

//PushBatch stashes the given message in pipe buffer, all stashed messages will
//be send by subsequent PushBatchCommit call
func (p *LocalProducerConsumer) PushBatch(key string, b interface{}) error {
	return p.pushLow(b)
}

//PushBatchCommit sends all stashed messages to pipe
func (p *LocalProducerConsumer) PushBatchCommit() error {
	//TODO: Drain the channel here
	return nil
}

//FetchNext receives next message from pipe. It's a blocking call, message can
//later be retreived by Pop call
func (p *LocalProducerConsumer) FetchNext() bool {
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
func (p *LocalProducerConsumer) Pop() (interface{}, error) {
	return p.msg, nil
}

//PushK pushes keyed message to pipe
func (p *LocalProducerConsumer) PushK(key string, b interface{}) error {
	return p.Push(b)
}

//Close producer/consumer
func (p *LocalProducerConsumer) Close() error {
	close(p.closeCh)
	return nil
}
