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

package metrics

import "sync/atomic"

//ProcessCounter counts number of started, finished and currently running
//processes
type ProcessCounter struct {
	MStarted  *Counter
	MFinished *Counter
	MRunning  *Counter

	Started  int64
	Finished int64

	name string
}

//ProcessCounterInit is the constructor for ProcessCounter
func ProcessCounterInit(c metricsFactory, name string, tags map[string]string) *ProcessCounter {
	var p ProcessCounter
	p.MStarted = CounterInit(c, name+"_started", tags)
	p.MFinished = CounterInit(c, name+"_finished", tags)
	p.MRunning = CounterInit(c, name+"_running", tags)
	if tags != nil {
		p.MStarted.Tag(tags)
		p.MFinished.Tag(tags)
		p.MRunning.Tag(tags)
	}
	p.name = name
	return &p
}

//Tag adds metric tags
func (p *ProcessCounter) Tag(tags map[string]string) {
	p.MStarted.Tag(tags)
	p.MFinished.Tag(tags)
	p.MRunning.Tag(tags)
}

//Inc increments number of processes started and reports metric for number of
//started and running processes
func (p *ProcessCounter) Inc() {
	atomic.AddInt64(&p.Started, 1)
	p.MStarted.Set(atomic.LoadInt64(&p.Started))
	p.MRunning.Set(atomic.LoadInt64(&p.Started) - atomic.LoadInt64(&p.Finished))
}

//Dec decrements number of processes finished and reports metric for number of
//finished and running processes
func (p *ProcessCounter) Dec() {
	atomic.AddInt64(&p.Finished, 1)
	p.MFinished.Set(atomic.LoadInt64(&p.Finished))
	p.MRunning.Set(atomic.LoadInt64(&p.Started) - atomic.LoadInt64(&p.Finished))
}

//Emit current value
func (p *ProcessCounter) Emit() {
	p.MStarted.Set(atomic.LoadInt64(&p.Started))
	p.MFinished.Set(atomic.LoadInt64(&p.Finished))
	p.MRunning.Set(atomic.LoadInt64(&p.Started) - atomic.LoadInt64(&p.Finished))
}

//Get returns number of currently running processes
func (p *ProcessCounter) Get() int64 {
	return atomic.LoadInt64(&p.Started) - atomic.LoadInt64(&p.Finished)
}
