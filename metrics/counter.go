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

//Counter is a stateful counter statistics variable
type Counter struct {
	backend counter
	value   int64
	name    string
}

//CounterInit is a constructor for Counter
func CounterInit(c metricsFactory, name string, tags map[string]string) *Counter {
	var p Counter
	p.backend = c.InitCounter(name)
	if tags != nil {
		p.backend.Tag(tags)
	}
	p.name = name
	return &p
}

//Tag adds metric tags
func (p *Counter) Tag(tags map[string]string) {
	p.backend.Tag(tags)
}

//Inc increments the value by v
func (p *Counter) Inc(v int64) {
	atomic.AddInt64(&p.value, v)
	p.backend.Update(atomic.LoadInt64(&p.value))
}

//Dec increments the value by v
func (p *Counter) Dec(v int64) {
	atomic.AddInt64(&p.value, -v)
	p.backend.Update(atomic.LoadInt64(&p.value))
}

//Set sets the value of the counter gauge to a specific value
func (p *Counter) Set(v int64) {
	atomic.StoreInt64(&p.value, v)
	p.backend.Update(atomic.LoadInt64(&p.value))
}

//Get returns current value of the counter
func (p *Counter) Get() int64 {
	return atomic.LoadInt64(&p.value)
}

//Emit current value
func (p *Counter) Emit() {
	p.backend.Update(atomic.LoadInt64(&p.value))
}
