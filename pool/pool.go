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

package pool

import (
	"sync"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/shutdown"
)

/*Thread is an implementation of pool interface */
type Thread interface {
	Start(m uint, f func())
	Adjust(m uint)
	Terminate() bool
	NumProcs() uint
}

type poolImpl struct {
	mutex       sync.Mutex
	numProcs    uint
	maxNumProcs uint
	fn          func()
}

/*Create helps to hide poolImpl in the package, but not really required */
func Create() Thread {
	p := &poolImpl{}
	return p
}

/*Start instantiates a pool of size of 'm' of 'f' goroutines */
/*Start and Create separation allows to pass pool instance to 'f' goroutine */
func (p *poolImpl) Start(m uint, f func()) {
	p.fn = f
	p.Adjust(m)
}

/*Adjust resizes the pool. It creates new threads if requested size is bigger
* then current size, while it assumes threads cooperation when requested size is
* smaller then current size. Threads should periodically call Terminate function
* and obey the result. */
func (p *poolImpl) Adjust(m uint) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Debugf("Current size=%v, current maximum size=%v, requested size=%v", p.numProcs, p.maxNumProcs, m)
	p.maxNumProcs = m
	if p.numProcs < p.maxNumProcs {
		adj := p.maxNumProcs - p.numProcs
		shutdown.Register(int32(adj))
		for i := uint(0); i < adj; i++ {
			go func() { defer shutdown.Done(); p.fn() }()
		}
		p.numProcs = m
	}
}

/*Terminate return true if the caller thread need to terminate */
func (p *poolImpl) Terminate() bool {
	//Uncomment if Terminate is called frequently
	//Introduces a race when thread can miss Pool resize event, that's ok, so as
	//some other threads may see the event, or we will see it on the next
	//iteration
	//	if p.numProcs <= p.maxNumProcs {
	//		return false
	//	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.numProcs > p.maxNumProcs {
		p.numProcs--
		log.Debugf("Terminating. Current size=%v, current maximum size=%v", p.numProcs, p.maxNumProcs)
		return true
	}

	return false
}

/*NumProcs return current size of the pool */
func (p *poolImpl) NumProcs() uint {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.numProcs
}
