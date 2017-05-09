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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
)

var cfg *config.AppConfig

func waitFor(current *int32, target int, i int, t *testing.T) {
	for atomic.LoadInt32(current) != int32(target) && i > 0 {
		time.Sleep(time.Millisecond * time.Duration(200))
		i--
	}

	if atomic.LoadInt32(current) != int32(target) {
		t.Fatalf("Expected nProcs=%v, current=%v", target, atomic.LoadInt32(current))
	}
}

func TestBasic(t *testing.T) {
	var m sync.Mutex
	var nProcs int32

	sig := make(chan bool)

	p := Create()

	if p.NumProcs() != 0 {
		t.Fatalf("Initially not zero")
	}

	p.Start(2, func() {
		m.Lock()
		atomic.AddInt32(&nProcs, 1)
		log.Debugf("Starting new proc, nProcs=%v", nProcs)
		m.Unlock()
		for !p.Terminate() {
			<-sig
			log.Debugf("Woken up")
		}
		m.Lock()
		atomic.AddInt32(&nProcs, -1)
		log.Debugf("Terminating proc, nProcs=%v", nProcs)
		m.Unlock()
	})

	/* Check that both real number and reported by thread pool equal to expected
	* value */
	waitFor(&nProcs, 2, 5, t)
	if p.NumProcs() != 2 {
		t.Fatalf("numProcs != 2")
	}

	p.Adjust(8)

	waitFor(&nProcs, 8, 5, t)
	if p.NumProcs() != 8 {
		t.Fatalf("numProcs != 8")
	}

	p.Adjust(3)

	for i := 0; i < 5; i++ {
		sig <- true
	}

	waitFor(&nProcs, 3, 5, t)
	if p.NumProcs() != 3 {
		t.Fatalf("numProcs != 3")
	}

	p.Adjust(0)
	for i := 0; i < 3; i++ {
		sig <- true
	}

	waitFor(&nProcs, 0, 5, t)
	if p.NumProcs() != 0 {
		t.Fatalf("numProcs != 0")
	}
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	log.Debugf("Config loaded %v", cfg)
	os.Exit(m.Run())
	log.Debugf("Starting shutdown")
}
