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
	"bytes"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
)

var cfg *config.AppConfig

func localConsumer(p Pipe, key string, cerr *int64) {
	defer wg.Done()
	c, err := p.NewConsumer(key)
	if log.E(err) {
		atomic.AddInt64(cerr, 1)
		return
	}
	var i int
	for c.FetchNext() {
		in, err := c.Pop()
		if log.E(err) {
			atomic.AddInt64(cerr, 1)
			return
		}
		b := in.([]byte)
		if len(b) == 0 {
			break
		}
		n := bytes.IndexByte(b, 0)
		if n == -1 {
			n = len(b)
		}
		s := string(b[:n])
		if s != key+"."+strconv.Itoa(i) {
			log.Debugf("Received: %v", s)
			log.Debugf("Expected: %v", key+"."+strconv.Itoa(i))
			atomic.AddInt64(cerr, 1)
			return
		}
		i++
	}
}

func localProducer(p Pipe, key string, cerr *int64) {
	defer wg.Done()
	c, err := p.NewProducer(key)
	if log.E(err) {
		atomic.AddInt64(cerr, 1)
		return
	}
	for i := 0; i < 1000; i++ {
		msg := key + "." + strconv.Itoa(i)
		b := []byte(msg)
		err = c.Push(b)
		if log.E(err) {
			atomic.AddInt64(cerr, 1)
			return
		}
	}
	err = c.Push(nil)
	if log.E(err) {
		atomic.AddInt64(cerr, 1)
		return
	}
}

func TestLocalBasic(t *testing.T) {
	p, err := Create("local", &cfg.Pipe, nil)
	test.CheckFail(err, t)

	var cerr int64

	wg.Add(16)
	for i := 0; i < 16; i++ {
		go localConsumer(p, "key"+strconv.Itoa(i), &cerr)
	}

	wg.Add(16)
	for i := 0; i < 16; i++ {
		go localProducer(p, "key"+strconv.Itoa(i), &cerr)
	}

	wg.Wait()

	if atomic.LoadInt64(&cerr) != 0 {
		t.FailNow()
	}
}

func TestLocalType(t *testing.T) {
	pt := "local"
	p, _ := initLocalPipe(&cfg.Pipe, nil)
	test.Assert(t, p.Type() == pt, "type should be "+pt)
}

func TestLocalPartitionKey(t *testing.T) {
	lp, _ := initLocalPipe(&cfg.Pipe, nil)
	p, err := lp.NewProducer("partition-key-test-topic")
	test.CheckFail(err, t)
	key := "some key"
	test.Assert(t, p.PartitionKey("log", key) == key, "local pipe should reteurn unmodified key")
	key = "other key"
	test.Assert(t, p.PartitionKey("snapshot", key) == key, "local pipe should return unmodified key")
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	if err := state.InitManager(shutdown.Context, cfg); err != nil {
		log.Fatalf("Failed to init State")
		os.Exit(1)
	}
	defer state.Close()

	os.Exit(m.Run())
}
