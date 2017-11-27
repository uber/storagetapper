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
	"testing"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/test"
)

var cfg *config.AppConfig

func localConsumer(p Pipe, key string, t *testing.T) {
	defer wg.Done()
	c, err := p.NewConsumer(key)
	if err != nil {
		t.FailNow()
	}
	var i int
	for c.FetchNext() {
		in, err := c.Pop()
		b := in.([]byte)
		test.CheckFail(err, t)
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
			t.FailNow()
		}
		i++
	}
}

func localProducer(p Pipe, key string, t *testing.T) {
	defer wg.Done()
	c, err := p.NewProducer(key)
	test.CheckFail(err, t)
	for i := 0; i < 1000; i++ {
		msg := key + "." + strconv.Itoa(i)
		b := []byte(msg)
		err = c.Push(b)
		test.CheckFail(err, t)
	}
	err = c.Push(nil)
	test.CheckFail(err, t)
}

func TestLocalBasic(t *testing.T) {
	shutdown.Setup()

	p, err := Create(shutdown.Context, "local", 16, cfg, nil)
	test.CheckFail(err, t)

	wg.Add(16)
	for i := 0; i < 16; i++ {
		go localConsumer(p, "key"+strconv.Itoa(i), t)
	}

	wg.Add(16)
	for i := 0; i < 16; i++ {
		go localProducer(p, "key"+strconv.Itoa(i), t)
	}

	wg.Wait()

	shutdown.Initiate()
	shutdown.Wait()
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()

	delimited = true

	os.Exit(m.Run())
}
