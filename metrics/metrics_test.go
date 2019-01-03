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

import (
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
)

var cfg *config.AppConfig

//TODO: Write some meaningful tests

//This is a copy from test/env.go to avoid dependency cycle
func assert(t *testing.T, cond bool) {
	if !cond {
		pc, file, no, _ := runtime.Caller(1)
		details := runtime.FuncForPC(pc)
		log.Fatalf("%v:%v %v", path.Base(file), no, path.Base(details.Name()))
		t.FailNow()
	}
}

func testMetrics(c metricsConstructor, t *testing.T) {
	metricsInit = c
	if err := Init(); err != nil {
		t.Fatalf("Init failed")
	}

	b := NewChangelogReaderMetrics(map[string]string{"process": "ChangelogReader"})
	st := NewStreamerMetrics(map[string]string{"process": "Streamer"})
	sn := NewSnapshotMetrics("", map[string]string{"process": "Snapshot"})

	b.NumTablesIngesting.Inc(1)
	assert(t, int64(1) == b.NumTablesIngesting.Get())

	sn.NumWorkers.Inc()
	assert(t, int64(1) == sn.NumWorkers.Get())
	sn.NumWorkers.Dec()
	assert(t, int64(0) == sn.NumWorkers.Get())

	b.NumWorkers.Inc()
	assert(t, int64(1) == b.NumWorkers.Get())
	b.NumWorkers.Dec()
	assert(t, int64(0) == b.NumWorkers.Get())

	st.NumWorkers.Inc()
	assert(t, int64(1) == st.NumWorkers.Get())
	st.NumWorkers.Dec()
	assert(t, int64(0) == st.NumWorkers.Get())

	IdleWorkers.Inc()
	assert(t, int64(1) == IdleWorkers.Get())
	IdleWorkers.Dec()
	assert(t, int64(0) == IdleWorkers.Get())

	b.EventsRead.Inc(1)
	b.ChangelogRowEventsWritten.Inc(1)
	b.ChangelogQueryEventsWritten.Inc(1)
	b.ChangelogUnhandledEvents.Inc(1)

	st.EventsRead.Inc(1)
	st.EventsWritten.Inc(1)
	st.BytesRead.Inc(1)
	st.BytesWritten.Inc(1)

	sn.EventsRead.Inc(1)
	sn.EventsWritten.Inc(1)
	sn.BytesRead.Inc(1)
	sn.BytesWritten.Inc(10)

	st.TimeInBuffer.Record(1)

	st.TimeInBuffer.Start()
	time.Sleep(1 * time.Millisecond)
	st.TimeInBuffer.Stop()
}

func TestMetricsBasic(t *testing.T) {
	testMetrics(noopMetricsInit, t)
	testMetrics(tallyMetricsInit, t)
}

func TestMain(m *testing.M) {
	cfg = config.Get()
	if cfg == nil {
		log.Fatalf("Can't load config")
	}

	os.Exit(m.Run())
	log.Debugf("Starting shutdown")
}
