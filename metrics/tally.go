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
	"io"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

//TallyConfiguration ...
type TallyConfiguration struct {
	Address  string
	Interval time.Duration
}

type tallyMetricsConfig struct {
	Metrics struct {
		Tally TallyConfiguration
	}
}

type tallyMetrics struct {
	RootScope tally.Scope
}

func (t *tallyMetrics) InitTimer(name string) timer {
	var tt tallyTimer
	tt.timer = t.RootScope.Timer(name)
	return &tt
}

func (t *tallyMetrics) InitCounter(name string) counter {
	var c tallyCounter
	c.counter = t.RootScope.Gauge(name)
	return &c
}

func (t *tallyMetrics) SubScope(name string) scope {
	return &tallyMetrics{t.RootScope.SubScope(name)}
}

func (t *tallyMetrics) Tagged(tags map[string]string) scope {
	return &tallyMetrics{t.RootScope.Tagged(tags)}
}

func newScope(address string, interval time.Duration) (tally.Scope, io.Closer) {
	statter, _ := statsd.NewBufferedClient(address,
		"stats", -1, 0)

	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:   types.MySvcName,
		Tags:     map[string]string{},
		Reporter: tallystatsd.NewReporter(statter, tallystatsd.Options{}),
	}, interval)

	return scope, closer
}

func tallyMetricsInit() (scope, error) {
	var cfg tallyMetricsConfig

	if err := config.LoadSection(&cfg); err != nil {
		return nil, err
	}

	log.Debugf("Metrics config: %+v", cfg)

	s, _ := newScope(cfg.Metrics.Tally.Address, cfg.Metrics.Tally.Interval)

	m := &tallyMetrics{
		RootScope: s,
	}

	return m, nil
}

type tallyCounter struct {
	counter tally.Gauge
}

func (t *tallyCounter) Update(value int64) {
	t.counter.Update(float64(value))
}

type tallyTimer struct {
	timer tally.Timer
	w     tally.Stopwatch
}

func (t *tallyTimer) Start() {
	t.w = t.timer.Start()
}

func (t *tallyTimer) Stop() {
	t.w.Stop()
}

func (t *tallyTimer) Record(value time.Duration) {
	t.timer.Record(value)
}
