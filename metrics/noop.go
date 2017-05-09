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

import "time"

type noopMetrics struct {
}

func (n *noopMetrics) InitTimer(name string) timer {
	var t noopTimer
	return &t
}

func (n *noopMetrics) InitCounter(name string) counter {
	var t noopCounter
	return &t
}

func noopMetricsInit(m *Metrics) error {
	var n noopMetrics
	m.factory = &n
	return nil
}

type noopCounter struct {
}

func (u *noopCounter) Update(value int64) {
}

func (u *noopCounter) Tag(tags map[string]string) {
}

type noopTimer struct {
}

func (u *noopTimer) Start() {
}

func (u *noopTimer) Stop() {
}

func (u *noopTimer) Record(value time.Duration) {
}

func (u *noopTimer) Tag(tags map[string]string) {
}
