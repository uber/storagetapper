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

package test

import (
	"testing"
	"time"

	"github.com/uber/storagetapper/shutdown"
)

func TestWaitForNumProc(t *testing.T) {
	if !WaitForNumProc(1, 200*time.Millisecond) {
		t.Fatalf("There should be 0 threads")
	}

	ch := make(chan bool)
	a := func() {
		defer shutdown.Done()
		<-ch
	}

	shutdown.Register(2)
	go a()
	go a()

	if WaitForNumProc(1, 200*time.Millisecond) {
		t.Fatalf("Should timeout because there is 2 threads")
	}

	ch <- true

	if !WaitForNumProc(1, 200*time.Millisecond) {
		t.Fatalf("Should succeed so as we signaled one proc to finish %v", shutdown.NumProcs())
	}

	ch <- true

	if !WaitForNumProc(0, 200*time.Millisecond) {
		t.Fatalf("Should succeed so as we signaled second proc to finish")
	}
}

func TestWaitForNumProcGreater(t *testing.T) {
	if !WaitForNumProcGreater(0, 200*time.Millisecond) {
		t.Fatalf("There should be 0 threads")
	}

	ch := make(chan bool)
	a := func() {
		defer shutdown.Done()
		<-ch
	}

	shutdown.Register(2)
	go a()
	go a()

	if WaitForNumProcGreater(3, 200*time.Millisecond) {
		t.Fatalf("Should timeout because there is 2 threads")
	}

	ch <- true

	if !WaitForNumProcGreater(1, 200*time.Millisecond) {
		t.Fatalf("Should succeed so as we signaled one proc to finish %v", shutdown.NumProcs())
	}

	ch <- true

	if !WaitForNumProcGreater(0, 200*time.Millisecond) {
		t.Fatalf("Should succeed so as we signaled second proc to finish")
	}
}
