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

package shutdown

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
)

const numProcs int32 = 57

func sigTerm(t *testing.T) {
	err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	if err != nil {
		fmt.Printf("TestBool: Error: %v", err)
		t.FailNow()
	}
}

func worker() {
	<-InitiatedCh()
	Done()
}

func TestBasic(t *testing.T) {
	log.Debugf("TestBasic")
	Setup()
	Register(numProcs)
	for i := int32(0); i < numProcs; i++ {
		go worker()
	}
	if Initiated() {
		t.Fatalf("Shouldn't be initiated")
	}
	Initiate()
	if !Initiated() {
		t.Fatalf("Should be initiated")
	}
	Wait()
	if NumProcs() != 0 {
		t.FailNow()
	}
	log.Debugf("TestBasicFinished")
}

func TestInitiateAndWait(t *testing.T) {
	log.Debugf("TestInitiateAndWait")
	Setup()
	Register(numProcs)
	for i := int32(0); i < numProcs; i++ {
		go worker()
	}
	if Initiated() {
		t.Fatalf("Shouldn't be initiated")
	}
	InitiateAndWait()
	if NumProcs() != 0 {
		t.FailNow()
	}
	log.Debugf("TestInitiateAndWaitFinished")
}

func TestSignal(t *testing.T) {
	log.Debugf("TestSignal")
	Setup()
	Register(numProcs)
	for i := int32(0); i < numProcs; i++ {
		go worker()
	}
	sigTerm(t)
	Wait()
	if NumProcs() != 0 {
		t.FailNow()
	}
	log.Debugf("TestSignalFinished")
}

func pollingWorker() {
	for !Initiated() {
		time.Sleep(time.Millisecond * 50)
	}
	Done()
}

func TestBool(t *testing.T) {
	log.Debugf("TestBool")
	Setup()
	Register(numProcs)
	for i := int32(0); i < numProcs; i++ {
		go pollingWorker()
	}
	sigTerm(t)
	Wait()
	if NumProcs() != 0 {
		t.FailNow()
	}
	log.Debugf("TestBoolFinished")
}

func TestMain(m *testing.M) {
	cfg := config.Get()
	log.Configure(cfg.LogType, cfg.LogLevel, false)
	os.Exit(m.Run())
}
