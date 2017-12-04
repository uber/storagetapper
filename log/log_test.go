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

package log

import (
	"fmt"
	"testing"
)

func TestBasic(t *testing.T) {
	Configure("std", "debug", false)

	Debugf("msg")
	Infof("msg")
	Warnf("msg")
	Errorf("msg")

	if !E(fmt.Errorf("Test error to pint")) {
		t.Fatalf("Should return true when error is not nil")
	}

	if E(nil) {
		t.Fatalf("Should return false when error is nil")
	}

	l := WithFields(Fields{"Test field 1": "val1", "Test field2": "val2"})

	l.Debugf("msg with fields")
	l.Infof("msg with fields")
	l.Warnf("msg with fields")
	l.Errorf("msg with fields")

	if !EL(l, fmt.Errorf("Print error with fields")) {
		t.Fatalf("Should return true when error is not nil")
	}

	l = WithFields(Fields{})

	l.Debugf("msg with empty fields")
	l.Infof("msg with empty fields")
	l.Warnf("msg with empty fields")
	l.Errorf("msg with empty fields")

}

func TestWrongLogger(t *testing.T) {
	Configure("non_existent_logger", "debug", false)
	if _, ok := def.(*stdlog); !ok {
		t.Fatalf("stdlog should be created by default")
	}

}

func TestWrongLevel(t *testing.T) {
	if ParseLevel("non_existent_level") != Info {
		t.Fatalf("default log level should info")
	}
}

//implementation of log interface to test panicf and fatalf calls
type nillog struct {
	fatalf int
	panicf int
}

//Debugf logs a message with given format and arguments
func (log *nillog) Debugf(format string, args ...interface{}) {
}

//Infof logs a message with given format and arguments
func (log *nillog) Infof(format string, args ...interface{}) {
}

//Warnf logs a message with given format and arguments
func (log *nillog) Warnf(format string, args ...interface{}) {
}

//Errorf logs a message with given format and arguments
func (log *nillog) Errorf(format string, args ...interface{}) {
}

//Fatalf logs a message with given format and arguments and then finishes the
//process with error exit code
func (log *nillog) Fatalf(format string, args ...interface{}) {
	log.fatalf++
}

//Panicf logs a message with given format and arguments and then panics
func (log *nillog) Panicf(format string, args ...interface{}) {
	log.panicf++
}

//WithFields attaches given key/value fields and return new logger with those
//fields attached
func (log *nillog) WithFields(fields Fields) Logger {
	return nil
}

func configureNil(level int, production bool) (Logger, error) {
	return &nillog{}, nil
}

func TestPanicAndFatal(t *testing.T) {
	registerPlugin("nillog", configureNil)

	Configure("nillog", "debug", false)

	nl, ok := def.(*nillog)

	if !ok {
		t.Fatalf("nillog should be created and be default logger")
	}

	Panicf("msg")

	if nl.panicf != 1 {
		t.Fatalf("panicf wasn't called")
	}

	Fatalf("msg")

	if nl.fatalf != 1 {
		t.Fatalf("fatalf wasn't called")
	}

	F(fmt.Errorf("Test error to pint"))
	F(nil)

	if nl.fatalf != 2 {
		t.Fatalf("fatalf wasn't called")
	}

}
