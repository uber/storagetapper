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
	"log"
	"os"
)

func init() {
	registerPlugin("std", configureStd)
}

func newStd(level int) Logger {
	return &stdlog{level: level, impl: log.New(os.Stderr, "", log.Lshortfile), skipFrames: 3}
}

func configureStd(level int, production bool) (Logger, error) {
	return newStd(level), nil
}

type stdlog struct {
	level      int
	impl       *log.Logger
	skipFrames int
}

func (log *stdlog) output(level int, format string, args ...interface{}) {
	if log.level >= level {
		_ = log.impl.Output(log.skipFrames+1, fmt.Sprintf(format, args...))
	}
}

//Debugf logs a message with given format and arguments
func (log *stdlog) Debugf(format string, args ...interface{}) {
	log.output(Debug, format, args...)
}

//Infof logs a message with given format and arguments
func (log *stdlog) Infof(format string, args ...interface{}) {
	log.output(Info, format, args...)
}

//Warnf logs a message with given format and arguments
func (log *stdlog) Warnf(format string, args ...interface{}) {
	log.output(Warn, format, args...)
}

//Errorf logs a message with given format and arguments
func (log *stdlog) Errorf(format string, args ...interface{}) {
	log.output(Error, format, args...)
}

//Fatalf logs a message with given format and arguments and then finishes the
//process with error exit code
func (log *stdlog) Fatalf(format string, args ...interface{}) {
	log.output(Fatal, format, args...)
	os.Exit(1)
}

//Panicf logs a message with given format and arguments and then panics
func (log *stdlog) Panicf(format string, args ...interface{}) {
	log.output(Panic, format, args...)
	panic(fmt.Sprintf(format, args...))
}

//WithFields attaches given key/value fields and return new logger with those
//fields attached
func (log *stdlog) WithFields(fields Fields) Logger {
	n := newStd(log.level).(*stdlog)
	n.skipFrames = 2

	if len(fields) == 0 {
		return n
	}

	var s = log.impl.Prefix()
	for k, v := range fields {
		if len(s) > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%v: %v", k, v)
	}

	n.impl.SetPrefix(s + ": ")

	return n
}
