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
	"github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
)

func init() {
	registerPlugin("logrus", configureLogrus)
}

//The indirection is needed to satisfy our logger's interface WithFields method
type logrusLogger struct {
	bark.Logger
}

func configureLogrus(level int, production bool) (Logger, error) {
	l := logrus.New()
	l.Level = logrus.Level(level)
	if production {
		l.Formatter = new(logrus.JSONFormatter)
	}
	return &logrusLogger{bark.NewLoggerFromLogrus(l)}, nil
}

func (l *logrusLogger) WithFields(keyValues Fields) Logger {
	return &logrusLogger{l.Logger.WithFields(bark.Fields(keyValues))}
}
