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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uber/storagetapper/config"
)

func init() {
	if loggers == nil {
		loggers = make(map[string]LoggerConstructor)
	}
	loggers["zap"] = configureZap
}

type zapConfig struct {
	Logging zap.Config
}

type zapLogger struct {
	*zap.SugaredLogger
}

func configureZap(level int, production bool) (Logger, error) {
	var cfg zapConfig
	cfg.Logging = zap.NewProductionConfig()
	if !production {
		cfg.Logging = zap.NewDevelopmentConfig()
	}

	cfg.Logging.Level = zap.NewAtomicLevelAt(zapcore.Level(Debug - level - 1))

	if err := config.LoadSection(&cfg); err != nil {
		return nil, err
	}

	l, err := cfg.Logging.Build(zap.AddCallerSkip(1))
	if err != nil {
		return nil, err
	}
	return &zapLogger{l.Sugar()}, nil

}

//FIXME: Need to set zap.AddCallerSkip(0) to show correct line numbers for non
//default loggers
func (l *zapLogger) WithFields(keyValues Fields) Logger {
	var f = make([]interface{}, 0)
	for k, v := range keyValues {
		f = append(f, k)
		f = append(f, v)
	}
	n := l.With(f...)
	return &zapLogger{n}
}
