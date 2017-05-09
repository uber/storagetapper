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
	"os"
	"strings"
)

//Log levels
const (
	Panic = iota
	Fatal
	Error
	Warn
	Info
	Debug
)

var levels = map[string]int{"panic": Panic + 1, "fatal": Fatal + 1, "error": Error + 1, "warn": Warn + 1, "info": Info + 1, "debug": Debug + 1}

//LoggerConstructor initializes logger plugin
type LoggerConstructor func(level int, production bool) (Logger, error)

//loggers contains registered logger plugins
var loggers map[string]LoggerConstructor

//def is a instance of default logger
var def = newStd(Info)

//Logger is our contract for the logger
type Logger interface {
	// Log at debug level with fmt.Printf-like formatting
	Debugf(format string, args ...interface{})
	// Log at info level with fmt.Printf-like formatting
	Infof(format string, args ...interface{})

	// Log at warning level with fmt.Printf-like formatting
	Warnf(format string, args ...interface{})

	// Log at error level with fmt.Printf-like formatting
	Errorf(format string, args ...interface{})

	// Log at fatal level with fmt.Printf-like formatting, then terminate process (irrecoverable)
	Fatalf(format string, args ...interface{})

	// Log at panic level with fmt.Printf-like formatting, then panic (recoverable)
	Panicf(format string, args ...interface{})

	// Return a logger with the specified key-value pairs set, to be  included in a subsequent normal logging call
	WithFields(keyValues Fields) Logger
}

//Fields is a map of key/value pairs which can be attached to a logger
//This fields will be printed with every message of the logger
type Fields map[string]interface{}

//ParseLevel converts string level to integer const
func ParseLevel(s string) int {
	if levels[strings.ToLower(s)] != 0 {
		return levels[s] - 1
	}

	return Info
}

//Configure initializes default logger
func Configure(logType string, logLevel string, production bool) {
	configure := loggers[strings.ToLower(logType)]
	if configure == nil {
		fmt.Printf("Logger '%v' is not registered, falling back to 'std'", logType)
		configure = configureStd
	}

	var err error
	def, err = configure(ParseLevel(logLevel), production)
	if err != nil {
		fmt.Printf("Error configuring logger: %v\n", err)
		os.Exit(1)
	}

	Debugf("Configured logger: %v, level: %v", logType, logLevel)
}

/*
func parentFileLineFunc(skip int) (string, int, string) {
	pc, file, no, _ := runtime.Caller(2 + skip)
	details := runtime.FuncForPC(pc)
	return path.Base(file), no, path.Base(details.Name())
}

func el(l Logger, err error, skip int) bool {
	if err != nil {
		file, no, fn := parentFileLineFunc(skip)
		//		l.Errorf("%v:%v %v: %v", file, no, fn, err.Error())
		l.WithFields(Fields{"file": file, "line": no, "func": fn}).Errorf("%v", err.Error())
		return true
	}
	return false
}
*/

//E logs the error if it's not nil
//Return true if error is not nil
func E(err error) bool {
	if err != nil {
		def.Errorf(err.Error())
	}
	return err != nil
}

//F logs the error if it's not nil and terminate the process
func F(err error) {
	if err != nil {
		def.Fatalf(err.Error())
	}
}

//EL logs the error if it's not nil with given logger
//Return true if error is not nil
//FIXME: this prints file/line of the l.Errorf call not EL caller
func EL(l Logger, err error) bool {
	if err != nil {
		l.Errorf(err.Error())
	}
	return err != nil
}

//Debugf logs a message with given format and arguments
func Debugf(format string, args ...interface{}) {
	def.Debugf(format, args...)
}

//Infof logs a message with given format and arguments
func Infof(format string, args ...interface{}) {
	def.Infof(format, args...)
}

//Warnf logs a message with given format and arguments
func Warnf(format string, args ...interface{}) {
	def.Warnf(format, args...)
}

//Errorf logs a message with given format and arguments
func Errorf(format string, args ...interface{}) {
	def.Errorf(format, args...)
}

//Fatalf logs a message with given format and arguments and then finishes the
//process with error exit code
func Fatalf(format string, args ...interface{}) {
	def.Fatalf(format, args...)
}

//Panicf logs a message with given format and arguments and then panics
func Panicf(format string, args ...interface{}) {
	def.Panicf(format, args...)
}

//WithFields attaches given key/value fields and return new logger with those
//fields attached
func WithFields(keyValues Fields) Logger {
	return def.WithFields(keyValues)
}
