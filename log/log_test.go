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
}
