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

// +build go1.7

package log

import (
	"fmt"
	"testing"
)

func TestZapBasic(t *testing.T) {
	for i := 0; i <= Debug; i++ {
		fmt.Printf("Level: %v\n", i)

		var err error
		def, err = configureZap(i, false)
		if err != nil {
			t.Fatalf("Init failed %v", err.Error())
		}

		Debugf("msg")
		Infof("msg")
		Warnf("msg")
		Errorf("msg")

		lf := WithFields(Fields{"Test field 1": "val1", "Test field2": "val2"})

		lf.Debugf("msg with fields")
		lf.Infof("msg with fields")
		lf.Warnf("msg with fields")
		lf.Errorf("msg with fields")
	}
}
