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

package config

import (
	"fmt"
	"testing"
)

var topicTests = [][]string{
	{"hp-tap-%s-%s-%s", "hp-tap-p1-p2-p3"},
	{"hp-tap-%s-%s", "hp-tap-p1-p2"},
	{"hp-tap-%s", "hp-tap-p1"},
	{"hp", "hp"},
	{"nm.service.%s.db.%s.table.%s", "nm.service.p1.db.p2.table.p3"},
	{"hp-tap-%s-%s-%s-v%d", "hp-tap-p1-p2-p3-v0"},
}

func TestTopicNameFormat(t *testing.T) {
	for _, r := range topicTests {
		if r[1] != GetTopicName(r[0], "p1", "p2", "p3", 0) {
			fmt.Printf("Received : %v", GetTopicName(r[0], "p1", "p2", "p3", 0))
			fmt.Printf("Reference: %v", r[1])
		}
	}
}
