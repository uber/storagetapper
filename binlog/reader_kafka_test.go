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

package binlog

import (
	"testing"

	"github.com/uber/storagetapper/pipe"
)

func TestKafkaBasic(t *testing.T) {
	CheckQueries(pipe.Kafka, testBasicPrepare, testBasic, testBasicResult, t)
}

func TestKafkaUseDB(t *testing.T) {
	CheckQueries(pipe.Kafka, testBasicPrepare, testUseDB, testUseDBResult, t)
}

func TestKafkaMultiColumn(t *testing.T) {
	CheckQueries(pipe.Kafka, testMultiColumnPrepare, testMultiColumn, testMultiColumnResult, t)
}

func TestKafkaMultiRow(t *testing.T) {
	CheckQueries(pipe.Kafka, testMultiColumnPrepare, testMultiRow, testMultiRowResult, t)
}

func TestKafkaCompoundKey(t *testing.T) {
	CheckQueries(pipe.Kafka, testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult, t)
}

func TestKafkaDDL(t *testing.T) {
	CheckQueries(pipe.Kafka, testDDLPrepare, testDDL, testDDLResult, t)
}
