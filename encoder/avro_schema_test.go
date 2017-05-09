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

package encoder

import (
	"testing"

	"encoding/json"

	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

/*
type inout struct {
	in  []interface{}
	out string
}

func TestGetRowKeyFromPKey(t *testing.T) {
	tests := []inout{}
	tests = append(tests, inout{in: []interface{}{"abc"}, out: "3abc"})
	tests = append(tests, inout{in: []interface{}{"abc", "def"}, out: "3abc3def"})
	tests = append(tests, inout{in: []interface{}{"ab", "cdef"}, out: "2ab4cdef"})
	tests = append(tests, inout{in: []interface{}{12, 345}, out: "2123345"})
	tests = append(tests, inout{in: []interface{}{123, 45}, out: "3123245"})
	tests = append(tests, inout{in: []interface{}{"a1b2", "3def"}, out: "4a1b243def"})
	tests = append(tests, inout{in: []interface{}{"j4", "3.13245985"}, out: "2j4103.13245985"})
	for _, test := range tests {
		test.Assert(t, GetRowKeyFromPKey(test.in), test.out)
	}
}
*/

func TestGetLatestSchema(t *testing.T) {
	av1 := `{"owner":"abcd@example.com","fields":[{"default":null,"type":["null","boolean"],"name":"test"}],"namespace":"test","name":"TEST","type":"record"}`
	var avSch types.AvroSchema
	err := json.Unmarshal([]byte(av1), &avSch)
	test.Assert(t, err == nil, "Error unmarshalling latest Avro schema into AvroSchema object")
}

func TestSchemaCodecHelper(t *testing.T) {
	av1 := `{"owner":"abcd@example.com","fields":[{"default":null,"type":["null","boolean"],"name":"test"}],"namespace":"test","name":"TEST","type":"record"}`
	var avSch types.AvroSchema
	err := json.Unmarshal([]byte(av1), &avSch)
	test.Assert(t, err == nil, "Error unmarshalling latest Avro schema into AvroSchema object")
	_, _, err = SchemaCodecHelper(&avSch)
	test.Assert(t, err == nil, "Not OK")
}
