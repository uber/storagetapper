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

package snapshot

import (
	"fmt"
	"strings"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/metrics"
)

//Reader is our contract for snapshot reader
type Reader interface {
	//End uninitializes snapshot reader
	End()

	//Pop pops record fetched by FetchNext
	//returns: primary key, partition key and encoded message
	Pop() (string, string, []byte, error)

	//FetchNext fetches the record from the source and encodes using encoder provided when reader created
	//This is a blocking method
	FetchNext() bool
}

//ReaderConstructor initializes logger plugin
type ReaderConstructor func(string, string, string, string, encoder.Encoder, *metrics.Snapshot) (Reader, error)

//Plugins contains registered snapshot reader plugins
var Plugins map[string]ReaderConstructor

func registerPlugin(name string, init ReaderConstructor) {
	if Plugins == nil {
		Plugins = make(map[string]ReaderConstructor)
	}
	Plugins[name] = init
}

//Start constructs new snapshot reader of specified type
func Start(input string, svc string, cluster string, dbs string, table string, enc encoder.Encoder, m *metrics.Snapshot) (Reader, error) {
	init := Plugins[strings.ToLower(input)]

	if init == nil {
		return nil, fmt.Errorf("unsupported snapshot input type: %s", strings.ToLower(input))
	}

	return init(svc, cluster, dbs, table, enc, m)
}

//FilterRow returns the Filter query based for specified input
func FilterRow(input string, table string) string {
	filter := ""
	conf := config.Get()
	if conf.Filters != nil && conf.Filters[input] != nil {
		rowFilter, ok := conf.Filters[input][table]
		if ok && len(rowFilter.Values) > 0 {
			filter = "where"
			for _, name := range rowFilter.Values {
				filter = filter + fmt.Sprintf(" %s %s '%s' AND", rowFilter.Column, rowFilter.Condition, name)
			}
			filter = strings.TrimSuffix(filter, "AND")
		}
	}
	return filter
}
