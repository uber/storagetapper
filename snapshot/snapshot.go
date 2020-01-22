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
type ReaderConstructor func(string, string, string, string, *config.TableParams, encoder.Encoder, *metrics.Snapshot) (Reader, error)

//Plugins contains registered snapshot reader plugins
var Plugins map[string]ReaderConstructor

func registerPlugin(name string, init ReaderConstructor) {
	if Plugins == nil {
		Plugins = make(map[string]ReaderConstructor)
	}
	Plugins[name] = init
}

//Start constructs new snapshot reader of specified type
func Start(input string, svc string, cluster string, dbs string, table string, params *config.TableParams, enc encoder.Encoder, m *metrics.Snapshot) (Reader, error) {
	init := Plugins[strings.ToLower(input)]

	if init == nil {
		return nil, fmt.Errorf("unsupported snapshot input type: %s", strings.ToLower(input))
	}

	return init(svc, cluster, dbs, table, params, enc, m)
}

//FilterRow returns the Filter query based for specified input
func FilterRow(input string, table string, params *config.TableParams) string {
	var rowFilters []*config.RowFilter

	if params == nil || len(params.RowFilter.Values) == 0 || params.RowFilter.Condition == "" || params.RowFilter.Column == "" {
		// if the legacy RowFilter is not set, use RowFilters if any
		if params != nil && len(params.RowFilters) > 0 {
			for _, rowFilter := range params.RowFilters {
				rf := rowFilter
				rowFilters = append(rowFilters, &rf)
			}
			return getWhereClauseFromRowFilters(rowFilters)
		}
		conf := config.Get()
		if conf.Filters == nil {
			return ""
		}
		if _, ok := conf.Filters[input]; !ok || conf.Filters[input] == nil {
			return ""
		}
		rf, ok := conf.Filters[input][table]
		if !ok || len(rf.Values) == 0 || rf.Condition == "" || rf.Column == "" {
			return ""
		}
		rowFilters = append(rowFilters, &rf)
	} else {
		rowFilters = append(rowFilters, &params.RowFilter)
	}

	return getWhereClauseFromRowFilters(rowFilters)
}

//ForceIndex returns the FORCE INDEX clause
func ForceIndex(params *config.TableParams) string {
	if params == nil || params.ForceIndex == "" {
		conf := config.Get()
		return getForceIndexClause(conf.ForceIndex)
	}
	return getForceIndexClause(params.ForceIndex)
}

// getWhereClauseFromRowFilters generates WHERE clause from a list of RowFilter
func getWhereClauseFromRowFilters(rowFilters []*config.RowFilter) string {
	subfilters := make([]string, 0, len(rowFilters))
	for _, rowFilter := range rowFilters {
		var colFilters []string
		op := rowFilter.Operator
		if op == "" {
			op = "AND"
		}

		for _, val := range rowFilter.Values {
			colFilters = append(colFilters, fmt.Sprintf("`%s` %s '%s'", rowFilter.Column, rowFilter.Condition, val))
		}

		if len(colFilters) == 0 {
			continue
		}
		subfilter := strings.Join(colFilters, fmt.Sprintf(" %s ", op))
		subfilters = append(subfilters, fmt.Sprintf("(%s)", subfilter))
	}

	if len(subfilters) == 0 {
		return ""
	}

	return "WHERE " + strings.Join(subfilters, " AND ")
}

//getForceIndexClause generates the FORCE INDEX clause from the given index name if any
func getForceIndexClause(index string) string {
	if index != "" {
		return fmt.Sprintf("FORCE INDEX (%s)", index)
	}
	return ""
}
