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

package db

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

// GetConnInfoForTest return address of local MySQL used by tests
func GetConnInfoForTest(dbl *Loc, connType ConnectionType, inputType string) (*Addr, error) {
	log.Infof("Fetching connection info for tests: DB %v, connType %v, inputType %v", dbl.Name, connType, inputType)

	if dbl.Cluster == "please_return_nil_db_addr" {
		return nil, fmt.Errorf("here_you_are")
	}

	return &Addr{
		Host: "localhost",
		Port: 3306,
		User: types.TestMySQLUser,
		Pwd:  types.TestMySQLPassword,
		DB:   dbl.Name,
	}, nil
}

// GetEnumeratorForTest return db location info enumerator used by tests
func GetEnumeratorForTest(svc, cluster, sdb, table, inputType string) (Enumerator, error) {
	log.Infof("Fetching enumerator for tests: svc %v, db %v, table %v inputType %v", svc, sdb, table,
		inputType)

	loc := &Loc{
		Cluster: cluster,
		Service: svc,
		Name:    sdb,
	}

	return &BuiltinEnumerator{data: []*Loc{loc}, current: -1}, nil
}

// IsValidConnForTest is DB connection validator for tests
func IsValidConnForTest(_ *Loc, _ ConnectionType, _ *Addr, _ string) bool {
	return true
}

// JSONServer starts a new server which serves json that's passed to it as an argument.
// The string should be json with a map of url->response. If the response is "error" then
// the server will return 500.
func JSONServer(js string) (*httptest.Server, map[string]json.RawMessage) {
	var out map[string]json.RawMessage
	err := json.Unmarshal([]byte(js), &out)
	if err != nil {
		panic(err)
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e, ok := out[r.URL.Path]
		if !ok {
			e, ok = out[r.URL.String()]
		}
		if !ok {
			w.WriteHeader(404)
			if _, err := w.Write([]byte("Not found")); err != nil {
				panic(err)
			}
			return
		}
		b, err := e.MarshalJSON()
		if err != nil {
			panic(err)
		}
		if string(b) == "error" {
			w.WriteHeader(500)
		}
		if _, err := w.Write(b); err != nil {
			panic(err)
		}
	}))

	return s, out
}
