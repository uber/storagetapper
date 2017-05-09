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

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

func schemaTableInit(t *testing.T) {
	conn := state.ConnectLow(cfg, true)
	if conn == nil {
		t.FailNow()
	}
	_, err := conn.Exec("TRUNCATE TABLE " + types.MyDbName + ".outputSchema")
	test.CheckFail(err, t)
	err = conn.Close()
	test.CheckFail(err, t)
}

func TestSchemaInfoAddDelCommands(t *testing.T) {
	schemaTableInit(t)
	add := schemaReq{
		Cmd:    "add",
		Name:   "test_schema_name1",
		Schema: "fake_schema_body", //TODO: No validation currently
	}
	body, _ := json.Marshal(add)
	req, err := http.NewRequest("POST", "/schema", bytes.NewReader(body))
	test.Assert(t, err == nil, "Schema info add failed: %v", err)
	res := httptest.NewRecorder()
	schemaCmd(res, req)
	test.Assert(t, res.Code == http.StatusOK, "Not OK")

	req, err = http.NewRequest("POST", "/schema", bytes.NewReader(body))
	test.CheckFail(err, t)
	res = httptest.NewRecorder()
	schemaCmd(res, req)
	test.Assert(t, res.Code == http.StatusInternalServerError, "Not OK")
	del := schemaReq{
		Cmd:  "del",
		Name: "test_schema_name1",
	}
	body, _ = json.Marshal(del)
	_, err = http.NewRequest("POST", "/schema", bytes.NewReader(body))
	test.Assert(t, err == nil, "Schema info del failed: %v", err)

	res = httptest.NewRecorder()
	req, err = http.NewRequest("POST", "/schema", bytes.NewReader(body))
	test.CheckFail(err, t)
	schemaCmd(res, req)
	test.Assert(t, http.StatusOK == res.Code, "Not OK")

	res = httptest.NewRecorder()
	req, err = http.NewRequest("POST", "/schema", bytes.NewReader(body))
	test.CheckFail(err, t)
	schemaCmd(res, req)
	test.Assert(t, http.StatusOK == res.Code, "Not OK")
}

func TestSchemaInfoNegative(t *testing.T) {
	schemaTableInit(t)
	add := schemaReq{
		Cmd:    "add",
		Name:   "test_schema_name1",
		Schema: "fake_schema_body",
	}
	add.Name = ""
	body, _ := json.Marshal(add)

	req, err := http.NewRequest("POST", "/schema", bytes.NewReader(body))
	test.Assert(t, err == nil, "Schema info add failed: %v", err)
	res := httptest.NewRecorder()
	schemaCmd(res, req)
	test.Assert(t, res.Code == http.StatusInternalServerError, "Not OK")

	add.Name = "test_schema_name2"
	add.Cmd = "update"
	req, err = http.NewRequest("POST", "/schema", bytes.NewReader(body))
	test.CheckFail(err, t)
	res = httptest.NewRecorder()
	schemaCmd(res, req)
	test.Assert(t, res.Code == http.StatusInternalServerError, "Not OK")
}
