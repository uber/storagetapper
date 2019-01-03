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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

//TODO: Test list filters

func schemaJSONRequest(cmd schemaReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest("POST", "/schema", bytes.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	schemaCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK")
	return res
}

func schemaFormRequest(typ string, cmd schemaReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body := url.Values{}
	body.Add("cmd", cmd.Cmd)
	body.Add("name", cmd.Name)
	//Prevent names collisions between Json, GET and POST requests
	if cmd.Name != "" && cmd.Cmd != "list" {
		body.Set("name", cmd.Name+"_"+typ)
	}
	body.Add("type", cmd.Type)
	body.Add("body", cmd.Body)
	body.Add("offset", fmt.Sprintf("%v", cmd.Offset))
	body.Add("limit", fmt.Sprintf("%v", cmd.Limit))
	body.Add("filter", cmd.Filter)
	req, err := http.NewRequest("GET", "/schema?"+body.Encode(), nil)
	if typ == "POST" {
		req, err = http.NewRequest("POST", "/schema", strings.NewReader(body.Encode()))
	}
	test.Assert(t, err == nil, "Failed: %v", err)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	res := httptest.NewRecorder()
	schemaCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK: code=%v", res.Code)
	return res
}

func schemaRequest(cmd schemaReq, code int, t *testing.T) {
	schemaJSONRequest(cmd, code, t)
	schemaFormRequest("GET", cmd, code, t)
	schemaFormRequest("POST", cmd, code, t)
}

func schemaTableInit(t *testing.T) {
	conn := state.GetDB()
	if conn == nil {
		t.FailNow()
	}
	test.ExecSQL(conn, t, "TRUNCATE TABLE "+types.MyDbName+".output_schema")
}

func TestSchemaInfoAddDelCommands(t *testing.T) {
	schemaTableInit(t)
	add := schemaReq{
		Cmd:  "add",
		Name: "test_schema_name1",
		Body: "fake_schema_body", //TODO: No validation currently
		Type: "avro",
	}
	schemaRequest(add, http.StatusOK, t)
	schemaRequest(add, http.StatusInternalServerError, t)

	del := schemaReq{
		Cmd:  "del",
		Name: "test_schema_name1",
		Type: "avro",
	}
	schemaRequest(del, http.StatusOK, t)
	schemaRequest(del, http.StatusOK, t)
}

func TestSchemaListCommands(t *testing.T) {
	schemaTableInit(t)

	schemas := []state.SchemaRow{
		{Name: "schema1", Type: "json", Body: "schema_body1"},
		{Name: "schema2", Type: "avro", Body: "schema_body2"},
		{Name: "schema3", Type: "msgpack", Body: "schema_body3"},
		{Name: "schema4", Type: "json", Body: "schema_body4"},
	}
	for _, v := range schemas {
		err := state.InsertSchema(v.Name, v.Type, v.Body)
		test.CheckFail(err, t)
	}

	req := schemaReq{
		Cmd: "list",
	}

	resp := schemaFormRequest("GET", req, http.StatusOK, t)
	rschemas := strings.Split(resp.Body.String(), "\n")
	if len(schemas) != len(rschemas)-1 {
		t.Fatal("Should return all 4 rows")
	}
	for i, v := range rschemas {
		if v == "" {
			continue
		}
		var a state.SchemaRow
		log.Debugf("%v", v)
		err := json.Unmarshal([]byte(v), &a)
		test.CheckFail(err, t)
		if !reflect.DeepEqual(a, schemas[i]) {
			t.Fatalf("%v: %v != %v", i, a, schemas[i])
		}
	}

	for i := range schemas {
		req := schemaReq{
			Cmd: "list",
		}
		switch i {
		case 0:
			req.Name = "schema1"
		case 1:
			req.Type = "avro"
		case 2:
			req.Body = "schema_body3"
		case 3:
			req.Body = "schema_body4"
		}
		resp := schemaFormRequest("GET", req, http.StatusOK, t)
		b, err := json.Marshal(schemas[i])
		test.CheckFail(err, t)
		if string(b)+"\n" != resp.Body.String() {
			t.Fatalf("Should return 1 record at index %v. got: %v", i, resp.Body.String())
		}
	}

	req = schemaReq{
		Cmd:    "list",
		Offset: 2,
	}
	resp = schemaFormRequest("GET", req, http.StatusOK, t)
	b, err := json.Marshal(schemas[2])
	test.CheckFail(err, t)
	rec := string(b) + "\n"
	b, err = json.Marshal(schemas[3])
	test.CheckFail(err, t)
	rec += string(b) + "\n"

	if rec != resp.Body.String() {
		t.Fatalf("Should return 2 records starting from index 2. got: %v", resp.Body.String())
	}

	req = schemaReq{
		Cmd:   "list",
		Limit: 2,
	}
	resp = schemaFormRequest("GET", req, http.StatusOK, t)
	b, err = json.Marshal(schemas[0])
	test.CheckFail(err, t)
	rec = string(b) + "\n"
	b, err = json.Marshal(schemas[1])
	test.CheckFail(err, t)
	rec += string(b) + "\n"

	if rec != resp.Body.String() {
		t.Fatalf("Should return 2 records starting from index 0. got: %v", resp.Body.String())
	}

	req = schemaReq{
		Cmd:    "list",
		Offset: 1,
		Limit:  2,
	}
	resp = schemaFormRequest("GET", req, http.StatusOK, t)
	b, err = json.Marshal(schemas[1])
	test.CheckFail(err, t)
	rec = string(b) + "\n"
	b, err = json.Marshal(schemas[2])
	test.CheckFail(err, t)
	rec += string(b) + "\n"

	if rec != resp.Body.String() {
		t.Fatalf("Should return 2 records starting from index 1. got: %v", resp.Body.String())
	}
}

func TestSchemaInfoCmdInvalidInput(t *testing.T) {
	schemaTableInit(t)
	add := schemaReq{
		Cmd:  "add",
		Name: "test_schema_name1",
		Body: "fake_schema_body",
		Type: "avro",
	}
	add.Name = ""
	schemaRequest(add, http.StatusInternalServerError, t)

	add.Name = "test_schema_name2"
	add.Cmd = "update"
	schemaRequest(add, http.StatusInternalServerError, t)

	add.Name = "test_schema_name2"
	add.Cmd = "add"
	add.Type = ""
	schemaRequest(add, http.StatusInternalServerError, t)

	req, err := http.NewRequest("POST", "/schema", bytes.NewReader([]byte("this is garbage json")))
	test.Assert(t, err == nil, "Failed: %v", err)
	req.Header.Add("Content-Type", "application/json")
	res := httptest.NewRecorder()
	schemaCmd(res, req)
	test.Assert(t, res.Code == http.StatusInternalServerError, "Not OK")
}
