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
	"github.com/uber/storagetapper/util"
)

func serverTableInit(t *testing.T) {
	conn := state.ConnectLow(cfg, true)
	if conn == nil {
		t.Fatalf("failed to connect to db")
	}
	_, err := conn.Exec("TRUNCATE TABLE " + types.MyDbName + ".state")
	test.CheckFail(err, t)

	err = util.ExecSQL(conn, `DROP DATABASE IF EXISTS db1_http_test`)
	test.CheckFail(err, t)
	err = util.ExecSQL(conn, `CREATE DATABASE IF NOT EXISTS db1_http_test`)
	test.CheckFail(err, t)

	err = util.ExecSQL(conn, `CREATE TABLE db1_http_test.table1_http_test (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)
	test.CheckFail(err, t)

	err = conn.Close()
	test.CheckFail(err, t)

}

func tableRequest(cmd tableCmdReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest("POST", "/table", bytes.NewReader(body))
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	tableCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK")
	return res
}

func addTable(cluster string, service string, db string, table string, t *testing.T) {
	err := util.ExecSQL(state.GetDB(), "INSERT INTO state(cluster, service, db, tableName) VALUES (?, ?, ?, ?)", cluster, service, db, table)
	test.CheckFail(err, t)
}

func TestServerTableListCommands(t *testing.T) {
	serverTableInit(t)

	addTable("clst1", "svc1", "db1", "table1", t)
	addTable("clst2", "svc1", "db2", "table2", t)
	addTable("clst2", "svc1", "db3", "table3", t)
	addTable("clst2", "svc1", "db4", "table3", t)
	addTable("clst2", "svc2", "db1", "table4", t)
	addTable("clst2", "svc2", "db2", "table5", t)

	list := tableCmdReq{
		Cmd:     "list",
		Cluster: "clst2",
		Service: "svc1",
		Db:      "db1",
	}

	resp := tableRequest(list, http.StatusOK, t)
	if string(resp.Body.Bytes()) != "" {
		t.Fatalf("wrong response 1: '%v'", string(resp.Body.Bytes()))
	}

	list.Cluster = ""
	resp = tableRequest(list, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table1","Input":"","Output":""}
` {
		t.Fatalf("wrong response 2: '%v'", string(resp.Body.Bytes()))
	}

	list.Service = ""
	resp = tableRequest(list, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table1","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db1","Table":"table4","Input":"","Output":""}
` {
		t.Fatalf("wrong response 3: '%v'", string(resp.Body.Bytes()))
	}

	list.Cluster = "clst2"
	list.Db = ""
	resp = tableRequest(list, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst2","Service":"svc1","Db":"db2","Table":"table2","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db3","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db4","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db1","Table":"table4","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db2","Table":"table5","Input":"","Output":""}
` {
		t.Fatalf("wrong response 4: '%v'", string(resp.Body.Bytes()))
	}

	list.Service = ""
	list.Cluster = ""
	list.Db = ""
	resp = tableRequest(list, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table1","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db2","Table":"table2","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db3","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db4","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db1","Table":"table4","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db2","Table":"table5","Input":"","Output":""}
` {
		t.Fatalf("wrong response 5: '%v'", string(resp.Body.Bytes()))
	}
}

func TestServerTableAddDelCommands(t *testing.T) {
	serverTableInit(t)

	add := tableCmdReq{
		Cmd:     "add",
		Cluster: "test_cluster_1",
		Service: "test_service_1",
		Db:      "db1_http_test",
		Table:   "table1_http_test",
	}

	tableRequest(add, http.StatusOK, t)
	tableRequest(add, http.StatusOK, t)

	reg, _ := state.TableRegistered(1)
	test.Assert(t, reg, "Table should be registered")

	del := tableCmdReq{
		Cmd:     "del",
		Cluster: "test_cluster_1",
		Service: "test_service_1",
		Db:      "db1_http_test",
		Table:   "table1_http_test",
	}

	tableRequest(del, http.StatusOK, t)
	tableRequest(del, http.StatusOK, t)

	reg, _ = state.TableRegistered(1)
	test.Assert(t, !reg, "Table should not be registered")
}

func TestServerTableNegative(t *testing.T) {
	serverTableInit(t)
	add := tableCmdReq{
		Cmd:     "add",
		Cluster: "test_cluster_1",
		Service: "test_service_1",
		Db:      "db1_http_test",
		Table:   "table1_http_test",
	}
	add.Cmd = ""
	tableRequest(add, http.StatusInternalServerError, t)

	add.Cmd = "add"
	add.Cluster = ""
	tableRequest(add, http.StatusInternalServerError, t)

	add.Cluster = "test_cluster_2"
	add.Service = ""
	tableRequest(add, http.StatusInternalServerError, t)

	add.Service = "test_service_2"
	add.Db = ""
	tableRequest(add, http.StatusInternalServerError, t)

	add.Db = "test_db_2"
	add.Table = ""
	tableRequest(add, http.StatusInternalServerError, t)

	add.Cmd = "change" //unknown command
	add.Table = "test_table_1"
	tableRequest(add, http.StatusInternalServerError, t)
}
