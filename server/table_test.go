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
	"strconv"
	"testing"

	"github.com/uber/storagetapper/log"
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

	for i := 0; i < 3; i++ {
		err = util.ExecSQL(conn, `DROP DATABASE IF EXISTS st_table_http_test`+strconv.Itoa(i))
		test.CheckFail(err, t)
		err = util.ExecSQL(conn, `CREATE DATABASE IF NOT EXISTS st_table_http_test`+strconv.Itoa(i))
		test.CheckFail(err, t)

		err = util.ExecSQL(conn, `DROP DATABASE IF EXISTS st_table_http1_test`+strconv.Itoa(i))
		test.CheckFail(err, t)
		err = util.ExecSQL(conn, `CREATE DATABASE IF NOT EXISTS st_table_http1_test`+strconv.Itoa(i))
		test.CheckFail(err, t)

		for j := 0; j < 3; j++ {
			err = util.ExecSQL(conn, `CREATE TABLE st_table_http_test`+strconv.Itoa(i)+`.table_http_test`+strconv.Itoa(j)+` (
				field1 BIGINT PRIMARY KEY,
				field2 BIGINT NOT NULL DEFAULT 0,
				UNIQUE INDEX(field2, field1)
			)`)
			test.CheckFail(err, t)
		}

		err = util.ExecSQL(conn, `CREATE TABLE st_table_http_test`+strconv.Itoa(i)+`.table0_http_test (
			field1 BIGINT PRIMARY KEY,
			field2 BIGINT NOT NULL DEFAULT 0,
			UNIQUE INDEX(field2, field1)
		)`)
		test.CheckFail(err, t)
	}

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
	err := util.ExecSQL(state.GetDB(), "INSERT INTO state(cluster, service, db, tableName, input, output, gtid, schemaGTID, rawSchema) VALUES (?, ?, ?, ?, '', '', '', '', '')", cluster, service, db, table)
	test.CheckFail(err, t)
}

func testServerTableListDelCommands(cmd string, t *testing.T) {
	serverTableInit(t)

	addTable("clst1", "svc1", "db1", "table1", t)
	addTable("clst1", "svc1", "db1", "table2", t)
	addTable("clst2", "svc1", "db2", "table2", t)
	addTable("clst2", "svc1", "db3", "table3", t)
	addTable("clst2", "svc1", "db4", "table3", t)
	addTable("clst2", "svc2", "db1", "table4", t)
	addTable("clst2", "svc2", "db2", "table5", t)

	req := tableCmdReq{
		Cmd:     cmd,
		Cluster: "clst2",
		Service: "svc1",
		Db:      "db1",
		Table:   "*",
	}

	resp := tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != "" {
		t.Fatalf("wrong response 1: '%v'", string(resp.Body.Bytes()))
	}

	req.Cluster = "*"
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table1","Input":"","Output":""}
{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table2","Input":"","Output":""}
` {
		t.Fatalf("wrong response 2: '%v'", string(resp.Body.Bytes()))
	}

	req.Service = "*"
	log.Debugf("ref %+v", req)
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table1","Input":"","Output":""}
{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table2","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db1","Table":"table4","Input":"","Output":""}
` {
		t.Fatalf("wrong response 3: '%v'", string(resp.Body.Bytes()))
	}

	req.Cluster = "clst2"
	req.Db = "*"
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst2","Service":"svc1","Db":"db2","Table":"table2","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db3","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db4","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db1","Table":"table4","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db2","Table":"table5","Input":"","Output":""}
` {
		t.Fatalf("wrong response 4: '%v'", string(resp.Body.Bytes()))
	}

	req.Service = "*"
	req.Cluster = "*"
	req.Db = "*"
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table1","Input":"","Output":""}
{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table2","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db2","Table":"table2","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db3","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db4","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db1","Table":"table4","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db2","Table":"table5","Input":"","Output":""}
` {
		t.Fatalf("wrong response 5: '%v'", string(resp.Body.Bytes()))
	}
}

//Dry run del output is identical to output of list command
func TestServerTableDelCommands(t *testing.T) {
	testServerTableListDelCommands("del", t)
}

func TestServerTableListCommands(t *testing.T) {
	testServerTableListDelCommands("list", t)
}

func TestServerTableDelApplyCommands(t *testing.T) {
	serverTableInit(t)

	addTable("clst1", "svc1", "db1", "table1", t)
	addTable("clst1", "svc1", "db1", "table2", t)
	addTable("clst2", "svc1", "db2", "table2", t)
	addTable("clst2", "svc1", "db3", "table3", t)
	addTable("clst2", "svc1", "db4", "table3", t)
	addTable("clst2", "svc2", "db1", "table4", t)
	addTable("clst2", "svc2", "db2", "table5", t)
	addTable("clst3", "svc3", "db3", "table6", t)

	req := tableCmdReq{
		Cmd:     "del",
		Cluster: "clst2",
		Service: "svc1",
		Db:      "db1",
		Table:   "*",
		Apply:   "yes",
	}

	resp := tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != "" {
		t.Fatalf("wrong response 1: '%v'", string(resp.Body.Bytes()))
	}

	req.Cluster = "*"
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table1","Input":"","Output":""}
{"Cluster":"clst1","Service":"svc1","Db":"db1","Table":"table2","Input":"","Output":""}
` {
		t.Fatalf("wrong response 2: '%v'", string(resp.Body.Bytes()))
	}

	req.Service = "*"
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst2","Service":"svc2","Db":"db1","Table":"table4","Input":"","Output":""}
` {
		t.Fatalf("wrong response 3: '%v'", string(resp.Body.Bytes()))
	}

	req.Cluster = "clst2"
	req.Db = "*"
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst2","Service":"svc1","Db":"db2","Table":"table2","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db3","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc1","Db":"db4","Table":"table3","Input":"","Output":""}
{"Cluster":"clst2","Service":"svc2","Db":"db2","Table":"table5","Input":"","Output":""}
` {
		t.Fatalf("wrong response 4: '%v'", string(resp.Body.Bytes()))
	}

	req.Service = "*"
	req.Cluster = "*"
	req.Db = "*"
	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `{"Cluster":"clst3","Service":"svc3","Db":"db3","Table":"table6","Input":"","Output":""}
` {
		t.Fatalf("wrong response 5: '%v'", string(resp.Body.Bytes()))
	}

	resp = tableRequest(req, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `` {
		t.Fatalf("wrong response 6: '%v'", string(resp.Body.Bytes()))
	}
}

func TestServerAddCommand(t *testing.T) {
	serverTableInit(t)

	listReq := tableCmdReq{
		Cmd: "list",
	}

	var ref string
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			addReq := tableCmdReq{
				Cmd:     "add",
				Cluster: "clst1",
				Service: "svc1",
				Db:      "st_table_http_test" + strconv.Itoa(i),
				Table:   "table_http_test" + strconv.Itoa(j),
			}

			tableRequest(addReq, http.StatusOK, t)

			resp := tableRequest(listReq, http.StatusOK, t)

			ref += `{"Cluster":"clst1","Service":"svc1","Db":"st_table_http_test` + strconv.Itoa(i) + `","Table":"table_http_test` + strconv.Itoa(j) + `","Input":"mysql","Output":"kafka"}
`
			log.Debugf("ref %+v", ref)

			if string(resp.Body.Bytes()) != ref {
				t.Fatalf("wrong response: '%v'", string(resp.Body.Bytes()))
			}
		}
	}

	delAllReq := tableCmdReq{
		Cmd:     "del",
		Cluster: "*",
		Service: "*",
		Db:      "*",
		Table:   "*",
		Apply:   "yes",
	}
	tableRequest(delAllReq, http.StatusOK, t)
	resp := tableRequest(listReq, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `` {
		t.Fatalf("wrong response1: '%v'", string(resp.Body.Bytes()))
	}

	addReq := tableCmdReq{
		Cmd:     "add",
		Service: "svc1",
		Cluster: "clst1",
		Db:      "st_table_http_test%",
		Table:   "table_http_test%",
	}
	tableRequest(addReq, http.StatusOK, t)

	resp = tableRequest(listReq, http.StatusOK, t)
	if string(resp.Body.Bytes()) != ref {
		t.Fatalf("wrong response: '%v'", string(resp.Body.Bytes()))
	}

	tableRequest(delAllReq, http.StatusOK, t)
	resp = tableRequest(listReq, http.StatusOK, t)
	if string(resp.Body.Bytes()) != `` {
		t.Fatalf("wrong response1: '%v'", string(resp.Body.Bytes()))
	}
}

func TestServerTableAddDelCommandsBasic(t *testing.T) {
	serverTableInit(t)

	add := tableCmdReq{
		Cmd:     "add",
		Cluster: "test_cluster_1",
		Service: "test_service_1",
		Db:      "st_table_http_test0",
		Table:   "table_http_test0",
	}

	tableRequest(add, http.StatusOK, t)
	tableRequest(add, http.StatusOK, t)

	reg, _ := state.TableRegistered(1)
	test.Assert(t, reg, "Table should be registered")

	del := tableCmdReq{
		Cmd:     "del",
		Cluster: "test_cluster_1",
		Service: "test_service_1",
		Db:      "st_table_http_test0",
		Table:   "table_http_test0",
		Apply:   "yes",
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
