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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/util"
)

func tableInit(t *testing.T) {
	var err error

	conn := state.GetDB()
	require.NotNil(t, conn)

	require.True(t, state.Reset())

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
}

func tableJSONRequest(cmd tableCmdReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest("POST", "/table", bytes.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	tableCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK")

	switch cmd.Cmd {
	case "del":
		require.True(t, state.SyncDeregisteredTables())
	case "add":
		require.True(t, state.SyncRegisteredTables())
	}

	return res
}

func tableFormRequest(typ string, cmd tableCmdReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body := url.Values{}
	body.Add("cmd", cmd.Cmd)
	body.Add("cluster", cmd.Cluster)
	body.Add("service", cmd.Service)
	body.Add("db", cmd.DB)
	body.Add("table", cmd.Table)
	if cmd.Table != "" && cmd.Cmd != "list" {
		body.Set("table", cmd.Table+"_"+typ)
	}
	body.Add("input", cmd.Input)
	body.Add("output", cmd.Output)
	body.Add("outputFormat", cmd.OutputFormat)
	body.Add("version", fmt.Sprintf("%v", cmd.Version))
	body.Add("offset", fmt.Sprintf("%v", cmd.Offset))
	body.Add("limit", fmt.Sprintf("%v", cmd.Limit))
	body.Add("filter", cmd.Filter)
	req, err := http.NewRequest("GET", "/table?"+body.Encode(), nil)
	if typ == "POST" {
		req, err = http.NewRequest("POST", "/table", strings.NewReader(body.Encode()))
	}
	test.Assert(t, err == nil, "Failed: %v", err)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	res := httptest.NewRecorder()
	tableCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK: code=%v", res.Code)

	return res
}

func tableRequest(cmd tableCmdReq, code int, t *testing.T) {
	tableJSONRequest(cmd, code, t)
	tableFormRequest("GET", cmd, code, t)
	tableFormRequest("POST", cmd, code, t)
}

func addTable(cluster string, service string, db string, table string, t *testing.T) {
	var stateID int64

	tx, err := state.GetDB().Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	res, err := tx.Exec("INSERT INTO registrations(cluster,service,db,table_name,input, "+
		"output,output_format,version) VALUES (?,?,?,?,?,?,?,?)", cluster, service, db, table, "mysql", "kafka", "json", 0)
	require.NoError(t, err)
	iid, err := res.LastInsertId()
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO state(service,cluster,db,table_name,input,output,output_format,version,reg_id) "+
		"VALUES (?,?,?,?,?,?,?,?,?)", service, cluster, db, table, "mysql", "kafka", "json", 0, iid)
	require.NoError(t, err)
	stateID, err = res.LastInsertId()
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO raw_schema(state_id,schema_gtid,raw_schema) VALUES (?,'','')", stateID)
	require.NoError(t, err)

	_, err = tx.Exec("INSERT IGNORE INTO cluster_state(cluster,gtid) VALUES (?,'')", cluster)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
	require.True(t, state.SyncRegisteredTables())
}

func compareResult(resp *bytes.Buffer, ref []tableListResponse, t *testing.T) {
	dec := json.NewDecoder(resp)

	for r := range ref {
		var m tableListResponse
		err := dec.Decode(&m)
		test.CheckFail(err, t)
		m.CreatedAt = time.Time{}
		m.UpdatedAt = time.Time{}
		log.Debugf("Matched: %v", m)
		if !reflect.DeepEqual(r, m) {
			test.CheckFail(err, t)
		}
	}

	var m tableListResponse
	err := dec.Decode(&m)
	test.Assert(t, err == io.EOF)
}

func TestServerTableListCommands(t *testing.T) {
	tableInit(t)

	addTable("clst1", "svc1", "db1", "table1", t)
	addTable("clst1", "svc1", "db1", "table2", t)
	addTable("clst2", "svc1", "db2", "table2", t)
	addTable("clst2", "svc1", "db3", "table3", t)
	addTable("clst2", "svc1", "db4", "table3", t)
	addTable("clst2", "svc2", "db2", "table5", t)

	req := tableCmdReq{
		Cmd:          "list",
		Cluster:      "*",
		Service:      "svc1",
		DB:           "db1",
		Table:        "*",
		Input:        "mysql",
		Output:       "kafka",
		OutputFormat: "json",
	}

	resp := tableJSONRequest(req, http.StatusOK, t)

	ref := []tableListResponse{
		{Cluster: "clst1", Service: "svc1", DB: "db1", Table: "table1", Input: "mysql", Output: "kafka", Version: 0, OutputFormat: "json"},
		{Cluster: "clst1", Service: "svc1", DB: "db1", Table: "table2", Input: "mysql", Output: "kafka", Version: 0, OutputFormat: "json"},
	}

	compareResult(resp.Body, ref, t)
}

func TestServerTableDelCommands(t *testing.T) {
	tableInit(t)

	addTable("clst1", "svc1", "db1", "table1", t)
	addTable("clst1", "svc1", "db1", "table2", t)
	addTable("clst2", "svc1", "db1", "table3", t)
	addTable("clst2", "svc2", "db1", "table4", t)

	req := tableCmdReq{
		Cluster: "clst2",
		Service: "svc1",
		DB:      "db1",
		Table:   "*",
		Input:   "mysql",
		Output:  "kafka",
	}

	req.Cmd = "del"
	resp := tableJSONRequest(req, http.StatusOK, t)
	require.Equal(t, "", resp.Body.String())

	reg, err := state.TableRegistered(req.Service, req.Cluster, req.DB, req.Table, req.Input, req.Output, req.Version, false)
	require.NoError(t, err)
	require.False(t, reg)

	req.Cluster = "*"
	req.Service = "*"
	req.Cmd = "list"
	resp = tableJSONRequest(req, http.StatusOK, t)

	ref := []tableListResponse{
		{Cluster: "clst1", Service: "svc1", DB: "db1", Table: "table1", Input: "mysql", Output: "kafka", Version: 0, OutputFormat: "json"},
		{Cluster: "clst1", Service: "svc1", DB: "db1", Table: "table2", Input: "mysql", Output: "kafka", Version: 0, OutputFormat: "json"},
		{Cluster: "clst2", Service: "svc2", DB: "db1", Table: "table4", Input: "mysql", Output: "kafka", Version: 0, OutputFormat: "json"},
	}

	compareResult(resp.Body, ref, t)
}

func TestServerTableAddDelListCommands(t *testing.T) {
	tableInit(t)

	listReq := tableCmdReq{
		Cmd: "list",
	}

	resp := tableJSONRequest(listReq, http.StatusOK, t)
	require.Equal(t, ``, resp.Body.String())

	req := tableCmdReq{
		Cmd:          "add",
		Cluster:      "test_cluster_1",
		Service:      "test_service_1",
		DB:           "st_table_http_test0",
		Table:        "table_http_test0",
		Input:        "mysql",
		Output:       "kafka",
		OutputFormat: "json",
		Params:       `{"Pipe":{"Compression":true}}`,
	}

	tableRequest(req, http.StatusOK, t)

	//Remove *_GET, *_POST tables from previous request
	tableInit(t)

	tableJSONRequest(req, http.StatusOK, t)

	reg, _ := state.TableRegisteredInState(1)
	require.True(t, reg, "Table st_table_http_test0.table_http_test0 should be registered")

	resp = tableJSONRequest(listReq, http.StatusOK, t)

	ref := []tableListResponse{{Cluster: "test_cluster_1", Service: "test_service_1", DB: "st_table_http_test0", Table: "table_http_test0", Input: "mysql", Output: "kafka", Version: 0, OutputFormat: "json", Params: "{\"Pipe\":{\"Compression\":true}}"}}

	compareResult(resp.Body, ref, t)

	req.Cmd = "del"
	tableRequest(req, http.StatusOK, t)

	reg, _ = state.TableRegisteredInState(1)
	require.False(t, reg, "Table should not be registered")

	resp = tableJSONRequest(listReq, http.StatusOK, t)
	require.Equal(t, ``, resp.Body.String())
}

func TestServerTableCmdIncorrectParams(t *testing.T) {
	tableInit(t)
	add := tableCmdReq{
		Cmd:     "add",
		Cluster: "test_cluster_1",
		Service: "test_service_1",
		DB:      "db1_http_test",
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
	add.DB = ""
	tableRequest(add, http.StatusInternalServerError, t)

	add.DB = "test_db_2"
	add.Table = ""
	tableRequest(add, http.StatusInternalServerError, t)

	add.Cmd = "change" //unknown command
	add.Table = "test_table_1"
	tableRequest(add, http.StatusInternalServerError, t)

	req, err := http.NewRequest("POST", "/table", bytes.NewReader([]byte("this is supposed to be garbage formatted json")))
	test.Assert(t, err == nil, "Failed: %v", err)
	req.Header.Add("Content-Type", "application/json")
	res := httptest.NewRecorder()
	tableCmd(res, req)
	test.Assert(t, res.Code == http.StatusInternalServerError, "Not OK")
}
