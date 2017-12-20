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
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"

	_ "github.com/go-sql-driver/mysql"
)

var (
	TestSvc = types.MySvcName
	TestDb  = types.MyDbName
	TestTbl = "test_schema"
	DbConn  *sql.DB
	cfg     *config.AppConfig
)

//UNIT TESTING

//Test Health Check endpoint
func TestServer_HealthCheck(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	test.Assert(t, err == nil, "Error creating request object: %v", err)
	res := httptest.NewRecorder()
	healthCheck(res, req)
	test.Assert(t, res.Code == http.StatusOK, "Error! Health check call did not return status code 200")
	test.Assert(t, res.Body.String() == "OK", "Not OK")
}

func createTestSchemaTable(t *testing.T) {
	test.ExecSQL(DbConn, t, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.test_schema (
		bi BIGINT NOT NULL,
		i INT NOT NULL,
		vc VARCHAR(20) NOT NULL,
		f FLOAT,
		d DOUBLE,
		t TEXT,
		bl BLOB,
		dt DATE,
		ts TIMESTAMP,
		PRIMARY KEY(bi)
	) ENGINE=INNODB`)
}

func dropTestSchemaTable() error {
	_, err := DbConn.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, TestTbl))
	return err
}

func performSchemaRegister(t *testing.T, tbl string) *httptest.ResponseRecorder {
	body := schemaReq{
		Cmd:     "register",
		Name:    "schema_name",
		Service: TestSvc,
		Db:      TestDb,
		Table:   tbl,
		Type:    "avro",
	}
	bodyBytes, _ := json.Marshal(body)
	req, err := http.NewRequest("POST", "/schema", bytes.NewReader(bodyBytes))
	test.Assert(t, err == nil, "Error creating request object: %v", err)
	res := httptest.NewRecorder()
	schemaCmd(res, req)
	err = state.DeleteSchema(encoder.GetOutputSchemaName(TestSvc, TestDb, tbl), "avro")
	test.CheckFail(err, t)
	return res
}

func performSchemaChange(t *testing.T, alterStmt string) *httptest.ResponseRecorder {
	log.Debugf("performSchemaChange")
	body := schemaReq{
		Cmd:     "change",
		Name:    "schema_name",
		Service: TestSvc,
		Db:      TestDb,
		Table:   TestTbl,
		Alter:   alterStmt,
		Type:    "avro",
	}
	bodyBytes, _ := json.Marshal(body)
	req, err := http.NewRequest("PUT", "/schema", bytes.NewReader(bodyBytes))
	test.Assert(t, err == nil, "Error creating request object: %v", err)
	res := httptest.NewRecorder()
	schemaCmd(res, req)
	return res
}

//TestSchemaHandler_SchemaRegister_Success tests Schema Register endpoint - success case
func TestSchemaHandler_SchemaRegister(t *testing.T) {
	createTestSchemaTable(t)
	res := performSchemaRegister(t, TestTbl)
	test.Assert(t, res.Code == http.StatusOK,
		"Schema register did not return 200 HTTP code")

	//TODO: Call heatpipe to fetch schema to ensure that
	//TODO: (a) its registered (b) its the same AVRO schema (c) delete the schema version

	test.CheckFail(dropTestSchemaTable(), t)
}

//TestSchemaHandler_SchemaRegister_Failure tests SchemaRegister for non existent table errors out
func TestSchemaHandler_SchemaRegister_Failure(t *testing.T) {
	res := performSchemaRegister(t, "bad_tbl")
	test.Assert(t, res.Code == http.StatusInternalServerError,
		"Schema register for non-existent table did not return HTTP 500 code")

	//TODO: Call heatpipe to fetch schema to ensure that this schema was not registered

}

//TestSchemaHandler_SchemaChange_Compatible tests SchemaChange for a compatible schema change
func TestSchemaHandler_SchemaChange(t *testing.T) {
	createTestSchemaTable(t)
	res := performSchemaRegister(t, TestTbl)
	test.Assert(t, http.StatusOK == res.Code,
		"Schema register in schema change test did not return 200 HTTP code")

	alterStmt := fmt.Sprintf(`ALTER TABLE %s.%s ADD dcml DECIMAL`, TestDb, TestTbl)
	res = performSchemaChange(t, alterStmt)
	test.Assert(t, http.StatusOK == res.Code,
		"Schema change did not return 200 HTTP OK code")

	//TODO: Call heatpipe to fetch schema to ensure that schema
	//TODO: (a) is changed (b) its the correct Avro change (c) delete the schema version

	test.CheckFail(dropTestSchemaTable(), t)
}

//TestSchemaHandler_SchemaChange_Incompatible tests SchemaChange raises error for incompatible schema change
func TestSchemaHandler_SchemaChange_Incompatible(t *testing.T) {
	createTestSchemaTable(t)
	res := performSchemaRegister(t, TestTbl)
	test.Assert(t, http.StatusOK == res.Code,
		"Schema register in schema change test did not return 200 HTTP code")

	alterStmt := fmt.Sprintf(`ALTER TABLE %s.%s DROP COLUMN bl`, TestDb, TestTbl)
	_ = performSchemaChange(t, alterStmt)
	//TODO: This will return a failure because of incompatible change. Currently, does not call
	//TODO: the heatpipe validation endpoint. So it will get a 200
	//test.Assert(t, res.Code == http.StatusInternalServerError,
	//	"Incompatible schema change request did not raise HTTP 500 error")

	//TODO: Call heatpipe to fetch schema to ensure that schema has not changed and delete it.

	test.CheckFail(dropTestSchemaTable(), t)
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	var err error
	DbConn, err = db.OpenService(&db.Loc{Service: TestSvc, Name: TestDb}, "")
	if err != nil {
		log.Warnf("MySQL is not available")
		os.Exit(0)
	}
	if !state.Init(cfg) {
		os.Exit(1)
	}
	defer func() {
		err := DbConn.Close()
		if err != nil {
			os.Exit(1)
		}
	}()
	os.Exit(m.Run())
}
