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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var (
	TestSvc    = types.MySvcName
	TestDB     = types.MyDBName
	TestTbl    = "test_schema"
	TestInput  = types.InputMySQL
	TestOutput = "kafka"
	TestFormat = "avro"
	DBConn     *sql.DB
	cfg        *config.AppConfig
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

var refSchema = &types.AvroSchema{Fields: []types.AvroField{
	{Name: "bi", Type: []types.AvroPrimitiveType{"null", "long"}, Default: interface{}(nil), Doc: ""},
	{Name: "i", Type: []types.AvroPrimitiveType{"null", "int"}, Default: interface{}(nil), Doc: ""},
	{Name: "vc", Type: []types.AvroPrimitiveType{"null", "string"}, Default: interface{}(nil), Doc: ""},
	{Name: "f", Type: []types.AvroPrimitiveType{"null", "float"}, Default: interface{}(nil), Doc: ""},
	{Name: "d", Type: []types.AvroPrimitiveType{"null", "double"}, Default: interface{}(nil), Doc: ""},
	{Name: "t", Type: []types.AvroPrimitiveType{"null", "string"}, Default: interface{}(nil), Doc: ""},
	{Name: "bl", Type: []types.AvroPrimitiveType{"null", "bytes"}, Default: interface{}(nil), Doc: ""},
	{Name: "dt", Type: []types.AvroPrimitiveType{"null", "string"}, Default: interface{}(nil), Doc: ""},
	{Name: "ts", Type: []types.AvroPrimitiveType{"null", "long"}, Default: interface{}(nil), Doc: ""},
	{Name: "ref_key", Type: []types.AvroPrimitiveType{"long"}, Default: interface{}(nil), Doc: ""},
	{Name: "row_key", Type: []types.AvroPrimitiveType{"bytes"}, Default: interface{}(nil), Doc: ""},
	{Name: "is_deleted", Type: []types.AvroPrimitiveType{"null", "boolean"}, Default: interface{}(nil), Doc: ""}},
	Name:          "storagetapper_test_schema",
	Namespace:     "storagetapper",
	Owner:         "storagetapper",
	SchemaVersion: 1,
	Type:          "record",
	Doc:           "",
	LastModified:  "",
}

func createTestSchemaTable(t *testing.T) {
	test.ExecSQL(DBConn, t, `CREATE TABLE IF NOT EXISTS `+types.MyDBName+`.test_schema (
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

func dropTestSchemaTable(t *testing.T) {
	test.ExecSQL(DBConn, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, TestTbl))
}

func performSchemaRegister(t *testing.T, tbl string) *httptest.ResponseRecorder {
	body := schemaReq{
		Cmd:       "register",
		Service:   TestSvc,
		DB:        TestDB,
		Table:     tbl,
		InputType: types.InputMySQL,
		Output:    TestOutput,
		Type:      TestFormat,
		Dst:       "state",
	}

	n, err := encoder.GetOutputSchemaName(TestSvc, TestDB, tbl, types.InputMySQL, "kafka", 0)
	test.CheckFail(err, t)
	err = state.DeleteSchema(n, TestFormat)
	test.CheckFail(err, t)

	log.Infof("Performing schema registration using request: %+v", body)

	bodyBytes, _ := json.Marshal(body)
	req, err := http.NewRequest("POST", "/schema", bytes.NewReader(bodyBytes))
	test.Assert(t, err == nil, "Error creating request object: %v", err)
	req.Header.Add("Content-Type", "application/json")

	res := httptest.NewRecorder()
	schemaCmd(res, req)

	return res
}

func performSchemaChange(t *testing.T, alterStmt string) *httptest.ResponseRecorder {
	body := schemaReq{
		Cmd:       "change",
		Service:   TestSvc,
		DB:        TestDB,
		Table:     TestTbl,
		Alter:     alterStmt,
		InputType: TestInput,
		Output:    TestOutput,
		Type:      TestFormat,
		Dst:       "state",
	}

	log.Infof("Performing schema change using request: %+v", body)

	bodyBytes, _ := json.Marshal(body)
	req, err := http.NewRequest("PUT", "/schema", bytes.NewReader(bodyBytes))
	test.CheckFail(err, t)
	req.Header.Add("Content-Type", "application/json")

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

	outputSchemaName, err := encoder.GetOutputSchemaName(TestSvc, TestDB, TestTbl, TestInput, TestOutput, 0)
	test.CheckFail(err, t)

	outSchema, err := encoder.GetLatestSchema("production", outputSchemaName, TestFormat)
	test.CheckFail(err, t)

	require.Equal(t, refSchema, outSchema)

	dropTestSchemaTable(t)
}

//TestSchemaHandler_SchemaRegister_Failure tests SchemaRegister for non existent table errors out
func TestSchemaHandler_SchemaRegister_Failure(t *testing.T) {
	res := performSchemaRegister(t, "bad_tbl")
	test.Assert(t, res.Code == http.StatusInternalServerError,
		"Schema register for non-existent table did not return HTTP 500 code")
}

//TestSchemaHandler_SchemaChange_Compatible tests SchemaChange for a compatible schema change
func TestSchemaHandler_SchemaChange(t *testing.T) {
	createTestSchemaTable(t)
	res := performSchemaRegister(t, TestTbl)
	test.Assert(t, http.StatusOK == res.Code,
		"Schema register in schema change test did not return 200 HTTP code")

	res = performSchemaChange(t, `ADD dcml DECIMAL`)
	test.Assert(t, http.StatusOK == res.Code, "Schema change did not return 200 HTTP OK code")

	outputSchemaName, err := encoder.GetOutputSchemaName(TestSvc, TestDB, TestTbl, TestInput, TestOutput, 0)
	test.CheckFail(err, t)

	outSchema, err := encoder.GetLatestSchema("production", outputSchemaName, TestFormat)
	test.CheckFail(err, t)

	require.Equal(t, 13, len(outSchema.Fields))
	require.Equal(t, types.AvroField{Name: "dcml", Type: []types.AvroPrimitiveType{"null", "double"}, Default: interface{}(nil), Doc: ""}, outSchema.Fields[9])
	outSchema.Fields = append(outSchema.Fields[:9], outSchema.Fields[10:13]...)
	require.Equal(t, refSchema, outSchema)

	dropTestSchemaTable(t)
}

//TestSchemaHandler_SchemaChange_Incompatible tests SchemaChange raises error for incompatible schema change
func TestSchemaHandler_SchemaChange_Incompatible(t *testing.T) {
	createTestSchemaTable(t)
	res := performSchemaRegister(t, TestTbl)
	test.Assert(t, http.StatusOK == res.Code,
		"Schema register in schema change test did not return 200 HTTP code")

	_ = performSchemaChange(t, `DROP COLUMN bl`)

	dropTestSchemaTable(t)
}

func TestStartStop(t *testing.T) {
	util.Timeout = 1 * time.Second
	go StartHTTPServer(58989)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	//Wait while server starts
	var err error
	for i := 0; i < 10; i++ {
		_, err = util.HTTPGet(ctx, "http://localhost:58989/health")
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	test.CheckFail(err, t)

	body, err := util.HTTPGet(ctx, "http://localhost:58989/health")
	test.CheckFail(err, t)
	test.Assert(t, string(body) == "OK", "got: %v", string(body))

	Shutdown()

	//Wait while server shuts down
	for i := 0; i < 10; i++ {
		_, err = util.HTTPGet(ctx, "http://localhost:58989/health")
		if err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	test.Assert(t, err != nil, "no server should be listening")
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	var err error
	encoder.GetLatestSchema = state.SchemaGet

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	if err := state.InitManager(shutdown.Context, cfg); err != nil {
		os.Exit(1)
	}
	defer state.Close()

	DBConn, err = db.OpenService(&db.Loc{Service: TestSvc, Name: TestDB}, "", types.InputMySQL)
	if err != nil {
		log.Warnf("MySQL is not available")
		os.Exit(0)
	}
	defer func() {
		err := DBConn.Close()
		if err != nil {
			os.Exit(1)
		}
	}()

	os.Exit(m.Run())
}
