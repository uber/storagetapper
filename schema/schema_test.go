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

package schema

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"

	"github.com/linkedin/goavro"

	_ "github.com/go-sql-driver/mysql"
)

var (
	TestSvc = types.MySvcName
	TestDb  = types.MyDbName
	TestTbl = "test_schema"
	conn    *sql.DB
)

var schemaTesttbl = map[string]string{
	"bi": "BIGINT",
	"i":  "INT",
	"vc": "VARCHAR",
	"f":  "FLOAT",
	"d":  "DOUBLE",
	"t":  "TEXT",
	"bl": "BLOB",
	"dt": "DATE",
	"ts": "TIMESTAMP",
}

func createTestSchemaTable(t *testing.T) {
	schemaStr := ""
	for col, colType := range schemaTesttbl {
		if colType == "VARCHAR" {
			colType = "VARCHAR(32)"
		}
		schemaStr += (col + " " + colType + ",")
	}
	test.ExecSQL(conn, t, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.`+TestTbl+` (`+
		schemaStr+
		` PRIMARY KEY(bi)
	) ENGINE=INNODB`)
}

func dropTestSchemaTable(t *testing.T) {
	test.ExecSQL(conn, t, `DROP TABLE IF EXISTS `+TestTbl)
	test.ExecSQL(conn, t, `DROP TABLE IF EXISTS `+TestTbl+"new")
}

func TestHasPrimaryKey(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	createTestSchemaTable(t)

	tblSchema, err := Get(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl)
	test.CheckFail(err, t)

	if !HasPrimaryKey(tblSchema) {
		t.Fatal("Table should have primary key")
	}

	test.ExecSQL(conn, t, `ALTER TABLE `+types.MyDbName+`.`+TestTbl+` DROP PRIMARY KEY`)

	tblSchema, err = Get(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl)
	test.CheckFail(err, t)

	if HasPrimaryKey(tblSchema) {
		t.Fatal("Table should not have primary key")
	}

	dropTestSchemaTable(t)
}

func TestGetRaw(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	createTestSchemaTable(t)

	loc := &db.Loc{Service: TestSvc, Name: TestDb}
	rawSchema, err := GetRaw(loc, TestTbl)
	test.CheckFail(err, t)

	test.ExecSQL(conn, t, `CREATE TABLE `+types.MyDbName+`.`+TestTbl+`new `+rawSchema)

	tblSchemaRef, err := Get(loc, TestTbl)
	test.CheckFail(err, t)

	tblSchema, err := Get(loc, TestTbl+"new")
	test.CheckFail(err, t)

	tblSchema.TableName = TestTbl //make table names equal for comparison

	if !reflect.DeepEqual(tblSchemaRef, tblSchema) {
		t.Fatalf("Wrong table create from raw schema")
	}

	loc.Cluster = "please_return_nil_db_addr"
	_, err = GetRaw(loc, TestTbl)
	test.Assert(t, err != nil, "invalid db loc should fail")

	dropTestSchemaTable(t)
}

// Test fetching schema from MySQL
func TestGetMySQLTableSchema(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	createTestSchemaTable(t)

	tblSchema, err := Get(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl)
	test.Assert(t, err == nil, fmt.Sprintf("Fetch MySQL schema errored out: %v", err))
	test.Assert(t, tblSchema.DBName == TestDb, "Not OK")
	test.Assert(t, tblSchema.TableName == TestTbl, "Not OK")
	test.Assert(t, len(tblSchema.Columns) == len(schemaTesttbl), "Not OK")
	for _, tblCol := range tblSchema.Columns {
		test.Assert(t, schemaTesttbl[tblCol.Name] != "", "Not OK")
		test.Assert(t, strings.ToUpper(tblCol.DataType) == schemaTesttbl[strings.ToLower(tblCol.Name)], "Not OK")
	}
	dropTestSchemaTable(t)
}

// Test GetMySQLTableSchema on non-existent table returns error
func TestGetMySQLTableSchema_Fail(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	_, err := Get(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl+"_nonexistent")
	test.Assert(t, err != nil, "GetMySQLTableSchema on a non-existent table did not raise error")

	_, err = Get(&db.Loc{Cluster: "please_return_nil_db_addr", Service: TestSvc, Name: TestDb}, TestTbl)
	test.Assert(t, err != nil, "GetMySQLTableSchema on a nil-addr did not raise error")
}

// Test conversion of MySQL to Avro
func TestConvertToAvroSchema(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	createTestSchemaTable(t)

	serAvroSchema, err := ConvertToAvro(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl, "avro")
	test.CheckFail(err, t)

	avroSchema := &types.AvroSchema{}
	err = json.Unmarshal(serAvroSchema, avroSchema)
	test.CheckFail(err, t)

	//Verify whether a valid Avro schema by creating new codec of goavro library
	_, err = goavro.NewCodec(string(serAvroSchema))
	test.CheckFail(err, t)

	//TODO: Validate by calling heatpipe endpoint as well

	dropTestSchemaTable(t)
}

func TestMain(m *testing.M) {
	_ = test.LoadConfig()

	var err error
	conn, err = db.OpenService(&db.Loc{Service: TestSvc, Name: TestDb}, "")
	if err != nil {
		log.Warnf("MySQL not available")
		os.Exit(0)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			os.Exit(1)
		}
	}()
	os.Exit(m.Run())
}
