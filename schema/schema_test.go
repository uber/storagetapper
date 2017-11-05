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
	"strings"
	"testing"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"

	"github.com/linkedin/goavro"

	_ "github.com/go-sql-driver/mysql"
)

var (
	TestSvc = types.MySvcName
	TestDb  = types.MyDbName
	TestTbl = "test_schema"
	DbConn  *sql.DB
	cfg     *config.AppConfig
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

func createTestSchemaTable() error {
	schemaStr := ""
	for col, colType := range schemaTesttbl {
		if colType == "VARCHAR" {
			colType = "VARCHAR(32)"
		}
		schemaStr += (col + " " + colType + ",")
	}
	log.Errorf("create table: %v", schemaStr)
	err := util.ExecSQL(DbConn, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.test_schema (`+
		schemaStr+
		` PRIMARY KEY(bi)
	) ENGINE=INNODB`)
	return err
}

func dropTestSchemaTable() error {
	_, err := DbConn.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, TestTbl))
	return err
}

// Test fetching schema from MySQL
func TestGetMySQLTableSchema(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	err := createTestSchemaTable()
	test.CheckFail(err, t)
	tblSchema, err := Get(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl)
	test.Assert(t, err == nil, fmt.Sprintf("Fetch MySQL schema errored out: %v", err))
	test.Assert(t, tblSchema.DBName == TestDb, "Not OK")
	test.Assert(t, tblSchema.TableName == TestTbl, "Not OK")
	test.Assert(t, len(tblSchema.Columns) == len(schemaTesttbl), "Not OK")
	for _, tblCol := range tblSchema.Columns {
		test.Assert(t, schemaTesttbl[tblCol.Name] != "", "Not OK")
		test.Assert(t, strings.ToUpper(tblCol.DataType) == schemaTesttbl[strings.ToLower(tblCol.Name)], "Not OK")
	}
	test.CheckFail(dropTestSchemaTable(), t)
}

// Test GetMySQLTableSchema on non-existent table returns error
func TestGetMySQLTableSchema_Fail(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	_, err := Get(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl+"_nonexistent")
	test.Assert(t, err != nil, "GetMySQLTableSchema on a non-existent table did not raise error")
}

// Test conversion of MySQL to Avro
func TestConvertToAvroSchema(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	err := createTestSchemaTable()
	test.CheckFail(err, t)
	serAvroSchema, err := ConvertToAvro(&db.Loc{Service: TestSvc, Name: TestDb}, TestTbl, "avro")
	test.CheckFail(err, t)

	avroSchema := &types.AvroSchema{}
	err = json.Unmarshal(serAvroSchema, avroSchema)
	test.CheckFail(err, t)

	//Verify whether a valid Avro schema by creating new codec of goavro library
	_, err = goavro.NewCodec(string(serAvroSchema))
	test.CheckFail(err, t)

	//TODO: Validate by calling heatpipe endpoint as well

	test.CheckFail(dropTestSchemaTable(), t)
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	var err error
	DbConn, err = db.OpenService(&db.Loc{Service: TestSvc, Name: TestDb}, "")
	if err != nil {
		log.Warnf("MySQL not available")
		os.Exit(0)
	}
	defer func() {
		err := DbConn.Close()
		if err != nil {
			os.Exit(1)
		}
	}()
	os.Exit(m.Run())
}
