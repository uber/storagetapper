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
	"fmt"
	"math/rand"
	"strings"

	"github.com/satori/go.uuid"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

// GetAvroSchemaFromAlterTable is called by the schema change endpoint. It takes in an ALTER TABLE
// statement as an input along with db and table name and creates the new resultant schema and
// returns it in Avro format
func GetAvroSchemaFromAlterTable(dbl *db.Loc, tblName string, typ string,
	alterTblStmt string) ([]byte, error) {

	CreateTempTable := "CREATE TEMPORARY TABLE %s LIKE %s"
	DropTempTable := "DROP TEMPORARY TABLE %s"

	conn, err := db.OpenService(dbl, "")
	defer func() { log.E(conn.Close()) }()
	if err != nil {
		return nil, err
	}
	currTbl := fmt.Sprintf("%s.%s", dbl.Name, tblName)
	tempTbl := fmt.Sprintf("%s.%s", dbl.Name, fmt.Sprintf("tmptbl_%d", rand.Intn(100000)))

	//Create a temp table using the existing table
	_, err = conn.Exec(fmt.Sprintf(CreateTempTable, tempTbl, currTbl))
	if err != nil {
		log.Errorf("Error creating temp table for handling Schema change using ALTER TABLE: %v", err)
		return nil, err
	}

	//Assuming the ALTER TABLE statement will have the table name in the <db>.<table> format,
	// modify the alter table statement to be executed on this temp table
	alterTblStmt = strings.Replace(alterTblStmt, currTbl, tempTbl, 1)

	//Execute the ALTER TABLE statement
	err = util.ExecSQL(conn, alterTblStmt)
	if err != nil {
		log.Errorf("Error executing ALTER TABLE on clone temp table: %v", err)
		return nil, err
	}

	//If ALTER TABLE succeeds, get the Avro schema of the corresponding MySQL schema of temp table
	avroSchema, err := ConvertToAvro(dbl, tblName, typ)
	if log.E(err) {
		return nil, err
	}

	// Drop the temp table. error not handled as it would be eventually dropped anyways.
	err = util.ExecSQL(conn, fmt.Sprintf(DropTempTable, tempTbl))

	return avroSchema, err
}

//MutateTable perform alter schema for the given table, using temporary table
//and return structured and raw schema
func MutateTable(db *sql.DB, svc string, dbName string, tableName string, alter string, ts *types.TableSchema, rawSchema *string) bool {
	//TODO: Wrap below SQL calls in a transaction
	tn := uuid.NewV4().String()
	ftn := "`" + types.MyDbName + "`.`" + tn + "`"
	c := fmt.Sprintf("%s_%s_%s", svc, dbName, tableName)

	if log.E(util.ExecSQL(db, "CREATE TABLE "+ftn+*rawSchema+" COMMENT='"+c+"'")) {
		return false
	}

	if log.E(util.ExecSQL(db, "ALTER TABLE "+ftn+" "+alter)) {
		return false
	}

	ct, err := getRawLow(db, ftn)
	if log.E(err) {
		return false
	}

	tsn, err := GetColumns(db, types.MyDbName, tn, "information_schema.columns", "")
	if log.E(err) {
		return false
	}

	if log.E(util.ExecSQL(db, "DROP TABLE "+ftn)) {
		return false
	}

	ts.DBName = dbName
	ts.TableName = tableName
	ts.Columns = tsn.Columns
	*rawSchema = ct

	return true
}
