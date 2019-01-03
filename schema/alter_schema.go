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

	"github.com/gofrs/uuid"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

// MutateTable perform alter schema for the given table, using temporary table
// and return structured and raw schema
func MutateTable(sdb *sql.DB, svc string, dbName string, tableName string, alter string, ts *types.TableSchema, rawSchema *string) bool {

	//TODO: Wrap below SQL calls in a transaction
	tn, err := uuid.NewV4()
	if log.E(err) {
		return false
	}

	ftn := "`" + types.MyDbName + "`.`" + tn.String() + "`"
	c := fmt.Sprintf("%s_%s_%s", svc, dbName, tableName)

	if log.E(util.ExecSQL(sdb, "CREATE TABLE "+ftn+*rawSchema+" COMMENT='"+c+"'")) {
		return false
	}

	if log.E(util.ExecSQL(sdb, "ALTER TABLE "+ftn+" "+alter)) {
		return false
	}

	ct, err := getRawLow(sdb, ftn)
	if log.E(err) {
		return false
	}

	tsn, err := GetColumns(sdb, types.MyDbName, tn.String())
	if log.E(err) {
		return false
	}

	if log.E(util.ExecSQL(sdb, "DROP TABLE "+ftn)) {
		return false
	}

	ts.DBName = dbName
	ts.TableName = tableName
	ts.Columns = tsn.Columns
	*rawSchema = ct

	return true
}
