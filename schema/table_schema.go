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
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

//HasPrimaryKey checks if given table has primary key
func HasPrimaryKey(s *types.TableSchema) bool {
	for _, c := range s.Columns {
		if c.Key == "PRI" {
			return true
		}
	}
	return false
}

func getRawLow(db *sql.DB, fullTable string) (string, error) {
	var ct, unused string
	/*FIXME: Can I pass nil here? */
	if err := db.QueryRow("SHOW CREATE TABLE "+fullTable).Scan(&unused, &ct); err != nil {
		return "", err
	}

	//Cut CONSTRAINTS
	var re = regexp.MustCompile(`(?im)^\s*CONSTRAINT.*\n`)
	ct = re.ReplaceAllString(ct, ``)
	var re1 = regexp.MustCompile(`,\n\)`) // Cut possible remaining comma at the end
	ct = re1.ReplaceAllString(ct, `
)`) //FIXME: Is there a better way to insert \n

	i := strings.Index(ct, "(")
	if i == -1 {
		return "", errors.New("Broken schema: " + ct)
	}

	return ct[i:], nil
}

// GetRaw returns output of SHOW CREATE TABLE after the "CREATE TABLE xyz ("
// So called "raw" schema
func GetRaw(dbl *db.Loc, fullTable string, inputType string) (string, error) {
	conn, err := db.OpenService(dbl, "", inputType)
	if err != nil {
		return "", err
	}
	defer func() { log.E(conn.Close()) }()
	return getRawLow(conn, fullTable)
}

// Get loads structured schema for "table", from master DB, identified by dbl
func Get(dbl *db.Loc, table string, inputType string) (*types.TableSchema, error) {
	conn, err := db.OpenService(dbl, "information_schema", inputType)
	if err != nil {
		return nil, err
	}
	defer func() { log.E(conn.Close()) }()
	return GetColumns(conn, dbl.Name, table)
}

// ParseColumnInfo reads parses column information into table schema
func ParseColumnInfo(rows *sql.Rows, dbName, table string) (*types.TableSchema, error) {
	tableSchema := types.TableSchema{DBName: dbName, TableName: table, Columns: []types.ColumnSchema{}}

	rowsCount := 0
	for rows.Next() {
		cs := types.ColumnSchema{}
		err := rows.Scan(&cs.Name, &cs.OrdinalPosition, &cs.IsNullable,
			&cs.DataType, &cs.CharacterMaximumLength, &cs.NumericPrecision,
			&cs.NumericScale, &cs.Type, &cs.Key)
		if err != nil {
			log.E(errors.Wrap(err, fmt.Sprintf("Error scanning table schema query result for %s.%s",
				dbName, table)))
			return nil, err
		}
		tableSchema.Columns = append(tableSchema.Columns, cs)
		rowsCount++
	}
	if err := rows.Err(); err != nil {
		log.F(err)
	}
	if rowsCount == 0 {
		return &tableSchema, fmt.Errorf("no schema columns for table %s.%s. check grants", dbName, table)
	}

	log.Debugf("Got schema from state for '%v.%v' = '%+v'", dbName, table, tableSchema)
	return &tableSchema, nil
}

// GetColumns reads structured schema from information_schema for table from given connection
func GetColumns(conn *sql.DB, dbName string, tableName string) (*types.TableSchema, error) {
	query := "SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, " +
		"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_TYPE, " +
		"COLUMN_KEY FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
		"ORDER BY ORDINAL_POSITION"

	log.Debugf("%v %v %v", query, dbName, tableName)

	rows, err := conn.Query(query, dbName, tableName)
	if log.E(errors.Wrap(err, fmt.Sprintf("Error fetching table schema for %s.%s", dbName, tableName))) {
		return nil, err
	}
	defer func() { log.E(rows.Close()) }()

	return ParseColumnInfo(rows, dbName, tableName)
}
