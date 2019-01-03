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

package util

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"time"

	"github.com/go-sql-driver/mysql"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

var h = &http.Client{}

//Timeout is a http request wait timeout
var Timeout = 15 * time.Second

//HTTPGetWithHeaders helper which returns response as a byte array
func HTTPGetWithHeaders(_ context.Context, url string, headers map[string]string) (body []byte, err error) {
	log.Debugf("GetURL: %v", url)

	var req *http.Request
	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	var resp *http.Response
	resp, err = h.Do(req)
	if err != nil {
		return
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = resp.Body.Close()
	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP Status: %v %v", resp.StatusCode, resp.Status)
	}

	return body, err
}

//HTTPGet helper which returns response as a byte array
func HTTPGet(ctx context.Context, url string) (body []byte, err error) {
	return HTTPGetWithHeaders(ctx, url, nil)
}

//HTTPPostJSONWithHeaders posts given JSON message to given URL
func HTTPPostJSONWithHeaders(url string, body string, headers map[string]string) error {
	log.Debugf("URL: %v, BODY: %v", url, body)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	if err != nil {
		return err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := h.Do(req)
	if err != nil {
		return err
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusAccepted {
		return fmt.Errorf("%+v: %+v", resp.Status, string(b))
	}
	log.Debugf("POST response: %v", string(b))
	err = resp.Body.Close()
	return err
}

//HTTPPostJSON posts given JSON message to given URL
func HTTPPostJSON(url string, body string) error {
	return HTTPPostJSONWithHeaders(url, body, nil)
}

//BytesToString converts zero terminated byte array to string
//Whole array is used when there is no zero in the string
func BytesToString(b []byte) string {
	n := bytes.IndexByte(b, 0)
	if n == -1 {
		n = len(b)
	}
	return string(b[:n])
}

//ExecTxSQL executes SQL query in given transaction
func ExecTxSQL(tx *sql.Tx, query string, param ...interface{}) error {
	log.Debugf("SQLTX: %v %v", query, param)
	_, err := tx.Exec(query, param...)
	return err
}

//ExecSQL executes SQL query
func ExecSQL(d *sql.DB, query string, param ...interface{}) error {
	log.Debugf("SQL: %v %v", query, param)
	_, err := d.Exec(query, param...)
	for i := 0; MySQLError(err, 1213) && err != nil && i < 3; i++ {
		log.Debugf("SQL(retrying after deadlock): %v %v", query, param)
		_, err = d.Exec(query, param...)
	}
	return err
}

//QuerySQL executes SQL query
func QuerySQL(d *sql.DB, query string, param ...interface{}) (*sql.Rows, error) {
	log.Debugf("SQL: %v %v", query, param)
	return d.Query(query, param...)
}

//QueryTxSQL executes SQL query
func QueryTxSQL(tx *sql.Tx, query string, param ...interface{}) (*sql.Rows, error) {
	log.Debugf("SQLTX: %v %v", query, param)
	return tx.Query(query, param...)
}

//QueryRowSQL executes SQL query which return single row
func QueryRowSQL(d *sql.DB, query string, param ...interface{}) *sql.Row {
	log.Debugf("SQLROW: %v %v", query, param)
	return d.QueryRow(query, param...)
}

//QueryTxRowSQL executes SQL query which return single row
func QueryTxRowSQL(tx *sql.Tx, query string, param ...interface{}) *sql.Row {
	log.Debugf("SQLTXROW: %v %v", query, param)
	return tx.QueryRow(query, param...)
}

//CheckTxIsolation return nil if transaction isolation is "level"
func CheckTxIsolation(tx *sql.Tx, level string) error {
	var txLevel string

	err := tx.QueryRow("select @@session.tx_isolation").Scan(&txLevel)
	if err != nil {
		return err
	}

	if txLevel != level {
		err = fmt.Errorf("transaction isolation level must be: %v, got: %v", level, txLevel)
	}

	return err
}

//MySQLError checks if givens error is MySQL error with given code
func MySQLError(err error, code uint16) bool {
	merr, ok := err.(*mysql.MySQLError)
	return ok && merr.Number == code
}

// SortedGTIDString convert GTID set into string, where UUIDs comes in
// lexicographically sorted order.
// The order is the same as in output of select @@global.gtid_executed
func SortedGTIDString(set *gomysql.MysqlGTIDSet) string {
	uuids := make([]string, 0, len(set.Sets))
	for u := range set.Sets {
		uuids = append(uuids, u)
	}

	sort.Strings(uuids)

	var b bytes.Buffer
	for _, v := range uuids {
		if b.Len() > 0 {
			b.WriteString(",")
		}
		b.WriteString(set.Sets[v].String())
	}

	return b.String()
}

// MySQLToDriverType converts mysql type names to sql driver type suitable for
// scan
/*FIXME: Use sql.ColumnType.DatabaseType instead if this function if go1.8 is
* used */
func MySQLToDriverType(mtype string, ftype string) interface{} {
	switch mtype {
	case "int", "integer", "tinyint", "smallint", "mediumint":
		if ftype == types.MySQLBoolean {
			return new(sql.NullBool)
		}
		return new(sql.NullInt64)
	case "timestamp", "datetime":
		return new(mysql.NullTime)
	case "bigint", "bit", "year":
		return new(sql.NullInt64)
	case "float", "double", "decimal", "numeric":
		return new(sql.NullFloat64)
	case "char", "varchar", "json":
		return new(sql.NullString)
	case "blob", "tinyblob", "mediumblob", "longblob":
		return new(sql.RawBytes)
	case "text", "tinytext", "mediumtext", "longtext", "date", "time", "enum", "set":
		return new(sql.NullString)
	default: // "binary", "varbinary" and others
		return new(sql.RawBytes)
	}
}

// PostgresToDriverType converts mysql type names to sql driver type suitable for
// scan
func PostgresToDriverType(psql string) interface{} {
	switch psql {
	case "int2", "int4", "int8":
		return new(sql.NullInt64)
	case "float4", "float8", "numeric":
		return new(sql.NullFloat64)
	case "text", "varchar":
		return new(sql.NullString)
	case "bool":
		return new(sql.NullBool)
	default:
		return new(sql.RawBytes)
	}
}

// ClickHouseToDriverType converts mysql type names to sql driver type suitable for
// scan
func ClickHouseToDriverType(psql string) interface{} {
	switch psql {
	case "int8", "int16", "int32", "int64":
		return new(sql.NullInt64)
	case "uint8", "uint16", "uint32": // FIXME: uint64
		return new(sql.NullInt64)
	case "float32", "float64":
		return new(sql.NullFloat64)
	case "string", "fixedstring":
		return new(sql.NullString)
	default: //FIXME: Date, DateTime, Enum, Array
		return new(sql.RawBytes)
	}
}
