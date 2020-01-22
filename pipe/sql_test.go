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

package pipe

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

func msgGenSQLLow(typ string, topic string, f1 int, f2 string, f3 float64) string {
	switch typ {
	case postgres:
		return fmt.Sprintf(`INSERT INTO "%v" (f1,f2,f3) VALUES (%v,E'%v',%v)`, topic, f1, f2, f3)
	default:
		return fmt.Sprintf("INSERT INTO `%v` (f1,f2,f3) VALUES (%v,'%v',%v)", topic, f1, f2, f3)
	}
}

func schemaGenSQLLow(Typ string, topic string) string {
	switch Typ {
	case clickhouse:
		return "CREATE TABLE " + topic + " (f1 Int64, f2 String, f3 Float64) ENGINE=Memory"
	case postgres:
		return "CREATE TABLE " + topic + " (f1 INT, f2 VARCHAR, f3 float)"
	default:
		return "CREATE TABLE " + topic + " (f1 INT, f2 VARCHAR(20), f3 float)"
	}
}

func msgGenSQL(topic string, i int) string {
	return msgGenSQLLow(mysql, topic, i, strconv.Itoa(i), float64(i))
}

func msgGenPGSQL(topic string, i int) string {
	return msgGenSQLLow(postgres, topic, i, strconv.Itoa(i), float64(i))
}

func schemaGenMySQL(topic string) string {
	return schemaGenSQLLow(mysql, topic)
}

func schemaGenSQL(topic string) string {
	return schemaGenSQLLow(clickhouse, topic)
}

func schemaGenPGSQL(topic string) string {
	return schemaGenSQLLow(postgres, topic)
}

var DSNnodb = map[string]string{
	postgres:   "host=localhost port=5432 user=postgres sslmode=disable",
	mysql:      types.TestMySQLUser + ":" + types.TestMySQLPassword + "@tcp(localhost:3306)/",
	clickhouse: "http://localhost:8123/default",
}
var DSN = map[string]string{
	//postgres: "postgres:@tcp(localhost:5432)/sqlpipetest?user=postgres&sslmode=disable",
	postgres: "host=localhost port=5432 user=postgres sslmode=disable database=sqlpipetest",
	mysql:    types.TestMySQLUser + ":" + types.TestMySQLPassword + "@tcp(localhost:3306)/sqlpipetest",
	//clickhouse: "tcp://localhost:9500/?database=sqlpipetest&debug=true",
	clickhouse: "http://localhost:8123/sqlpipetest",
}

func getConn(drv string, t *testing.T) *sql.DB {
	conn, err := sql.Open(drv, DSNnodb[drv])
	test.CheckFail(err, t)
	return conn
}

func deleteTestSQLTopics(conn *sql.DB, t *testing.T) {
	test.ExecSQL(conn, t, "DROP DATABASE IF EXISTS sqlpipetest")
	test.ExecSQL(conn, t, "CREATE DATABASE sqlpipetest")
}

func testSQLBasic(drv string, typ int, t *testing.T) {
	log.Debugf("Starting %v %v", drv, typ)

	conn := getConn(drv, t)
	msgGenFn = msgGenSQL
	dsn := DSN[drv]
	if drv == postgres {
		schemaGenFn = schemaGenPGSQL
		msgGenFn = msgGenPGSQL
	} else if drv == mysql {
		schemaGenFn = schemaGenMySQL
	} else {
		schemaGenFn = schemaGenSQL
	}
	p := &sqlPipe{config.PipeConfig{SQL: config.SQLConfig{Type: drv, DSN: dsn}}}

	startCh = make(chan bool)

	deleteTestSQLTopics(conn, t)
	testLoopReversed(p, t, typ)

	//FIXME: For now SQL pipe is only one who changes this.
	//need make it more generic in the future.
	msgGenFn = msgGenDef
	schemaGenFn = schemaGenDef
}

func TestMySQLBasic(t *testing.T) {
	testSQLBasic(mysql, BATCH, t)
	testSQLBasic(mysql, NOKEY, t)
}

func TestPostgreSQLBasic(t *testing.T) {
	testSQLBasic(postgres, BATCH, t)
	testSQLBasic(postgres, NOKEY, t)
}

func TestClickHouseSQLBasic(t *testing.T) {
	testSQLBasic(clickhouse, BATCH, t)
	testSQLBasic(clickhouse, NOKEY, t)
}

func TestSQLType(t *testing.T) {
	for pt, dsn := range DSN {
		p := &sqlPipe{config.PipeConfig{SQL: config.SQLConfig{Type: pt, DSN: dsn}}}
		test.Assert(t, p.Type() == pt, "type should be "+pt)
	}
}

func testSQLEscape(drv string, escStr string, resStr string, t *testing.T) {
	conn := getConn(drv, t)
	deleteTestSQLTopics(conn, t)

	pt := &sqlPipe{config.PipeConfig{SQL: config.SQLConfig{Type: drv, DSN: DSN[drv]}}}

	p, err := pt.NewProducer("ttt")
	require.NoError(t, err)

	err = p.PushSchema("", []byte(schemaGenSQLLow(drv, `ttt`)))
	require.NoError(t, err)
	//	err = p.Push([]byte(`INSERT INTO ttt (f1,f2,f3) VALUES (0, '\0\b\n\r\Z\"\'\\', 0)`))
	err = p.Push([]byte(msgGenSQLLow(drv, `ttt`, 0, escStr, 0)))
	require.NoError(t, err)

	c, err := pt.NewConsumer("ttt")
	require.NoError(t, err)

	n := c.FetchNext()
	require.Equal(t, true, n)
	i, err := c.Pop()
	require.NoError(t, err)

	r, ok := i.([]byte)
	require.Equal(t, true, ok)
	//	require.Equal(t, "INSERT INTO `ttt` (f1,f2,f3) VALUES (0,'\\0\\b\\n\\r\\Z\\\"\\'\\\\',0)", string(r))
	//require.Equal(t, msgGenSQLLow(drv, `ttt`, 0, "\\0\\b\\n\\r\\Z\\\"\\'\\\\", 0), string(r))
	require.Equal(t, msgGenSQLLow(drv, `ttt`, 0, resStr, 0), string(r))
}

func TestMySQLEscape(t *testing.T) {
	escStr := `\0\b\n\r\Z\"\'\\`
	testSQLEscape(mysql, escStr, escStr, t)
}

func TestPostgresEscape(t *testing.T) {
	testSQLEscape(postgres, `\b\n\r\Z\"\'\\`, `\b\n\rZ\"\'\\`, t)
}

func TestClickHouseEscape(t *testing.T) {
	//Driver returns already escaped chars, besides ' and \
	testSQLEscape(clickhouse, `\\0\\b\\n\\rZ\"\'\\`, `\0\b\n\rZ"'\`, t)
}
