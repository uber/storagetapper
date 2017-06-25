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
	"database/sql"
	"fmt"
	"log"
	"path"
	"runtime"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber/storagetapper/types"
)

//checkFail is a copy from test package to avoid cyclic dependencies
func checkFail(err error, t *testing.T) {
	if err != nil {
		pc, file, no, _ := runtime.Caller(1)
		details := runtime.FuncForPC(pc)
		log.Fatalf("%v:%v %v: Test failed: %v", path.Base(file), no, path.Base(details.Name()), err.Error())
		t.FailNow()
	}
}

func TestBasic(t *testing.T) {
	conn, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", types.TestMySQLUser, types.TestMySQLPassword, "localhost", 3306, ""))
	if err != nil {
		t.Skip("Can't connect to MySQL")
	}
	err = conn.Ping()
	if err != nil {
		t.Skip("Can't connect to MySQL")
	}

	checkFail(err, t)
	checkFail(ExecSQL(conn, "DROP DATABASE IF EXISTS util_test"), t)
	checkFail(ExecSQL(conn, "CREATE DATABASE IF NOT EXISTS util_test"), t)
	checkFail(ExecSQL(conn, "CREATE TABLE IF NOT EXISTS util_test.util_test(id int not null primary key)"), t)
	checkFail(ExecSQL(conn, "INSERT INTO util_test.util_test VALUES (1), (2), (3)"), t)
	rows, err := QuerySQL(conn, "SELECT * FROM util_test.util_test WHERE id > ?", 0)
	checkFail(err, t)
	defer func() { checkFail(rows.Close(), t) }()
	var i, id int
	for rows.Next() {
		err = rows.Scan(&id)
		checkFail(err, t)
		if i+1 != id {
			t.Fatalf("%v != %v", i, id)
		}
		i++
	}
	checkFail(rows.Err(), t)

	err = QueryRowSQL(conn, "SELECT * FROM util_test.util_test WHERE id > 2").Scan(&id)
	checkFail(err, t)
	err = QueryRowSQL(conn, "SELECT * FROM util_test.util_test WHERE id > 3").Scan(&id)
	if err == nil {
		t.Fatalf("Should fail with no rows error")
	}

	checkFail(ExecSQL(conn, "DROP DATABASE util_test"), t)
}

func TestBytesToString(t *testing.T) {
	s := "test_string"
	if s != BytesToString([]byte(s)) {
		t.Fatalf("Bad conversion to string")
	}
}
