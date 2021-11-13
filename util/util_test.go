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

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
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

func TestSortedGTIDString(t *testing.T) {
	var tests = []struct {
		name   string
		input  string
		output string
	}{
		{"single", "433cceb3-2d4c-11e8-84e7-a0369f692c68:1-198865160", "433cceb3-2d4c-11e8-84e7-a0369f692c68:1-198865160"},
		{"single_with_trivial_interval", "433cceb3-2d4c-11e8-84e7-a0369f692c68:111111", "433cceb3-2d4c-11e8-84e7-a0369f692c68:111111"},
		{"single_with_multiple_intervals", "433cceb3-2d4c-11e8-84e7-a0369f692c68:1-1988:25000-26000:50000", "433cceb3-2d4c-11e8-84e7-a0369f692c68:1-1988:25000-26000:50000"},
		{"multiple_unsorted_with_multiple_intervals", "aa77177b-3644-11e7-ba0f-a0369f64e8b4:1-225440807,433cceb3-2d4c-11e8-84e7-a0369f692c68:1-1988:25000-26000:50000", "433cceb3-2d4c-11e8-84e7-a0369f692c68:1-1988:25000-26000:50000,aa77177b-3644-11e7-ba0f-a0369f64e8b4:1-225440807"},
		{"multiple_sorted_with_multiple_intervals", "433cceb3-2d4c-11e8-84e7-a0369f692c68:1-1988:25000-26000:50000,aa77177b-3644-11e7-ba0f-a0369f64e8b4:1-225440807", "433cceb3-2d4c-11e8-84e7-a0369f692c68:1-1988:25000-26000:50000,aa77177b-3644-11e7-ba0f-a0369f64e8b4:1-225440807"},
		{"big_uuid", `433cceb3-2d4c-11e8-84e7-a0369f692c68:1-198865160,aa77177b-3644-11e7-ba0f-a0369f64e8b4:1-2254408074,0f141c14-5ad9-11e6-a343-2c600ca2324e:7168-7169,092ec193-5502-11e7-8283-a0369f7ac2c0:1-1174534,e98c88e4-2210-11e6-b0fe-a0369f64e8b4:49626-50485,057bc6a0-0b98-11e6-9e75-a0369f692ed4:1-2117447492:2117447494-2426085299,38eb6e2a-d389-11e4-ab94-a0369f3f609c:1-2169718964,2385cdbb-b88c-11e5-80ed-a0369f692c68:1-1127598861,eb0247f0-5962-11e6-99bb-a0369f7a3850:1-1489134046,2d5acba7-4e7e-11e4-8808-a0369f3f609c:1-632707759,6bcea338-55a1-11e7-8691-a0369f7ae138:1-331647999,55ca0ce2-a0ea-11e3-9c29-f01fafe7d66c:1-64792958,b669815f-6617-11e6-ac95-a0369f692c68:1-3984440196,acbd01ee-50e7-11e7-a7c1-a0369f7afdb8:1-75149,57d85f1f-4f45-11e4-8d1a-a0369f3f59c0:1-350988677,07176af8-b60a-11e5-b092-a0369f7a3850:1-802640351,cd980745-55a6-11e7-86b5-a0369f7afdb8:1-321864313,2f12120b-2231-11e6-b1d0-a0369f7a2108:4-1598147208`, "057bc6a0-0b98-11e6-9e75-a0369f692ed4:1-2117447492:2117447494-2426085299,07176af8-b60a-11e5-b092-a0369f7a3850:1-802640351,092ec193-5502-11e7-8283-a0369f7ac2c0:1-1174534,0f141c14-5ad9-11e6-a343-2c600ca2324e:7168-7169,2385cdbb-b88c-11e5-80ed-a0369f692c68:1-1127598861,2d5acba7-4e7e-11e4-8808-a0369f3f609c:1-632707759,2f12120b-2231-11e6-b1d0-a0369f7a2108:4-1598147208,38eb6e2a-d389-11e4-ab94-a0369f3f609c:1-2169718964,433cceb3-2d4c-11e8-84e7-a0369f692c68:1-198865160,55ca0ce2-a0ea-11e3-9c29-f01fafe7d66c:1-64792958,57d85f1f-4f45-11e4-8d1a-a0369f3f59c0:1-350988677,6bcea338-55a1-11e7-8691-a0369f7ae138:1-331647999,aa77177b-3644-11e7-ba0f-a0369f64e8b4:1-2254408074,acbd01ee-50e7-11e7-a7c1-a0369f7afdb8:1-75149,b669815f-6617-11e6-ac95-a0369f692c68:1-3984440196,cd980745-55a6-11e7-86b5-a0369f7afdb8:1-321864313,e98c88e4-2210-11e6-b0fe-a0369f64e8b4:49626-50485,eb0247f0-5962-11e6-99bb-a0369f7a3850:1-1489134046"}}

	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			in, err := gomysql.ParseMysqlGTIDSet(v.input)
			require.NoError(t, err)
			inm, ok := in.(*gomysql.MysqlGTIDSet)
			require.True(t, ok)
			res := SortedGTIDString(inm)
			require.Equal(t, v.output, res)
		})
	}
}
