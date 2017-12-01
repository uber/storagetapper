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

package lock

import (
	"fmt"
	"os"
	"testing"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/test"

	_ "github.com/go-sql-driver/mysql"
)

var dbAddr = db.GetInfoForTest(&db.Loc{Name: ""}, 0)

func TestLockTickets(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	n := 7
	lock := make([]Lock, n)

	for i := 0; i < n; i++ {
		lock[i] = Create(dbAddr, n)
	}

	for i := 0; i < n; i++ {
		if !lock[i].Lock("test_lock") {
			t.Fatalf("Should allow %v concurrent lock", n)
		}
	}

	locknp1 := Create(dbAddr, n)
	if locknp1.Lock("test_lock") {
		t.Fatalf("Should allow only %v concurrent locks", n)
	}

	for i := 0; i < n; i++ {
		if !lock[i].Unlock() {
			t.Fatalf("Unlock failure %v", i)
		}
	}

	for i := 0; i < n; i++ {
		if !lock[i].Lock("test_lock") {
			t.Fatalf("Should allow %v concurrent lock", n)
		}
	}

	for i := 0; i < n; i++ {
		if !lock[i].Unlock() {
			t.Fatalf("Unlock failure %v", i)
		}
	}
}

func TestLockBasic(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	lock1 := Create(dbAddr, 1)
	lock2 := Create(dbAddr, 1)

	if res := lock1.Lock("test_lock_1"); !res {
		t.Fail()
	}

	if res := lock2.Lock("test_lock_1"); res {
		t.Fail()
	}

	if res := lock2.Lock("test_lock_2"); !res {
		t.Fail()
	}

	lock1.Unlock()
	lock2.Unlock()

	if res := lock2.Lock("test_lock_1"); !res {
		t.Fail()
	}

	if !lock2.Refresh() {
		t.Fail()
	}

	if lock1.Refresh() {
		t.Fail()
	}

	var mylock2 = lock2.(*myLock)
	err := mylock2.conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !lock2.Refresh() {
		t.Fail()
	}

	lock2.Unlock()
	lock2.Unlock()
}

func (m *myLock) killConn() error {
	var id int64
	if err := m.conn.QueryRow("SELECT connection_id()").Scan(&id); err != nil {
		return err
	}
	conn, err := db.Open(&m.ci)
	if err != nil {
		return err
	}
	if _, err = conn.Exec(fmt.Sprintf("KILL %v", id)); err != nil {
		return err
	}
	return nil
}

func TestFailRefresh(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	lock1 := Create(dbAddr, 1)
	lock2 := Create(dbAddr, 1)

	if res := lock1.Lock("test_lock_1"); !res {
		t.Fail()
	}

	var mylock1 = lock1.(*myLock)
	if mylock1 == nil {
		t.Fatal("unexpected nil pointer")
	}
	if !mylock1.IsLockedByMe() {
		t.Fail()
	}
	if err := mylock1.killConn(); err != nil {
		t.Fatal(err)
	}

	//There should be no error message in the log:
	//IsLockedByMe: error: sql: Scan error on column index 0: converting driver.Value type <nil> ("<nil>") to a int64: invalid syntax
	if !lock1.Refresh() {
		t.Fail()
	}

	if err := mylock1.killConn(); err != nil {
		t.Fatal(err)
	}

	if res := lock2.Lock("test_lock_1"); !res {
		t.Fail()
	}

	if lock1.Refresh() {
		t.Fail()
	}

	lock2.Unlock()
}

func TestLockNegative(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	addr := *dbAddr
	addr.Port = 55555

	lock1 := Create(&addr, 1)

	if res := lock1.Lock("test_lock_1"); res {
		t.Fail()
	}
}

func TestMain(m *testing.M) {
	test.LoadConfig()
	os.Exit(m.Run())
}
