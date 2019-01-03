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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

var dbAddr, _ = db.GetConnInfoForTest(&db.Loc{Name: ""}, db.Master, types.InputMySQL)

func TestLockShared(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	n := 7
	lock := make([]Lock, n)

	for i := 0; i < n; i++ {
		lock[i] = Create(dbAddr)
	}

	for i := 0; i < n; i++ {
		if !lock[i].TryLockShared("test_lock", n) {
			t.Fatalf("Should allow %v concurrent lock", n)
		}
	}

	locknp1 := Create(dbAddr)
	if locknp1.TryLockShared("test_lock", n) {
		t.Fatalf("Should allow only %v concurrent locks", n)
	}

	for i := 0; i < n; i++ {
		if !lock[i].Close() {
			t.Fatalf("Unlock failure %v", i)
		}
	}

	for i := 0; i < n; i++ {
		if !lock[i].TryLockShared("test_lock", n) {
			t.Fatalf("Should allow %v concurrent lock", n)
		}
	}

	for i := 0; i < n; i++ {
		if !lock[i].Close() {
			t.Fatalf("Unlock failure %v", i)
		}
	}
}

func TestLockBasic(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	lock1 := Create(dbAddr)
	lock2 := Create(dbAddr)

	if res := lock1.TryLock("test_lock_1"); !res {
		t.Fail()
	}

	if res := lock2.TryLock("test_lock_1"); res {
		t.Fail()
	}

	if res := lock2.TryLock("test_lock_2"); !res {
		t.Fail()
	}

	if ok := lock1.Unlock(); !ok {
		t.Fail()
	}
	if ok := lock2.Unlock(); !ok {
		t.Fail()
	}

	if res := lock2.TryLock("test_lock_1"); !res {
		t.Fail()
	}

	if !lock2.Refresh() {
		t.Fail()
	}

	//Not locked lock refresh is NOOP, so always successful
	if !lock1.Refresh() {
		t.Fail()
	}

	var mylock2 = lock2.(*myLock)
	if mylock2 == nil {
		t.Fatal("unexpected nil pointer")
	}
	err := mylock2.conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !lock2.Refresh() {
		t.Fail()
	}

	if ok := lock2.Unlock(); !ok {
		t.Fail()
	}

	//The Unlock call is idempotent and return value indicate if lock was actually
	//released
	if ok := lock2.Unlock(); ok {
		t.Fail()
	}
}

func (m *myLock) killConn() error {
	conn, err := db.Open(&m.ci)
	if err != nil {
		return err
	}
	if _, err = conn.Exec(fmt.Sprintf("KILL %v", m.connID)); err != nil {
		return err
	}
	return nil
}

func TestFailRefresh(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	lock1 := Create(dbAddr)
	lock2 := Create(dbAddr)

	if res := lock1.TryLock("test_lock_1"); !res {
		t.Fail()
	}

	var myLock1 = lock1.(*myLock)
	if myLock1 == nil {
		t.Fatal("unexpected nil pointer")
	}
	if !myLock1.IsLockedByMe() {
		t.Fail()
	}
	if err := myLock1.killConn(); err != nil {
		t.Fatal(err)
	}

	if !myLock1.Refresh() {
		t.Fail()
	}

	if err := myLock1.killConn(); err != nil {
		t.Fatal(err)
	}

	if res := lock2.TryLock("test_lock_1"); !res {
		t.Fail()
	}

	if myLock1.Refresh() {
		t.Fail()
	}

	lock2.Close()
}

func TestLockNegative(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	addr := *dbAddr
	addr.Port = 55555

	lock1 := Create(&addr)

	if res := lock1.TryLock("test_lock_1"); res {
		t.Fail()
	}
}

func TestLockWait(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	lock1 := Create(dbAddr)
	require.True(t, lock1.Lock("test_lock_1", 1*time.Second))
	require.True(t, lock1.Refresh())

	var wg sync.WaitGroup
	ch := make(chan bool)

	wg.Add(1)
	go func() {
		defer wg.Done()

		lock2 := Create(dbAddr)
		require.False(t, lock2.Lock("test_lock_1", 1*time.Second))

		<-ch
		require.True(t, lock2.Lock("test_lock_1", 1*time.Second))
		require.True(t, lock2.Unlock())
	}()

	ch <- true
	lock1.Unlock()

	wg.Wait()
}

func TestGetConnID(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	lock1 := Create(dbAddr)
	require.True(t, lock1.Lock("test_lock_1", 1*time.Second))

	var myLock1 = lock1.(*myLock)
	require.NotNil(t, myLock1)

	require.True(t, lock1.Close())
	_, err := myLock1.getConnID()
	require.Error(t, err)
}

func TestFailUnlock(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	lock1 := Create(dbAddr)
	lock2 := Create(dbAddr)

	if res := lock1.TryLock("test_lock_1"); !res {
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

	if res := lock2.TryLock("test_lock_1"); !res {
		t.Fail()
	}

	if lock1.Unlock() {
		t.Fatalf("should fail to unlock")
	}

	var mylock2 = lock2.(*myLock)
	if mylock2 == nil {
		t.Fatal("unexpected nil pointer")
	}

	test.CheckFail(mylock1.conn.Close(), t)

	if lock1.Unlock() {
		t.Fatalf("should fail to unlock")
	}

	if !lock2.Unlock() {
		t.Fatalf("shouldn't fail to unlock")
	}

	if !lock1.Close() {
		t.Fatalf("shouldn't fail to close")
	}

	if !lock2.Close() {
		t.Fatalf("shouldn't fail to close")
	}

	//Unlock after close
	if lock2.Unlock() {
		t.Fatalf("should fail to unlock")
	}

	//Test double close
	if !lock2.Close() {
		t.Fatalf("shouldn't fail to close")
	}

	//Test IsLockedByMe after close
	if mylock1.IsLockedByMe() {
		t.Fail()
	}
}

func TestMain(m *testing.M) {
	test.LoadConfig()
	os.Exit(m.Run())
}
