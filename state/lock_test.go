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

package state

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/types"
)

func lockAndCheckLockedTaskBasic(t *testing.T, num int) {
	m := make(map[int64]bool)
	for i := 1; i <= num; i++ {
		r, err := GetTableTask("w1", 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, r)
		m[r.ID] = true
	}

	require.Equal(t, num, len(m))

	for i := 1; i <= num; i++ {
		require.Equal(t, true, m[int64(i)])
	}

	r, err := GetTableTask("w1", 1*time.Minute)
	require.NoError(t, err)
	require.Nil(t, r)
}

func getTableLocForTest(i int) *types.TableLoc {
	d := fmt.Sprintf("%v", i)
	return &types.TableLoc{Service: "locksvc" + d, Cluster: "lockclst" + d, DB: "lockdb" + d, Table: "lockt0" + d, Input: "mysql", Output: "kafka", Version: 0}
}

func TestStateLockTaskBasic(t *testing.T) {
	resetState(t)

	for i := 1; i <= 4; i++ {
		insertStateRowForTest(int64(i), getTableLocForTest(i), "", "", t)
	}

	lockAndCheckLockedTaskBasic(t, 4)

	// Set locked_at to be expired
	execSQL(t, "UPDATE state SET locked_at=NOW()-INTERVAL 1 MINUTE")

	lockAndCheckLockedTaskBasic(t, 4)
}

func TestStateLockDeletedTaskBasic(t *testing.T) {
	resetState(t)

	for i := 1; i <= 4; i++ {
		insertStateRowForTest(int64(i), getTableLocForTest(i), "", "", t)
	}

	lockAndCheckLockedTaskBasic(t, 4)

	execSQL(t, "UPDATE state SET locked_at=NOW()-INTERVAL 1 MINUTE")
	execSQL(t, "UPDATE state SET deleted_at=NOW()")

	r, err := GetTableTask("worker_id_1", 5*time.Second)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestStateLockTaskRefresh(t *testing.T) {
	resetState(t)

	for i := 1; i <= 4; i++ {
		insertStateRowForTest(int64(i), getTableLocForTest(i), "", "", t)
	}

	lockAndCheckLockedTaskBasic(t, 4)

	// Set locked_at to be expired
	execSQL(t, "UPDATE state SET locked_at=NOW()-INTERVAL 1 MINUTE")

	//Other workers should fail to refresh
	for i := 1; i <= 4; i++ {
		r := RefreshTableLock(int64(i), "w2")
		require.False(t, r)
	}

	//We should be able to refresh all our locks
	for i := 1; i <= 4; i++ {
		r := RefreshTableLock(int64(i), "w1")
		require.True(t, r)
	}

	//All tasks are locked, expecting empty output
	r, err := GetTableTask("w1", 2*time.Minute)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestStateLockTaskScheduleInterval(t *testing.T) {
	resetState(t)
	tl := &types.TableLoc{Service: "locksvc1", Cluster: "lockclst1", DB: "lockdb1", Table: "lockt01", Input: "notmysql", Output: "kafka", Version: 0}
	insertStateRowForTest(1, tl, "", `{"Schedule":{"Interval":10}}`, t)
	tl.Table = "lockt02"
	insertStateRowForTest(2, tl, "", "", t)

	r, err := GetTableTask("w1", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, r)

	r, err = GetTableTask("w1", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, r)

	// Set locked_at and snapshotted_at to be expired
	execSQL(t, "UPDATE state SET locked_at=NOW()-INTERVAL 10 MINUTE,snapshotted_at=NOW()-INTERVAL 20 MINUTE,need_snapshot=0")

	//First task should be returned because of expired Schedule.Interval
	r, err = GetTableTask("w1", 5*time.Minute)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, int64(1), r.ID)

	//Second tasks locked_at has not been expired, it's not mysql and it doesn't
	//need to be snapshotted
	r, err = GetTableTask("w1", 5*time.Minute)
	require.NoError(t, err)
	require.Nil(t, r)

	execSQL(t, "UPDATE state SET need_snapshot=1 WHERE id=2")

	r, err = GetTableTask("w1", 5*time.Minute)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, int64(2), r.ID)
}

func lockAndCheckLockedClusterBasic(t *testing.T, num int) {
	m := make(map[string]bool)
	for i := 1; i <= num; i++ {
		_, c, _, err := GetClusterTask(types.InputMySQL, "w1", 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, c)
		m[c] = true
	}

	require.Equal(t, num, len(m))

	for i := 1; i <= num; i++ {
		d := fmt.Sprintf("%v", i)
		require.Equal(t, true, m["lockclst"+d])
	}

	_, c, _, err := GetClusterTask(types.InputMySQL, "w1", 1*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "", c)
}

func TestStateLockClusterBasic(t *testing.T) {
	resetState(t)

	for i := 1; i <= 8; i++ {
		d := fmt.Sprintf("%v", i)
		d2 := fmt.Sprintf("%v", (i+1)/2)
		insertStateRowForTest(int64(i), &types.TableLoc{Service: "locksvc" + d2, Cluster: "lockclst" + d2, DB: "lockdb" + d, Table: "lockt0" + d, Input: "mysql", Output: "kafka", Version: 0}, "", "", t)
	}

	lockAndCheckLockedClusterBasic(t, 4)

	// Set locked_at to be expired
	execSQL(t, "UPDATE cluster_state SET locked_at=NOW()-INTERVAL 1 MINUTE")

	lockAndCheckLockedClusterBasic(t, 4)
}

func TestStateLockClusterRefresh(t *testing.T) {
	resetState(t)

	for i := 1; i <= 8; i++ {
		d := fmt.Sprintf("%v", i)
		d2 := fmt.Sprintf("%v", (i+1)/2)
		insertStateRowForTest(int64(i), &types.TableLoc{Service: "locksvc" + d2, Cluster: "lockclst" + d2, DB: "lockdb" + d, Table: "lockt0" + d, Input: "mysql", Output: "kafka", Version: 0}, "", "", t)
	}

	lockAndCheckLockedClusterBasic(t, 4)

	// Set locked_at to be expired
	execSQL(t, "UPDATE cluster_state SET locked_at=NOW()-INTERVAL 1 MINUTE")

	//Other workers should fail to refresh
	for i := 1; i <= 4; i++ {
		d := fmt.Sprintf("%v", i)
		r := RefreshClusterLock("lockclst"+d, "w2")
		require.False(t, r)
	}

	for i := 1; i <= 4; i++ {
		d := fmt.Sprintf("%v", i)
		r := RefreshClusterLock("lockclst"+d, "w1")
		require.True(t, r)
	}

	_, c, _, err := GetClusterTask(types.InputMySQL, "w1", 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "", c)
}

/*
//This is attempt to reproduce deadlock between GetClusterTask and GetTablelTask
func TestStateLockTableAndClusterParallel(t *testing.T) {
	resetState(t)

	for i := 1; i <= 200; i++ {
		d := fmt.Sprintf("%v", i)
		d2 := fmt.Sprintf("%v", (i-1)/4+1)
		insertStateRowForTest(int64(i), &types.TableLoc{Service: "locksvc" + d2, Cluster: "lockclst" + d2, DB: "lockdb" + d, Table: "lockt0" + d, Input: "mysql", Output: "kafka", Version: 0}, "", "", t)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() { defer wg.Done(); lockAndCheckLockedTaskBasic(t, 200) }()
	go func() { defer wg.Done(); lockAndCheckLockedClusterBasic(t, 50) }()

	wg.Wait()
}
*/
