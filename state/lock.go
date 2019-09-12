// Copyright (c) 2018 Uber Technologies, Inc.
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
	"database/sql"
	"time"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/util"
)

func getTableTask(workerID string, lockTimeout time.Duration) (*Row, error) {
	tx, err := mgr.conn.Begin()
	if log.E(err) {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var id int64
	var add string
	if CheckMySQLVersion("8") {
		add = " SKIP LOCKED"
	}
	ir := util.QueryTxRowSQL(tx, `SELECT id FROM state WHERE deleted_at IS NULL AND (locked_at IS NULL OR locked_at < NOW() - INTERVAL ? SECOND) AND (input = 'mysql' OR snapshotted_at IS NULL OR need_snapshot OR (params->"$.Schedule.Interval" > 0 AND snapshotted_at + INTERVAL params->"$.Schedule.Interval" SECOND < NOW())) LIMIT 1 FOR UPDATE`+add, lockTimeout/time.Second)
	err = ir.Scan(&id)
	if err != nil && err == sql.ErrNoRows {
		return nil, nil // no tasks
	}
	if log.E(err) {
		return nil, err
	}
	r, err := util.QueryTxSQL(tx, stateSQL+` WHERE state.id=?`, id)
	if log.E(err) {
		return nil, err
	}
	srow, err := parseRows(r)
	if log.E(err) {
		return nil, err
	}
	if len(srow) == 0 {
		return nil, nil // no tasks
	}
	row := &srow[0]
	tm := time.Now().Round(time.Second)
	if row.TimeForSnapshot(tm) {
		err = advanceSnapshottedAt(tx, row.ID, tm)
		if log.E(err) {
			return nil, err
		}
		row.NeedSnapshot = true
		row.SnapshottedAt = tm
	}
	err = util.ExecTxSQL(tx, `UPDATE state SET locked_at=NOW(), worker_id=? WHERE id=?`, workerID, row.ID)
	if log.E(err) {
		return nil, err
	}
	err = tx.Commit()
	if log.E(err) {
		return nil, err
	}
	return row, nil
}

//GetTableTask picks first available task which needs attention and which lock is expired
//The task is locked by the given workerID and lock time is set to current time
func GetTableTask(workerID string, lockTimeout time.Duration) (r *Row, err error) {
	for i := 0; i < trxRetryCount; i++ {
		r, err = getTableTask(workerID, lockTimeout)
		if err == nil || !isRetriableErr(err) {
			return
		}
	}
	return
}

//RefreshTableLock updates the lock time of the given task and worker
//Returns false if the task is not locked by given worker anymore
func RefreshTableLock(stateID int64, workerID string) bool {
	// `IF(locked_at=NOW(),NOW()+1,NOW())` is to handle the case when we call
	// Refresh(Cluster/Table)Lock lock immediately after locking and locked_at is equal to NOW().
	// We update it with NOW()+1 in order to still get RowsAffected() = 1 back.
	// This helps to avoid complex transaction logic to handle that bound case.
	res, err := mgr.conn.Exec(`UPDATE state SET locked_at=IF(locked_at=NOW(),NOW()+INTERVAL 1 SECOND,NOW()) WHERE id=? AND worker_id=?`, stateID, workerID)
	if log.E(err) {
		return false
	}
	n, err := res.RowsAffected()
	if log.E(err) {
		return false
	}
	if n == 1 {
		return true
	}
	return false
}

func getClusterTask(input string, workerID string, lockTimeout time.Duration) (string, string, string, error) {
	tx, err := mgr.conn.Begin()
	if log.E(err) {
		return "", "", "", err
	}
	defer func() { _ = tx.Rollback() }()
	var service, cluster, db, add string
	if CheckMySQLVersion("8") {
		add = " OF c SKIP LOCKED"
	}
	err = util.QueryTxRowSQL(tx, `SELECT DISTINCT s.service, c.cluster, s.db FROM cluster_state c join state s USING(cluster) WHERE s.deleted_at IS NULL AND s.input=? AND (c.locked_at IS NULL OR c.locked_at < NOW() - INTERVAL ? SECOND) LIMIT 1 FOR UPDATE`+add, input, lockTimeout/time.Second).Scan(&service, &cluster, &db)
	if err != nil && err == sql.ErrNoRows {
		return "", "", "", nil // no clusters
	}
	if log.E(err) {
		return "", "", "", err
	}
	err = util.ExecTxSQL(tx, `UPDATE cluster_state SET locked_at=NOW(), worker_id=? WHERE cluster=?`, workerID, cluster)
	if log.E(err) {
		return "", "", "", err
	}
	err = tx.Commit()
	if log.E(err) {
		return "", "", "", err
	}
	return service, cluster, db, err
}

//GetClusterTask pick first available cluster task and locks it
func GetClusterTask(input string, workerID string, lockTimeout time.Duration) (s string, c string, d string, err error) {
	for i := 0; i < trxRetryCount; i++ {
		s, c, d, err = getClusterTask(input, workerID, lockTimeout)
		if err == nil || !isRetriableErr(err) {
			return
		}
	}
	return
}

//RefreshClusterLock updates the lock time of the given cluster and worker
//Returns false if the task is not locked by given worker anymore
func RefreshClusterLock(cluster string, workerID string) bool {
	res, err := mgr.conn.Exec(`UPDATE cluster_state SET locked_at=IF(locked_at=NOW(),NOW()+INTERVAL 1 SECOND,NOW()) WHERE cluster=? AND worker_id=?`, cluster, workerID)
	if log.E(err) {
		return false
	}
	n, err := res.RowsAffected()
	if log.E(err) {
		return false
	}
	if n == 1 {
		return true
	}
	return false
}
