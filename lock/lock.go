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

/*Package lock provides distributed lock interface.
  Implementations include:
    * MySQL's GET_LOCK() based distributed locking
*/
package lock

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

/*Lock is general distributed lock interface*/
type Lock interface {
	/*TryLock tries to acquire a lock. Returns false if failed*/
	TryLock(s string) bool
	//Lock waits for the specified timeout for the lock to be available. Timeout
	//has seconds granularity
	Lock(s string, timeout time.Duration) bool
	/*Check if we still have the lock. Try to reacquire if necessary.
	  Returns false in the case of failure*/
	Refresh() bool
	/*Unlock the lock. Returns false if there was failure*/
	Unlock() bool
	//Close releases resources associated with the lock
	Close() bool
}

type myLock struct {
	conn     *sql.DB
	name     string
	ci       db.Addr
	n        int
	ntickets int
}

/*Create an instance of Lock*/
//ntickets - is concurrency allowed for the lock, meaing n processes can hold the lock
//at the same time
func Create(ci *db.Addr, ntickets int) Lock {
	return &myLock{nil, "", *ci, 0, ntickets}
}

func (m *myLock) log() log.Logger {
	return log.WithFields(log.Fields{"lock": m.name, "ticket": m.n})
}

func (m *myLock) closeConn() bool {
	if m.conn == nil {
		return true
	}
	m.log().Debugf("Closing connection")
	res := log.E(m.conn.Close())
	m.conn = nil
	return !res
}

func (m *myLock) openConn() bool {
	if m.conn != nil {
		return true
	}
	var err error
	m.conn, err = db.Open(&m.ci)
	if log.EL(m.ci.Log(), err) {
		return false
	}
	m.conn.SetConnMaxLifetime(-1)
	m.conn.SetMaxIdleConns(1)
	m.conn.SetMaxOpenConns(1)
	return true
}

func (m *myLock) lockName() string {
	return fmt.Sprintf("%s.%s.%d", types.MySvcName, m.name, m.n)
}

func (m *myLock) Lock(s string, timeout time.Duration) bool {
	var err error
	var res sql.NullBool
	m.name = s
	m.log().Debugf("Acquiring lock")
	for m.n = 0; m.n < m.ntickets; m.n++ {
		if !m.openConn() {
			return false
		}
		err = m.conn.QueryRow("SELECT GET_LOCK(?,?)", m.lockName(), timeout/time.Second).Scan(&res)
		//true - success, false - timeout, NULL - error
		if err == nil && res.Valid && res.Bool {
			m.log().Debugf("Acquired lock")
			return true
		}
		if log.EL(m.log(), err) {
			m.closeConn()
		}
	}
	m.log().Debugf("Failed to acquire lock")
	return false
}

func (m *myLock) TryLock(s string) bool {
	return m.Lock(s, 0)
}

func (m *myLock) IsLockedByMe() bool {
	var lockedBy, myConnID int64
	if m.conn == nil {
		return false
	}
	//Here we assume that connection_id() cannot be 0
	err := m.conn.QueryRow("SELECT IFNULL(IS_USED_LOCK(?),0), connection_id()", m.lockName()).Scan(&lockedBy, &myConnID)
	log.Debugf("lockedBy: %v myConnID: %v", lockedBy, myConnID)
	if err != nil || myConnID == 0 || lockedBy != myConnID {
		if err != nil {
			m.log().Errorf("IsLockedByMe: error: " + err.Error())
			m.closeConn()
		} else {
			m.log().Debugf("IsLockedByMe: lockedBy: %v, != myConnID: %v", lockedBy, myConnID)
		}
		return false
	}
	return true
}

func (m *myLock) Refresh() bool {
	if m.IsLockedByMe() {
		return true
	}
	return m.Lock(m.name, 0)
}

func (m *myLock) Unlock() bool {
	m.log().Debugf("Releasing")
	if m.conn == nil {
		return false
	}
	var res sql.NullBool
	err := m.conn.QueryRow("SELECT RELEASE_LOCK(?)", m.lockName()).Scan(&res)
	if log.EL(m.log(), err) {
		return false
	}
	if !res.Valid {
		m.log().Errorf("Lock did not exists on release")
		return false
	}
	if !res.Bool {
		m.log().Errorf("Lock was not hold by me")
		return false
	}
	return true
}

func (m *myLock) Close() bool {
	return m.closeConn()
}
