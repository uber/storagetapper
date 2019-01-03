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
    * MySQL GET_LOCK() based distributed locking
*/
package lock

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

/*Lock is general distributed lock interface*/
type Lock interface {
	// Try to acquire a lock. Returns false if failed.
	TryLock(s string) bool
	// Try to acquire a lock. Returns false if failed.
	// Allows n simultaneous locks to be held
	TryLockShared(s string, n int) bool

	// Try to acquire a lock and wait for specified period of time for the lock
	// to become available. Returns false if failed.
	Lock(s string, waitDuration time.Duration) bool

	// Check if we still have the lock. Try to reacquire if necessary.
	// Returns false in the case of failure
	Refresh() bool

	// Unlock the lock. Returns false if there was failure
	Unlock() bool

	//Close releases resources associated with the lock
	Close() bool
}

//TODO: Improve performance of sharded lock. Now we linearly try to take the
//lock for tickets from 0 to ntickets

type myLock struct {
	conn     *sql.DB
	connID   int64
	name     string
	ci       db.Addr
	n        int
	mu       sync.Mutex
	isLocked bool
}

//Create an instance of Lock
//ntickets - is concurrency allowed for the lock, meaning n processes can hold the lock
//at the same time
func Create(ci *db.Addr) Lock {
	return &myLock{ci: *ci}
}

func (m *myLock) log() log.Logger {
	return log.WithFields(log.Fields{"lock": m.name, "ticket": m.n})
}

func (m *myLock) lockName() string {
	return fmt.Sprintf("%s.%s.%d", types.MySvcName, m.name, m.n)
}

// Lock waits for the duration specified for the lock to be available
// Also TryLock will reuse the connection if it already exists otherwise
// it will create a new one.
func (m *myLock) lock(s string, timeout time.Duration, ntickets int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	var res sql.NullBool
	m.name = s
	for m.n = 0; m.n < ntickets; m.n++ {
		if !m.createConn() {
			return false
		}
		err = m.conn.QueryRow("SELECT GET_LOCK(?,?)", m.lockName(), timeout/time.Second).Scan(&res)
		//true - success, false - timeout, NULL - error
		if err == nil && res.Valid && res.Bool {
			m.isLocked = true
			return true
		}
		if log.EL(m.log(), err) {
			m.closeConn()
		}
	}
	return false
}

// Lock waits for the duration specified for the lock to be available
// Also TryLock will reuse the connection if it already exists otherwise
// it will create a new one.
func (m *myLock) Lock(s string, timeout time.Duration) bool {
	return m.lock(s, timeout, 1)
}

// TryLock tries to take a lock and returns an error if it cannot. If the lock
// is already held then TryLock is noop.
func (m *myLock) TryLock(s string) bool {
	return m.Lock(s, 0)
}

// TryLockShared tries to take a lock and returns an error if it cannot. If the lock
// is already held then TryLock is noop.
func (m *myLock) TryLockShared(s string, n int) bool {
	return m.lock(s, 0, n)
}

// IsLockedByMe checks whether the lock is held by the same connection as
// the current lock object.
func (m *myLock) IsLockedByMe() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lockedBy int64
	if m.conn == nil {
		return false
	}

	err := m.conn.QueryRow("SELECT IFNULL(IS_USED_LOCK(?), 0)", m.lockName()).Scan(&lockedBy)
	log.Debugf("lock: lockedBy: %v, myConnID: %v", lockedBy, m.connID)

	if err != nil || m.connID == 0 || lockedBy != m.connID {
		if err != nil {
			m.log().Errorf("IsLockedByMe: error: " + err.Error())
			m.closeConn()
		} else {
			m.log().Debugf("IsLockedByMe: lockedBy: %v, != myConnID: %v", lockedBy, m.connID)
		}
		return false
	}
	return true
}

// Refresh tries to keep the lock fresh.
func (m *myLock) Refresh() bool {
	if !m.isLocked {
		return true
	}
	if m.IsLockedByMe() {
		return true
	}

	return m.TryLock(m.name)
}

// Unlock releases locks associated with the connection
func (m *myLock) Unlock() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isLocked {
		return false
	}
	m.isLocked = false
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

func (m *myLock) createConn() bool {
	if m.conn != nil && m.conn.Ping() == nil {
		// Reset the connection ID as Ping may have opened a new connection
		m.connID, _ = m.getConnID()
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

	m.connID, _ = m.getConnID()

	return true
}

func (m *myLock) closeConn() bool {
	if m.conn == nil {
		return true
	}

	res := log.E(m.conn.Close())
	m.conn = nil
	m.connID = 0
	m.isLocked = false

	return !res
}

func (m *myLock) getConnID() (int64, error) {
	if m.conn == nil {
		return 0, errors.New("Connection not established yet")
	}

	var myConnID int64
	err := m.conn.QueryRow("SELECT connection_id()").Scan(&myConnID)
	return myConnID, err
}
