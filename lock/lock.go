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

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

/*Lock is general distributed lock interface*/
type Lock interface {
	/*Try to acquire a lock. Returns false if failed*/
	Lock(s string) bool
	/*Check if we still have the lock. Try to reacquire if necessary.
	  Returns false in the case of failure*/
	Refresh() bool
	/*Unlock the lock. Returns false if there was failure*/
	Unlock() bool
}

//TODO: Implement sharded lock, which will allow N clients to get a
//lock at the same time instead of 1 currently.

type myLock struct {
	conn     *sql.DB
	name     string
	ci       db.Addr
	n        int
	ntickets int
}

/*type ZooLock struct {
}*/

/*Create an instance of Lock*/
//ntickets - is concurrency allowed for the lock, meaing n processes can hold the lock
//at the same time
func Create(ci *db.Addr, ntickets int) Lock {
	/*	if cfg.LockType == "zoo" {
		return &ZooLock{}; */
	return &myLock{nil, "", *ci, 0, ntickets}
}

func (m *myLock) closeConn() bool {
	if m.conn == nil {
		return true
	}
	if err := m.conn.Close(); err != nil {
		log.Warnf("Error closing connection: %v", err)
		return false
	}
	m.conn = nil
	return true
}

func (m *myLock) Lock(s string) bool {
	var err error
	var res sql.NullBool
	log.Debugf("Acquiring lock: " + s)
	m.conn, err = db.Open(&m.ci)
	if err != nil {
		return false
	}
	m.conn.SetConnMaxLifetime(-1)
	m.conn.SetMaxIdleConns(1)
	m.conn.SetMaxOpenConns(1)
	m.name = s
	for m.n = 0; m.n < m.ntickets; m.n++ {
		err = m.conn.QueryRow(fmt.Sprintf("SELECT GET_LOCK('%s.%s.%d',0)", types.MySvcName, s, m.n)).Scan(&res)
		if err == nil && res.Valid && res.Bool {
			log.Debugf("Acquired lock: %v, ticket: %v", s, m.n)
			return true
		}
		if err != nil {
			log.Debugf("Failed to acquire lock: %v, ticket: %v, Error: %v", s, m.n, err.Error())
		} else {
			log.Debugf("Failed to acquire lock: %v, ticket: %v", s, m.n)
		}
	}
	log.Debugf("Failed to acquire lock: " + s)
	m.closeConn()
	return false
}

func (m *myLock) IsLockedByMe() bool {
	var lockedBy, myConnID int64
	if m.conn == nil {
		return false
	}
	err := m.conn.QueryRow(fmt.Sprintf("SELECT IS_USED_LOCK('%s.%s.%d'), connection_id()", types.MySvcName, m.name, m.n)).Scan(&lockedBy, &myConnID)
	log.Debugf("lock: lockedBy: %v myConnID: %v", lockedBy, myConnID)
	if err != nil || myConnID == 0 || lockedBy != myConnID {
		if err != nil {
			log.Debugf("IsLockedByMe: error: " + err.Error())
		} else {
			log.Debugf("IsLockedByMe: lockedBy: %v, != myConnID: %v", lockedBy, myConnID)
		}
		return false
	}
	return true
}

func (m *myLock) Refresh() bool {
	if m.IsLockedByMe() {
		return true
	}

	m.closeConn()

	return m.Lock(m.name)
}

func (m *myLock) Unlock() bool {
	log.Debugf("lock: Releasing: %s, ticket %d", m.name, m.n)
	return m.closeConn()
}
