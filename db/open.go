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

package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/uber/storagetapper/log"
)

const (
	connInfoTimeout   = 15 * time.Second
	enumeratorTimeout = 30 * time.Second
)

// GetConnInfo is database address resolving function
var GetConnInfo = GetConnInfoByType

// GetEnumerator is database location enumerating function
var GetEnumerator = GetEnumeratorByType

// IsValidConn is database connection validator function that verifies connection is to the correct DB
var IsValidConn = IsValidConnByType

// Log returns logger with Addr fields
func (a *Addr) Log() log.Logger {
	return log.WithFields(log.Fields{"user": a.User, "host": a.Host, "port": a.Port, "db": a.Db})
}

// SQLMode can be substituted by tests
// Difference from default value is ONLY_FULL_GROUP_BY removed, required by
// state SQL joins
var SQLMode = "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

//OpenModeType opens database connection by given address
func OpenModeType(ci *Addr, drv, mode string) (*sql.DB, error) {
	ci.Log().Debugf("Connect string")
	dbc, err := sql.Open(drv, fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?sql_mode='"+mode+"'&interpolateParams=true&parseTime=true&loc=Local&time_zone='SYSTEM'", ci.User, ci.Pwd, ci.Host, ci.Port, ci.Db))

	if log.EL(ci.Log(), err) {
		return nil, err
	}
	// Open doesn't open a connection. Validate DSN data:
	//FIXME: in go1.8 change to: err = dbc.PingContext(shutdown.Context)
	err = dbc.Ping()
	if log.EL(ci.Log(), err) {
		return nil, err
	}
	ci.Log().Infof("Connected")
	return dbc, nil
}

//Open opens database connection by given address
func Open(ci *Addr) (*sql.DB, error) {
	return OpenModeType(ci, "mysql", SQLMode)
}

// OpenService resolves db information for database and connects to that db.
// substDB can be passed to override the db resolved by database locator.
func OpenService(dbl *Loc, substDB string, inputType string) (*sql.DB, error) {
	dbl.LogFields().Infof("Fetching connection info")

	ci, err := GetConnInfo(dbl, Slave, inputType)
	if err != nil {
		return nil, err
	}

	if substDB != "" {
		ci.Db = substDB
	}

	return Open(ci)
}

// GetConnInfoByType returns DB connection info by type of DB node
func GetConnInfoByType(dbl *Loc, connType int, inputType string) (*Addr, error) {
	res, err := NewResolver(inputType)
	if log.E(err) {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), connInfoTimeout)
	defer cancel()

	ci, err := res.GetInfo(ctx, dbl, connType)
	if err != nil {
		err = errors.Wrap(err, "Failed to fetch DB connection info")
		dbl.LogFields().Errorf(err.Error())
		return nil, err
	}

	return ci, nil
}

// GetEnumeratorByType returns a DB location enumerator depending on the input type
func GetEnumeratorByType(svc, cluster, sdb, table, inputType string) (Enumerator, error) {
	res, err := NewResolver(inputType)
	if log.E(err) {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), enumeratorTimeout)
	defer cancel()

	return res.GetEnumerator(ctx, svc, cluster, sdb, table)
}

// IsValidConnByType checks the validity of the connection to make sure connection is to the correct DB
func IsValidConnByType(dbl *Loc, connType int, addr *Addr, inputType string) bool {
	res, err := NewResolver(inputType)
	if err != nil {
		log.Errorf(errors.Wrap(err, "Invalid connection").Error())
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), connInfoTimeout)
	defer cancel()

	return res.IsValidConn(ctx, dbl, connType, addr)
}
