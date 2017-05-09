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
	"database/sql"
	"errors"
	"fmt"

	"github.com/uber/storagetapper/log"
)

//GetInfoFunc is a type to implement db address resolver polymorhism
type GetInfoFunc func(*Loc, int) *Addr

//GetInfo is database address resolving function
var GetInfo GetInfoFunc // No default resolver

//Open opens database connection by given address
func Open(ci *Addr) (*sql.DB, error) {
	log.Debugf("Connect string: %v:x@tcp(%v:%v)/%v", ci.User, ci.Host, ci.Port, ci.Db)
	dbc, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", ci.User, ci.Pwd, ci.Host, ci.Port, ci.Db))
	if err != nil {
		log.Errorf("Error connecting to MySQL: %v", err)
		log.Errorf("Connect string: %v:x@tcp(%v:%v)/%v", ci.User, ci.Host, ci.Port, ci.Db)
		return nil, err
	}

	// Open doesn't open a connection. Validate DSN data:
	//FIXME: in go1.8 change to: err = dbc.PingContext(shutdown.Context)
	err = dbc.Ping()
	if err != nil {
		log.Errorf("Error opening connection to MySQL: %v", err)
		log.Errorf("Connect string: %v:x@tcp(%v:%v)/%v", ci.User, ci.Host, ci.Port, ci.Db)
		return nil, err
	}

	log.Infof("Connected to db: %s, username: %s, host: %v", ci.Db, ci.User, ci.Host)

	return dbc, err
}

/*OpenService resolves db information for database
and connects to that db.
substDB can be passed to db to different db on the host resolved by database locator */
func OpenService(dbl *Loc, substDB string) (*sql.DB, error) {
	if ci := GetInfo(dbl, Slave); ci != nil {
		if substDB != "" {
			ci.Db = substDB
		}
		return Open(ci)
	}

	err := errors.New("Failed to get db info")
	dbl.LogFields().Errorf("error: %v", err.Error())
	return nil, err
}
