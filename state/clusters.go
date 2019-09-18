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
	"database/sql"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/util"
)

//InsertClusterInfo adds connection information for the cluster "name" to state
func InsertClusterInfo(ci *db.Addr) error {
	err := util.ExecSQL(mgr.conn, "INSERT INTO clusters(name,host,port,user,password) VALUES(?, ?, ?, ?, ?)",
		ci.Name, ci.Host, ci.Port, ci.User, ci.Pwd)
	if log.E(err) {
		return err
	}
	log.Debugf("Cluster added: name:%v Host:%v Port:%v User:%v", ci.Name, ci.Host, ci.Port, ci.User)
	return nil
}

//DeleteClusterInfo delete cluster connection info from state database
func DeleteClusterInfo(name string) error {
	err := util.ExecSQL(mgr.conn, "DELETE FROM clusters WHERE name=?", name)
	if log.E(err) {
		return err
	}
	log.Debugf("Cluster deleted: %+v", name)
	return nil
}

//GetClusterInfo lists cluster connection info from state database
func GetClusterInfo(cond string, args ...interface{}) ([]db.Addr, error) {
	log.Debugf("List clusters %v", cond)
	rows, err := util.QuerySQL(mgr.conn, "SELECT name,host,port,user,password FROM clusters "+cond, args...)
	if err != nil {
		return nil, err
	}
	defer func() { log.E(rows.Close()) }()
	res := make([]db.Addr, 0)
	var r db.Addr
	for rows.Next() {
		if err := rows.Scan(&r.Name, &r.Host, &r.Port, &r.User, &r.Pwd); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

//ConnectInfoGet resolves database address using state clusters table
//If no Slaves we need to fall back to master
func ConnectInfoGet(l *db.Loc, connType db.ConnectionType) (*db.Addr, error) {
	var c string
	var a db.Addr

	//FIXME: Add connection type to info in state

	if mgr.conn == nil {
		log.Debugf("State hasn't been initialized yet")
		return nil, nil
	}

	c = l.Cluster
	if l.Cluster == "" {
		err := util.QueryRowSQL(mgr.conn, "SELECT cluster FROM state WHERE service=? AND db=?", l.Service,
			l.Name).Scan(&c)
		if err != nil && err != sql.ErrNoRows {
			log.E(err)
			return nil, err
		}
		if c != "" {
			log.Debugf("Cluster name resolved from state: %v by service=%v, db=%v, connType=%v", c, l.Service,
				l.Name, connType)
		}
	}

	err := util.QueryRowSQL(mgr.conn, "SELECT name,host,port,user,password FROM clusters WHERE name=? AND type=?", c, connType.String()).Scan(&c, &a.Host, &a.Port, &a.User, &a.Pwd)
	//If the connection type is not available then check if the other type is
	if err == sql.ErrNoRows && connType == db.Slave {
		err = util.QueryRowSQL(mgr.conn, "SELECT name,host,port,user,password FROM clusters WHERE name=? AND type=?", c, db.Master.String()).Scan(&c, &a.Host, &a.Port, &a.User, &a.Pwd)
	} else if err == sql.ErrNoRows && connType == db.Master {
		err = util.QueryRowSQL(mgr.conn, "SELECT name,host,port,user,password FROM clusters WHERE name=? AND type=?", c, db.Slave.String()).Scan(&c, &a.Host, &a.Port, &a.User, &a.Pwd)
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if log.E(err) {
		return nil, err
	}

	if l.Cluster != "" && c != l.Cluster {
		log.Errorf("Cluster name mismatch, given: %v, in state %v", l.Cluster, c)
		return nil, nil
	}

	a.Db = l.Name
	a.Name = c

	return &a, nil
}
