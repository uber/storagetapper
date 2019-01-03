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

package config

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"

	"github.com/uber/storagetapper/types"
)

var conn *sql.DB
var dsn string
var mysqlLock sync.Mutex

//copy of util.MySQLError
func mySQLError(err error, code uint16) bool {
	merr, ok := err.(*mysql.MySQLError)
	return ok && merr.Number == code
}

//This is a creative copy of state.connect function
func stateConnect(s string, db string) (*sql.DB, error) {
	if s == "" {
		return nil, nil
	}
	// url.Parse requires scheme in the beginning of the URL, just prepend
	// with random scheme if it wasn't in the config file URL
	if !strings.Contains(s, "://") {
		s = "dsn://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	var host, port string = u.Host, ""
	if strings.Contains(u.Host, ":") {
		host, port, err = net.SplitHostPort(u.Host)
		if err != nil {
			return nil, err
		}
	}
	if u.User.Username() == "" || host == "" {
		return nil, fmt.Errorf("host and username required in state connect URL")
	}
	if port == "" {
		port = "3306"
	}
	uport, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, err
	}
	pwd, _ := u.User.Password()
	conn, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", u.User.Username(), pwd, host, uport, db))
	if err != nil {
		return nil, err
	}
	if err = conn.Ping(); err != nil {
		return nil, err
	}
	return conn, err
}

//this is a copy from util package to avoid import cycle
func execSQL(query string, param ...interface{}) error {
	_, err := conn.Exec(query, param...)
	if mySQLError(err, 1146) {
		if err = mysqlInitLow(); err != nil {
			return err
		}
	}
	for i := 0; mySQLError(err, 1213) && i < 3; i++ {
		_, err = conn.Exec(query, param...)
	}
	return err
}

func queryRowSQL(query string, param ...interface{}) *sql.Row {
	return conn.QueryRow(query, param...)
}

//mysqlInitLow depends on StateConnectURL variable read by previous config loader
//in chain
func mysqlInitLow() (err error) {
	mysqlLock.Lock()
	defer mysqlLock.Unlock()

	if conn == nil {
		conn, err = stateConnect(dsn, "")
		if err != nil {
			return err
		}
	}
	if conn == nil {
		return nil
	}

	err = execSQL(`CREATE DATABASE IF NOT EXISTS ` + types.MyDbName + ` DEFAULT CHARACTER SET latin1`)
	if err != nil || conn == nil {
		return
	}

	return execSQL(`CREATE TABLE IF NOT EXISTS ` + types.MyDbName + `.config(
		name VARCHAR(256),
		created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
		body TEXT,
		PRIMARY KEY (name, created_at)
	)`)
}

func mysqlInit(icfg interface{}) (err error) {
	cfg, ok := icfg.(*AppConfigODS)
	if !ok || conn != nil {
		return
	}
	dsn = cfg.StateConnectURL
	return mysqlInitLow()
}

func mysqlRead(cfg interface{}, name string) ([]byte, error) {
	if err := mysqlInit(cfg); err != nil || conn == nil {
		return nil, err
	}
	var body string
	query := "SELECT body FROM " + types.MyDbName + ".config WHERE name=? ORDER BY created_at DESC LIMIT 1"
	err := queryRowSQL(query, name).Scan(&body)
	if mySQLError(err, 1146) {
		if err = mysqlInitLow(); err != nil {
			return nil, err
		}
		err = queryRowSQL(query, name).Scan(&body)
	}
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		err = conn.Close()
		if err != nil {
			return nil, err
		}
		conn = nil
	}
	return []byte(body), err
}

//TODO: Prevent overwriting newer config in DB, by comparing updatedAt variable and
//created_at field
func mysqlWrite(cfg interface{}, name string, body []byte) error {
	if err := mysqlInit(cfg); err != nil || conn == nil {
		return err
	}
	err := execSQL("INSERT INTO "+types.MyDbName+".config(name,body) VALUES (?,?)", name, body)
	if err != nil {
		err = conn.Close()
		if err != nil {
			return err
		}
		conn = nil
	}
	return err
}
