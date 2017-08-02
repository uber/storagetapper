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

package util

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/uber/storagetapper/log"
)

var h *http.Client

func init() {
	h = &http.Client{
		Timeout: time.Second * 3,
	}
}

//HTTPGet helper which returns response as a byte array
func HTTPGet(url string) (body []byte, err error) {
	log.Debugf("GetURL: %v", url)

	var resp *http.Response
	resp, err = h.Get(url)
	if err != nil {
		return
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = resp.Body.Close()
	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP Status: %v %v", resp.StatusCode, resp.Status)
	}

	return body, err
}

//HTTPPostJSON posts given JSON message to give URL
func HTTPPostJSON(url string, body string) error {
	log.Debugf("URL: %v, BODY: %v", url, body)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(body)))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%+v: %+v", resp.Status, string(b))
	}
	err = resp.Body.Close()
	return err
}

//BytesToString converts zero terminated byte array to string
//Whole array is used when there is no zero in the string
func BytesToString(b []byte) string {
	n := bytes.IndexByte(b, 0)
	if n == -1 {
		n = len(b)
	}
	return string(b[:n])
}

//ExecSQL executes SQL query
func ExecSQL(d *sql.DB, query string, param ...interface{}) error {
	log.Debugf("SQL: %v %v", query, param)
	_, err := d.Exec(query, param...)
	for i := 0; err != nil && i < 3; i++ {
		merr, ok := err.(*mysql.MySQLError)
		if !ok || merr.Number != 1213 {
			return err
		}
		log.Debugf("SQL(retrying after deadlock): %v %v", query, param)
		_, err = d.Exec(query, param...)
	}
	return err
}

//QuerySQL executes SQL query
func QuerySQL(d *sql.DB, query string, param ...interface{}) (*sql.Rows, error) {
	log.Debugf("SQL: %v %v", query, param)
	return d.Query(query, param...)
}

//QueryRowSQL executes SQL query which return single row
func QueryRowSQL(d *sql.DB, query string, param ...interface{}) *sql.Row {
	log.Debugf("SQL: %v %v", query, param)
	return d.QueryRow(query, param...)
}
