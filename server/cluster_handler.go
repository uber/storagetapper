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

package server

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"

	"errors"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
)

//clusterInfoReq body of register/deregister cluster request
//Contains all the information required to db to the cluster
type clusterInfoReq struct {
	Cmd    string
	Name   string
	Host   string
	Port   uint16
	User   string
	Pw     string
	Offset int64
	Limit  int
}

func handleClusterListCmd(w http.ResponseWriter, t *clusterInfoReq) error {
	var err error
	var cond string
	var args = make([]interface{}, 0)

	cond, args = addSQLCond(cond, args, "name", "=", t.Name)
	cond, args = addSQLCond(cond, args, "host", "=", t.Host)
	cond, args = addSQLCond(cond, args, "user", "=", t.User)
	if t.Port != 0 {
		cond, args = addSQLCond(cond, args, "port", "=", fmt.Sprintf("%+v", t.Port))
	}

	if cond != "" {
		cond = " WHERE " + cond
	}

	if t.Offset != 0 || t.Limit != 0 {
		if t.Limit == 0 && t.Offset != 0 {
			t.Limit = int((^uint(0)) >> 1) //MaxInt
		}
		cond += fmt.Sprintf(" LIMIT %v,%v", t.Offset, t.Limit)
	}

	var rows []db.Addr
	if rows, err = state.GetClusterInfo(cond, args...); err == nil {
		var resp []byte
		for _, v := range rows {
			var b []byte
			if b, err = json.Marshal(&db.Addr{Name: v.Name, Host: v.Host, Port: v.Port, User: v.User}); err != nil {
				break
			}
			resp = append(resp, b...)
			resp = append(resp, '\n')
		}

		if err == nil {
			_, err = w.Write(resp)
		}
	}

	return err
}

func clusterInfoCmd(w http.ResponseWriter, r *http.Request) {
	s := clusterInfoReq{}
	ct, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch {
	case ct == "application/x-www-form-urlencoded":
		s.Cmd = r.FormValue("cmd")
		s.Name = r.FormValue("name")
		s.Host = r.FormValue("host")
		s.User = r.FormValue("user")
		s.Pw = r.FormValue("pw")
		if r.FormValue("port") != "" {
			i, err := strconv.ParseInt(r.FormValue("port"), 10, 16)
			if err != nil {
				log.E(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			s.Port = uint16(i)
		}
		if r.FormValue("offset") != "" {
			o, err := strconv.ParseInt(r.FormValue("offset"), 10, 64)
			if err != nil {
				log.E(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			s.Offset = int64(o)
		}
		if r.FormValue("limit") != "" {
			l, err := strconv.ParseInt(r.FormValue("limit"), 10, 64)
			if err != nil {
				log.E(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			s.Limit = int(l)
		}
	case ct == "application/json":
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			log.E(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	default:
		code := http.StatusUnsupportedMediaType
		http.Error(w, http.StatusText(code), code)
		return
	}

	if s.Cmd == "list" {
		err = handleClusterListCmd(w, &s)
	} else if len(s.Name) == 0 {
		err = errors.New("invalid command. Name cannot be empty")
	} else if s.Cmd == "add" {
		if len(s.Host) == 0 || len(s.User) == 0 {
			err = errors.New("Invalid 'add' command. Host and User cannot be empty")
		} else {
			err = state.InsertClusterInfo(s.Name, &db.Addr{Host: s.Host, Port: s.Port, User: s.User, Pwd: s.Pw})
		}
	} else if s.Cmd == "del" {
		err = state.DeleteClusterInfo(s.Name)
	} else {
		err = errors.New("Unknown command (possible commands: add/del)")
	}
	if err != nil {
		log.Errorf("Cluster http: cmd=%v, name=%v, error=%v", s.Cmd, s.Name, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
