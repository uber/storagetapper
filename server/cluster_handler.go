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
	"net/http"

	"errors"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
)

//clusterInfoReq body of register/deregister cluster request
//Contains all the information required to db to the cluster
type clusterInfoReq struct {
	Cmd  string
	Name string
	Host string
	Port uint16
	User string
	Pw   string
}

func clusterInfoCmd(w http.ResponseWriter, r *http.Request) {
	s := clusterInfoReq{}
	err := json.NewDecoder(r.Body).Decode(&s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//TODO: Implement "list" command

	if len(s.Name) == 0 {
		err = errors.New("Invalid command. Name cannot be empty")
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
