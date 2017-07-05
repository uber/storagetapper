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

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/state"
)

type tableCmdReq struct {
	Cmd     string
	Cluster string
	Service string
	Db      string
	Table   string
	Input   string
	Output  string
}

//BufferTopicNameFormat is a copy of config variable, used in DeregisterTable.
//Initialized in main
var BufferTopicNameFormat string

func updateTableRegCnt() {
	cnt, err := state.GetCount()
	if err == nil {
		metrics.GetGlobal().NumTablesRegistered.Set(int64(cnt))
	}
}

func addSQLCond(cond string, args []interface{}, name string, val string) (string, []interface{}) {
	if len(val) != 0 {
		if len(cond) != 0 {
			cond += "AND "
		}
		cond += name + "=? "
		args = append(args, val)
	}
	return cond, args
}

type tableListResponse struct {
	Cluster string
	Service string
	Db      string
	Table   string
	Input   string
	Output  string
}

func handleListCmd(w http.ResponseWriter, t *tableCmdReq) error {
	var err error
	var cond string
	var args = make([]interface{}, 0)

	cond, args = addSQLCond(cond, args, "cluster", t.Cluster)
	cond, args = addSQLCond(cond, args, "service", t.Service)
	cond, args = addSQLCond(cond, args, "db", t.Db)

	var rows state.Type
	if rows, err = state.GetCond(cond, args...); err == nil {
		var resp []byte
		for _, v := range rows {
			var b []byte
			if b, err = json.Marshal(&tableListResponse{Cluster: v.Cluster, Service: v.Service, Db: v.Db, Table: v.Table, Input: v.Input, Output: v.Output}); err != nil {
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

func tableCmd(w http.ResponseWriter, r *http.Request) {
	t := tableCmdReq{}
	err := json.NewDecoder(r.Body).Decode(&t)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if t.Cmd == "list" {
		err = handleListCmd(w, &t)
	} else if len(t.Cluster) == 0 || len(t.Service) == 0 || len(t.Db) == 0 || len(t.Table) == 0 {
		err = errors.New("Invalid command. All fields(cluster,service,db,table) must not be empty")
	} else if t.Cmd == "add" {
		cfg := config.Get()
		if t.Input == "" {
			t.Input = cfg.DefaultInputType
		}
		if t.Output == "" {
			t.Output = cfg.OutputPipeType
		}
		if !state.RegisterTable(&db.Loc{Cluster: t.Cluster, Service: t.Service, Name: t.Db}, t.Table, t.Input, t.Output) {
			err = errors.New("Error registering table")
		} else {
			updateTableRegCnt()
		}
	} else if t.Cmd == "del" {
		if !state.DeregisterTable(t.Service, t.Db, t.Table) || !pipe.DeleteKafkaOffsets(state.GetDB(), config.GetTopicName(BufferTopicNameFormat, t.Service, t.Db, t.Table)) {
			err = errors.New("Error deregistering table")
		} else {
			updateTableRegCnt()
		}
	} else {
		err = errors.New("Unknown command (possible commands add/del/list)")
	}
	if err != nil {
		log.Errorf("Table http: cmd=%v, cluster=%v, db=%v, error=%v", t.Cmd, t.Cluster, t.Db, err)

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
