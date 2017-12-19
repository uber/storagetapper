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
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"errors"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/util"
)

type tableCmdReq struct {
	Cmd          string
	Cluster      string
	Service      string
	Db           string
	Table        string
	Input        string
	Output       string
	Version      int
	OutputFormat string
	Apply        string
}

func updateTableRegCnt() {
	cnt, err := state.GetCount()
	if err == nil {
		metrics.GetGlobal().NumTablesRegistered.Set(int64(cnt))
	}
}

func addSQLCond(cond string, args []interface{}, name string, op string, val string) (string, []interface{}) {
	if len(val) != 0 && val != "*" {
		if len(cond) != 0 {
			cond += "AND "
		}
		cond += name + " " + op + " " + "? "
		args = append(args, val)
	}
	return cond, args
}

type tableListResponse struct {
	Cluster      string
	Service      string
	Db           string
	Table        string
	Input        string
	Output       string
	Version      int
	OutputFormat string
}

func iterateRows(rows *sql.Rows, t *tableCmdReq) error {
	var d, n string
	for rows.Next() {
		if err := rows.Scan(&d, &n); err != nil {
			return err
		}
		if !state.RegisterTable(&db.Loc{Cluster: t.Cluster, Service: t.Service, Name: d}, n, t.Input, t.Output, t.Version, t.OutputFormat) {
			return fmt.Errorf("Error registering table: %v.%v", d, n)
		}
	}
	return nil
}

func handleAddCmd(w http.ResponseWriter, t *tableCmdReq) error {
	if t.Input == "" {
		t.Input = config.Get().DefaultInputType
	}

	//no wildcards case
	if len(t.Db) != 0 && len(t.Table) != 0 && t.Db != "*" && t.Table != "*" && !strings.ContainsAny(t.Db, "%") && !strings.ContainsAny(t.Table, "%") {
		if !state.RegisterTable(&db.Loc{Cluster: t.Cluster, Service: t.Service, Name: t.Db}, t.Table, t.Input, t.Output, t.Version, t.OutputFormat) {
			return errors.New("Error registering table")
		}
		updateTableRegCnt()
		return nil
	}

	conn, err := db.OpenService(&db.Loc{Service: t.Service, Cluster: t.Cluster, Name: ""}, "")
	if err != nil {
		return err
	}

	query := "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'test')"

	var args = make([]interface{}, 0)
	query, args = addSQLCond(query, args, "table_schema", "LIKE", t.Db)
	query, args = addSQLCond(query, args, "table_name", "LIKE", t.Table)

	var rows *sql.Rows
	rows, err = util.QuerySQL(conn, query, args...)
	if err != nil {
		return err
	}
	defer func() { log.E(rows.Close()) }()

	err = iterateRows(rows, t)

	updateTableRegCnt()

	if err == nil {
		err = rows.Err()
	}

	return err
}

func handleDelListCmd(w http.ResponseWriter, t *tableCmdReq, del bool) error {
	var err error
	var cond string
	var args = make([]interface{}, 0)

	cond, args = addSQLCond(cond, args, "cluster", "=", t.Cluster)
	cond, args = addSQLCond(cond, args, "service", "=", t.Service)
	cond, args = addSQLCond(cond, args, "db", "=", t.Db)
	cond, args = addSQLCond(cond, args, "tableName", "=", t.Table)

	var rows state.Type
	if rows, err = state.GetCond(cond, args...); err == nil {
		var resp []byte
		for _, v := range rows {
			var b []byte
			var topic string
			topic, err = config.Get().GetChangelogTopicName(v.Service, v.Db, v.Table, v.Input, v.Output, v.Version)
			if err != nil {
				break
			}
			if del && strings.ToLower(t.Apply) == "yes" && (!state.DeregisterTable(v.Service, v.Db, v.Table, v.Input, v.Output, v.Version) || !pipe.DeleteKafkaOffsets(state.GetDB(), topic)) {
				err = fmt.Errorf("Error deregistering table: service=%v db=%v table=%v", v.Service, v.Db, v.Table)
				break
			}
			if b, err = json.Marshal(&tableListResponse{Cluster: v.Cluster, Service: v.Service, Db: v.Db, Table: v.Table, Input: v.Input, Output: v.Output, Version: v.Version, OutputFormat: v.OutputFormat}); err != nil {
				break
			}
			resp = append(resp, b...)
			resp = append(resp, '\n')
		}
		if err == nil {
			_, err = w.Write(resp)
		}
	}

	updateTableRegCnt()

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
		err = handleDelListCmd(w, &t, false)
	} else if len(t.Service) == 0 || len(t.Cluster) == 0 || len(t.Db) == 0 || len(t.Table) == 0 || len(t.Output) == 0 || (t.Cmd == "add" && len(t.OutputFormat) == 0) {
		err = errors.New("Invalid command. All fields(service,cluster,db,table,output,outputFormat) must not be empty")
		//	} else if t.Service == "*" && t.Cluster == "*" && t.Db == "*" && t.Table == "*" {
		//		err = errors.New("Invalid command. At least one of the fields(service,cluster,db,table) must not be a wildcard")
	} else if t.Cmd == "del" {
		err = handleDelListCmd(w, &t, true)
	} else if t.Cmd == "add" {
		err = handleAddCmd(w, &t)
	} else {
		err = errors.New("Unknown command (possible commands add/del/list)")
	}
	if err != nil {
		log.Errorf("Table http: cmd=%v, service=%v, cluster=%v, db=%v, table=%v, error=%v", t.Cmd, t.Service, t.Cluster, t.Db, t.Table, err)

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
