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
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
)

type tableCmdReq struct {
	Cmd           string
	Cluster       string
	Service       string
	DB            string
	Table         string
	Input         string
	Output        string
	OutputFormat  string
	Offset        int64
	Limit         int64
	Filter        string
	Type          string //if set to state list shards, registrations otherwise
	PublishSchema string
	Params        string
	Version       int
	CreateTopic   bool
	AutoVersion   bool
}

type tableListResponse struct {
	Cluster       string    `json:"cluster"`
	Service       string    `json:"service"`
	DB            string    `json:"db"`
	Table         string    `json:"table"`
	Input         string    `json:"input"`
	Output        string    `json:"output"`
	Version       int       `json:"version"`
	OutputFormat  string    `json:"outputFormat"`
	SnapshottedAt time.Time `json:"snapshottedAt"`
	NeedSnapshot  bool      `json:"needSnapshot"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	Params        string    `json:"params,omitempty"`
}

func checkSQLFormat(output, format string) bool {
	return output != "postgres" && output != "mysql" && output != "clickhouse" || ((output == "postgres" || output == "mysql") && strings.HasPrefix(format, "ansisql")) || ((output == "mysql" || output == "clickhouse") && strings.HasPrefix(format, "mysql"))
}

func fillSQLFormat(t *tableCmdReq) {
	if t.OutputFormat == "" {
		if t.Output == "mysql" || t.Output == "clickhouse" {
			t.OutputFormat = "mysql"
		} else if t.Output == "postgres" {
			t.OutputFormat = "ansisql"
		}
	}
}

func handleAddCmd(_ http.ResponseWriter, t *tableCmdReq) error {
	fillSQLFormat(t)

	if !checkSQLFormat(t.Output, t.OutputFormat) {
		return fmt.Errorf("incompatible output format. MySQL, Postgres, ClickHouse outputs only support SQL output format")
	}
	if t.Params != "" {
		v := &config.TableParams{}
		err := json.Unmarshal([]byte(t.Params), v)
		if err != nil {
			return fmt.Errorf("invalid table params value: %v", err)
		}
	}
	if t.AutoVersion {
		var err error
		t.Version, err = state.TableMaxVersion(t.Service, t.Cluster, t.DB, t.Table, t.Input, t.Output)
		if err != nil {
			return err
		}
		t.Version++
	}
	if t.PublishSchema != "" && t.Input == types.InputMySQL {
		err := SchemaRegister(t.Service, t.Cluster, t.DB, t.Table, t.Input, t.Output, t.Version, t.OutputFormat, t.PublishSchema, t.CreateTopic && t.Output == "kafka")
		if err != nil {
			return err
		}
	}
	tn, err := config.Get().GetChangelogTopicName(t.Service, t.DB, t.Table, t.Input, t.Output, t.Version, time.Now())
	if err != nil {
		return err
	}
	//TODO: Implement generic interface for handing offsets in pipe
	if err := pipe.DeleteKafkaOffsets(tn, state.GetDB()); err != nil {
		return err
	}
	if !state.RegisterTable(t.Cluster, t.Service, t.DB, t.Table, t.Input, t.Output, t.Version, t.OutputFormat, t.Params) {
		return errors.New("error registering table")
	}

	return nil
}

func handleDelCmd(_ http.ResponseWriter, t *tableCmdReq) error {
	if !state.DeregisterTable(t.Cluster, t.Service, t.DB, t.Table, t.Input, t.Output, t.Version) {
		return errors.New("error deregistering table")
	}

	return nil
}

func handleListCmd(w http.ResponseWriter, t *tableCmdReq) error {
	var err error
	var cond string
	var args = make([]interface{}, 0)

	cond, args = state.AddSQLCond(cond, args, "AND", "cluster", "=", t.Cluster)
	cond, args = state.AddSQLCond(cond, args, "AND", "service", "=", t.Service)
	cond, args = state.AddSQLCond(cond, args, "AND", "db", "=", t.DB)
	cond, args = state.AddSQLCond(cond, args, "AND", "table_name", "=", t.Table)
	cond, args = state.AddSQLCond(cond, args, "AND", "input", "=", t.Input)
	cond, args = state.AddSQLCond(cond, args, "AND", "output", "=", t.Output)
	if t.Version != 0 {
		cond, args = state.AddSQLCond(cond, args, "AND", "version", "=", fmt.Sprintf("%d", t.Version))
	}

	cond, args = addFilter(cond, args, []string{"cluster", "service", "db", "table_name", "input", "output", "output_format"}, t.Filter)

	if t.Offset != 0 || t.Limit != 0 {
		if t.Limit == 0 && t.Offset != 0 {
			t.Limit = int64((^uint64(0)) >> 1) //MaxInt
		}
		cond += fmt.Sprintf(" LIMIT %v,%v", t.Offset, t.Limit)
	}

	var rows state.Type
	if t.Type == "state" {
		rows, err = state.GetCond(cond, args...)
	} else {
		rows, err = state.GetRegCond(cond, args...)
	}
	if err == nil {
		var resp []byte
		for _, v := range rows {
			if v.Cluster == "" {
				v.Cluster = "*"
			}
			if v.DB == "" {
				v.DB = "*"
			}
			var b []byte
			if b, err = json.Marshal(&tableListResponse{Cluster: v.Cluster, Service: v.Service, DB: v.DB, Table: v.Table, Input: v.Input, Output: v.Output, Version: v.Version, OutputFormat: v.OutputFormat, SnapshottedAt: v.SnapshottedAt, NeedSnapshot: v.NeedSnapshot, Params: v.ParamsRaw, CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt}); err != nil {
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

func parseTableForm(w http.ResponseWriter, r *http.Request) *tableCmdReq {
	var err error
	t := tableCmdReq{}
	ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch {
	case ct == "application/x-www-form-urlencoded", ct == "multipart/form-data", ct == "":
		t.Cmd = r.FormValue("cmd")
		t.Cluster = r.FormValue("cluster")
		t.Service = r.FormValue("service")
		t.DB = r.FormValue("db")
		t.Table = r.FormValue("table")
		t.Input = strings.ToLower(r.FormValue("input"))
		t.Output = strings.ToLower(r.FormValue("output"))
		t.OutputFormat = strings.ToLower(r.FormValue("outputFormat"))
		t.Filter = r.FormValue("filter")
		t.Type = r.FormValue("type")
		t.PublishSchema = strings.ToLower(r.FormValue("publishSchema"))
		s := strings.ToLower(r.FormValue("createTopic"))
		t.CreateTopic = s == "true" || s == "1"
		if r.FormValue("version") != "" {
			i, err := strconv.ParseInt(r.FormValue("version"), 10, 32)
			if log.E(err) {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return nil
			}
			t.Version = int(i)
		}
		if t.Offset, t.Limit, err = parsePagination(r); log.E(err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil
		}
		t.Params = r.FormValue("params")
	case ct == "application/json":
		if err := json.NewDecoder(r.Body).Decode(&t); log.E(err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil
		}
	default:
		code := http.StatusUnsupportedMediaType
		http.Error(w, http.StatusText(code), code)
		return nil
	}

	return &t
}

func outputSQL(s string) bool {
	return s != "mysql" && s != "postgres" && s != "clickhouse"
}

func tableCmd(w http.ResponseWriter, r *http.Request) {
	var err error

	t := parseTableForm(w, r)
	if t == nil {
		return
	}

	if t.Cmd == "list" {
		err = handleListCmd(w, t)
	} else if len(t.Service) == 0 || len(t.Cluster) == 0 || len(t.DB) == 0 || len(t.Table) == 0 {
		err = errors.New("invalid command, parameters(service,cluster,db,table) must not be empty")
	} else if t.Cmd == "add" && len(t.OutputFormat) == 0 && !outputSQL(t.Output) {
		err = errors.New("invalid add command. outputFormat must not be empty")
	} else if (t.Cmd == "del" || t.Cmd == "add") && (len(t.Input) == 0 || len(t.Output) == 0) {
		err = fmt.Errorf("parameters(input,output) must not be empty for '%v' command", t.Cmd)
	} else if t.Cmd == "del" {
		err = handleDelCmd(w, t)
	} else if t.Cmd == "add" {
		err = handleAddCmd(w, t)
	} else {
		err = errors.New("unknown command (possible commands add/del/list)")
	}
	if err != nil {
		log.Errorf("Table http: cmd=%v, service=%v, cluster=%v, db=%v, table=%v, input=%v, output=%v, version=%v, outputFormat=%v, error=%v", t.Cmd, t.Service, t.Cluster, t.DB, t.Table, t.Input, t.Output, t.Version, t.OutputFormat, err)

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
