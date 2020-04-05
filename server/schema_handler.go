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
	"strings"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
)

func createKafkaTopic(topic string, input string, service string, db string, table string) error {
	return nil
}

// SchemaRegister handles the POST request to get Avro schema from table
// definition for given service,db,table
func SchemaRegister(svc string, cluster string, sdb string, table string, inputType string, output string, version int, formatType string, dst string, createTopic bool) error {
	avroSchema, err := schema.ConvertToAvro(&db.Loc{Cluster: cluster, Service: svc, Name: sdb}, table, inputType, formatType)
	if err != nil {
		return err
	}

	outputSchemaName, err := encoder.GetOutputSchemaName(svc, sdb, table, inputType, output, version)
	if err != nil {
		return err
	}

	if dst == "state" || dst == "all" {
		err = state.InsertSchema(outputSchemaName, formatType, string(avroSchema))
		if err != nil {
			return err
		}
	}

	if createTopic {
		tm := time.Now()
		c, err := config.Get().GetChangelogTopicName(svc, sdb, table, inputType, "kafka", version, tm)
		if err != nil {
			return err
		}
		err = createKafkaTopic(c, inputType, svc, sdb, table)
		if err != nil {
			return err
		}

		o, err := config.Get().GetOutputTopicName(svc, sdb, table, inputType, "kafka", version, tm)
		if err != nil {
			return err
		}
		err = createKafkaTopic(o, inputType, svc, sdb, table)
		if err != nil {
			return err
		}
	}

	log.Infof("AvroSchema registered for(%v,%v, %v,%v,%v,%v,%v) = %s", svc, cluster, sdb, table, inputType, output, version, avroSchema)
	return nil
}

//SchemaChange handles the POST request to alter schema
func SchemaChange(svc string, cluster string, sdb string, table string, inputType string, output string, version int, formatType string, alter string, dst string) error {
	var ts types.TableSchema
	var rs string
	var err error

	if rs, err = schema.GetRaw(&db.Loc{Cluster: cluster, Service: svc, Name: sdb}, table, inputType); log.E(err) {
		return err
	}

	if !schema.MutateTable(state.GetDB(), svc, sdb, table, alter, &ts, &rs) {
		return fmt.Errorf("error applying alter table")
	}

	avroSchema, err := schema.ConvertToAvroFromSchema(&ts, formatType)
	if log.E(err) {
		return err
	}

	outputSchemaName, err := encoder.GetOutputSchemaName(svc, sdb, table, inputType, output, version)
	if err != nil {
		return err
	}

	if dst == "state" || dst == "all" {
		err = state.UpdateSchema(outputSchemaName, formatType, string(avroSchema))
		if err != nil {
			return err
		}
	}

	log.Infof("Change schema for(%v,%v,%v,%v,%v,%v) = %s, from %v", svc, sdb, table, inputType, output, version, avroSchema, alter)
	return nil
}

//schemaReq body of register/deregister schema request
type schemaReq struct {
	Cmd         string
	Name        string
	Body        string
	Service     string
	Cluster     string
	DB          string
	Table       string
	InputType   string
	Output      string
	Version     int
	Alter       string
	Dst         string
	Type        string
	Offset      int64
	Limit       int64
	Filter      string
	CreateTopic bool
}

func addFilter(cond string, args []interface{}, fields []string, filter string) (string, []interface{}) {
	if filter == "" {
		return cond, args
	}
	l := len(cond)
	if l > 0 {
		if !strings.HasSuffix(cond, " AND ") {
			cond += " AND "
		}
	}
	cond += "("
	filter = "%" + filter + "%"
	for _, f := range fields {
		cond, args = state.AddSQLCond(cond, args, "OR", f, "LIKE", filter)
	}
	cond += ")"
	return cond, args
}

func handleSchemaListCmd(w http.ResponseWriter, t *schemaReq) error {
	var err error
	var cond string
	var args = make([]interface{}, 0)

	cond, args = state.AddSQLCond(cond, args, "AND", "name", "=", t.Name)
	cond, args = state.AddSQLCond(cond, args, "AND", "type", "=", t.Type)
	cond, args = state.AddSQLCond(cond, args, "AND", "schema_body", "=", t.Body)
	cond, args = addFilter(cond, args, []string{"name", "type", "schema_body"}, t.Filter)

	if cond != "" {
		cond = " WHERE " + cond
	}

	if t.Offset != 0 || t.Limit != 0 {
		if t.Limit == 0 && t.Offset != 0 {
			t.Limit = int64((^uint64(0)) >> 1) //MaxInt
		}
		cond += fmt.Sprintf(" LIMIT %v,%v", t.Offset, t.Limit)
	}

	var rows []state.SchemaRow
	if rows, err = state.ListOutputSchema(cond, args...); err == nil {
		var resp []byte
		for _, v := range rows {
			var b []byte
			if b, err = json.Marshal(v); err != nil {
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

func parseSchemaForm(w http.ResponseWriter, r *http.Request) *schemaReq {
	var err error
	s := schemaReq{}
	ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch {
	case ct == "application/x-www-form-urlencoded", ct == "multipart/form-data", ct == "":
		s.Cmd = r.FormValue("cmd")
		s.Name = r.FormValue("name")
		s.Body = r.FormValue("body")
		s.Service = r.FormValue("service")
		s.Cluster = r.FormValue("cluster")
		s.DB = r.FormValue("db")
		s.Table = r.FormValue("table")
		s.InputType = strings.ToLower(r.FormValue("inputType"))
		s.Type = r.FormValue("type")
		s.Dst = r.FormValue("dst")
		s.Alter = r.FormValue("alter")
		s.Filter = r.FormValue("filter")
		if s.Offset, s.Limit, err = parsePagination(r); log.E(err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil
		}
	case ct == "application/json":
		if err := json.NewDecoder(r.Body).Decode(&s); log.E(err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil
		}
	default:
		code := http.StatusUnsupportedMediaType
		http.Error(w, http.StatusText(code), code)
		return nil
	}
	return &s
}

func schemaCmd(w http.ResponseWriter, r *http.Request) {
	var err error
	//TODO: Validate schema

	s := parseSchemaForm(w, r)
	if s == nil {
		return
	}

	log.Infof("Received schema command with parameters: %+v", s)

	if s.Cmd == "list" {
		err = handleSchemaListCmd(w, s)
	} else if len(s.Type) == 0 {
		err = errors.New("type field cannot be empty")
	} else if len(s.Name) == 0 && s.Cmd != "register" && s.Cmd != "change" {
		err = errors.New("name field cannot be empty")
	} else if s.Cmd == "add" {
		err = state.InsertSchema(s.Name, s.Type, s.Body)
	} else if s.Cmd == "del" {
		err = state.DeleteSchema(s.Name, s.Type)
	} else if s.InputType == "" || s.Output == "" {
		err = errors.New("InputType and Output cannot be empty")
	} else if s.Cmd == "register" {
		err = SchemaRegister(s.Service, s.Cluster, s.DB, s.Table, s.InputType, s.Output, s.Version, s.Type, s.Dst, s.CreateTopic)
	} else if s.Cmd == "change" { //mutate, alter?
		err = SchemaChange(s.Service, s.Cluster, s.DB, s.Table, s.InputType, s.Output, s.Version, s.Type, s.Alter, s.Dst)
	} else {
		err = errors.New("unknown command (possible commands: add/del/register/change)")
	}

	if err != nil {
		log.Errorf("Schema http: cmd=%v, name=%v, schema=%v, error=%v", s.Cmd, s.Name, s.Body, err)
		//FIXME: Do not return actual error
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
