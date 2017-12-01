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
	"net/http"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/state"
)

//SchemaRegReq defines the body format for a schema registration request
type SchemaRegReq struct {
	Svc string
	Db  string
	Tbl string
}

//SchemaChgReq defines the params needed for Schema Change request
type SchemaChgReq struct {
	Svc       string
	Db        string
	Tbl       string
	AlterStmt string
}

//SchemaRegister handles the POST request to get Avro schema from table
//definition for given service,db,table
func SchemaRegister(svc string, sdb string, table string, typ string) error {
	avroSchema, err := schema.ConvertToAvro(&db.Loc{Service: svc, Name: sdb}, table, typ)
	if err != nil {
		return err
	}

	err = state.InsertSchema(encoder.GetOutputSchemaName(svc, sdb, table), typ, string(avroSchema))
	if err != nil {
		return err
	}

	//TODO: Make call to Heatpipe to register this schema

	log.Infof("AvroSchema registered for(%v,%v,%v) = %s", svc, sdb, table, avroSchema)
	return nil
}

//SchemaChange handles the POST request to alter schema
func SchemaChange(svc string, sdb string, table string, typ string, alter string) error {
	avroSchema, err := schema.GetAvroSchemaFromAlterTable(&db.Loc{Service: svc, Name: sdb}, table, typ, alter)
	if err != nil {
		return err
	}

	err = state.UpdateSchema(encoder.GetOutputSchemaName(svc, sdb, table), typ, string(avroSchema))
	if err != nil {
		return err
	}

	//TODO: Make call to Heatpipe to update this schema

	log.Infof("Change schema for(%v,%v,%v) = %s, from %v", svc, sdb, table, avroSchema, alter)
	return nil
}

//schemaReq body of register/deregister schema request
type schemaReq struct {
	Cmd     string
	Name    string
	Schema  string
	Service string
	Db      string
	Table   string
	Alter   string
	Type    string
}

func schemaCmd(w http.ResponseWriter, r *http.Request) {
	//TODO: Implement "list" command
	//TODO: Validate schema

	s := schemaReq{}

	err := json.NewDecoder(r.Body).Decode(&s)
	if err != nil {
		//Will be handle in the next if
	} else if len(s.Type) == 0 {
		err = errors.New("type field cannot be empty")
	} else if len(s.Name) == 0 && s.Cmd != "register" && s.Cmd != "change" {
		err = errors.New("name field cannot be empty")
	} else if s.Cmd == "add" {
		err = state.InsertSchema(s.Name, s.Type, s.Schema)
	} else if s.Cmd == "del" {
		err = state.DeleteSchema(s.Name, s.Type)
	} else if s.Cmd == "register" {
		err = SchemaRegister(s.Service, s.Db, s.Table, s.Type)
	} else if s.Cmd == "change" { //mutate, alter?
		err = SchemaChange(s.Service, s.Db, s.Table, s.Type, s.Alter)
	} else {
		err = errors.New("unknown command (possible commands: add/del/change/register)")
	}

	if err != nil {
		log.Errorf("Schema http: cmd=%v, name=%v, schema=%v, error=%v", s.Cmd, s.Name, s.Schema, err)
		//FIXME: Do not return acutal error
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
