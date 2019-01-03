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
	"encoding/json"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

//SchemaRow represents state schema row
type SchemaRow struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Body string `json:"body"`
}

//InsertSchema inserts output schema into state
func InsertSchema(name string, typ string, schema string) error {
	err := util.ExecSQL(mgr.conn, "INSERT INTO output_schema(name,type,schema_body) VALUES(?, ?, ?)", name, typ, schema)
	if log.E(err) {
		return err
	}
	log.Debugf("Schema added: name:%v type: %v schema:%v", name, typ, schema)
	return nil
}

//UpdateSchema inserts output schema into state
func UpdateSchema(name string, typ string, schema string) error {
	err := util.ExecSQL(mgr.conn, "UPDATE output_schema SET schema_body=? WHERE name=? AND type=?", schema, name, typ)
	if log.E(err) {
		return err
	}
	log.Debugf("Schema updated: name:%v type: %v schema:%v", name, typ, schema)
	return nil
}

//DeleteSchema deletes output schema from the state
func DeleteSchema(name string, typ string) error {
	err := util.ExecSQL(mgr.conn, "DELETE FROM output_schema WHERE name=? AND type=?", name, typ)
	if log.E(err) {
		return err
	}
	log.Debugf("Schema deleted: %+v %+v", name, typ)
	return nil
}

//GetOutputSchema returns output schema from the state
func GetOutputSchema(name string, typ string) string {
	var body string

	err := util.QueryRowSQL(mgr.conn, "SELECT schema_body FROM output_schema WHERE name=? AND type=?", name, typ).Scan(&body)

	if err != nil {
		if err.Error() != "sql: no rows in result set" {
			log.E(err)
		}
		return ""
	}
	log.Debugf("Return output schema from state, name: %v, type: %v body: %v", name, typ, body)
	return body
}

//SchemaGet is builtin schema resolver
func SchemaGet(namespace string, schemaName string, typ string) (*types.AvroSchema, error) {
	var err error
	var a *types.AvroSchema

	s := GetOutputSchema(schemaName, typ)
	if s != "" {
		a = &types.AvroSchema{}
		err = json.Unmarshal([]byte(s), a)
		//TODO: Implement proper schema version handling in state
		if a.SchemaVersion == 0 {
			a.SchemaVersion = 1
		}
	}

	return a, err
}

//ListOutputSchema lists schemas from state, filtered by cond
func ListOutputSchema(cond string, args ...interface{}) ([]SchemaRow, error) {
	log.Debugf("List schemas %v", cond)
	rows, err := util.QuerySQL(mgr.conn, "SELECT name,type,schema_body FROM output_schema "+cond, args...)
	if err != nil {
		return nil, err
	}
	defer func() { log.E(rows.Close()) }()
	res := make([]SchemaRow, 0)
	var r SchemaRow
	for rows.Next() {
		if err := rows.Scan(&r.Name, &r.Type, &r.Body); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}
