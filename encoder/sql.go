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

package encoder

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

func init() {
	registerPlugin("ansisql", initAnsiSQLEncoder)
	registerPlugin("mysql", initMySQLEncoder)
	registerPlugin("ansisql_idempotent", initAnsiSQLIdempotentEncoder)
	registerPlugin("mysql_idempotent", initMySQLIdempotentEncoder)
}

type sqlEncoder struct {
	encoder

	typ          string
	c            jsonEncoder
	insertPrefix string
	deletePrefix string
	identQuote   string

	idempotentInsert bool
}

func initEncoder(tp, service, db, table, input string, output string, version int, quote string, ii bool, filtering bool) (Encoder, error) {
	return &sqlEncoder{typ: tp, c: jsonEncoder{encoder: encoder{Service: service, Db: db, Table: table, Input: input, Output: output, Version: version, filterEnabled: filtering}}, identQuote: quote, idempotentInsert: ii}, nil
}

func initMySQLEncoder(service, db, table, input string, output string, version int, filtering bool) (Encoder, error) {
	return initEncoder("mysql", service, db, table, input, output, version, "`", false, filtering)
}

func initAnsiSQLEncoder(service, db, table, input string, output string, version int, filtering bool) (Encoder, error) {
	return initEncoder("ansisql", service, db, table, input, output, version, "\"", false, filtering)
}

func initMySQLIdempotentEncoder(service, db, table, input string, output string, version int, filtering bool) (Encoder, error) {
	return initEncoder("mysql_idempotent", service, db, table, input, output, version, "`", true, filtering)
}

func initAnsiSQLIdempotentEncoder(service, db, table, input string, output string, version int, filtering bool) (Encoder, error) {
	return initEncoder("ansisql_idempotent", service, db, table, input, output, version, "\"", true, filtering)
}

//Schema returns table schema
func (e *sqlEncoder) Schema() *types.TableSchema {
	return e.c.inSchema
}

//EncodeSchema encodes current output schema
func (e *sqlEncoder) EncodeSchema(seqno uint64) ([]byte, error) {
	return []byte(e.appendSchema(e.c.inSchema, e.c.filter)), nil
}

func (e *sqlEncoder) quotedIdent(id string) string {
	return e.identQuote + util.EscapeQuotes(id, e.identQuote[0]) + e.identQuote
}

/*UpdateCodec refreshes the schema from state DB */
func (e *sqlEncoder) UpdateCodec() error {
	if err := e.c.UpdateCodec(); err != nil {
		return err
	}

	t := e.quotedIdent(e.c.Table)

	e.insertPrefix = "INSERT INTO " + t + " (" + e.appendFieldNames(e.c.inSchema, e.c.filter, false) + ") VALUES ("
	e.deletePrefix = "DELETE FROM " + t + " WHERE"

	return nil
}

//CommonFormat encodes common format event into byte array
func (e *sqlEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		if err := e.c.UpdateCodec(); err != nil {
			return nil, err
		}
	}
	//FIXME: Assumes that cf is in input schema format, so we need to filter it
	//to conform to output schema
	return e.CommonFormatEncode(cf)
}

//Type returns this encoder type
func (e *sqlEncoder) Type() string {
	return e.typ
}

func (e *sqlEncoder) encodeSQLValue(d interface{}) string {
	switch v := d.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + util.MySQLEscape(false, v) + "'"
	case []byte:
		return "0x" + hex.EncodeToString(v)
	default:
		return fmt.Sprintf("%v", d)
	}
}

func bufWrite(b *bytes.Buffer, s string) {
	_, _ = b.WriteString(s)
}

func (e *sqlEncoder) appendSetFields(b *bytes.Buffer, seqno uint64) {
	for i := 0; i < len(e.c.inSchema.Columns); i++ {
		if e.c.inSchema.Columns[i].Key == "PRI" {
			continue
		}
		cn := e.quotedIdent(e.c.inSchema.Columns[i].Name)
		bufWrite(b, cn)
		bufWrite(b, "= IF(seqno < VALUES(seqno), VALUES(")
		bufWrite(b, cn)
		bufWrite(b, "),")
		bufWrite(b, cn)
		bufWrite(b, "),")
	}
	bufWrite(b, " seqno = IF(seqno < VALUES(seqno), VALUES(seqno), seqno)")
	bufWrite(b, ";")
}

func (e *sqlEncoder) appendPKWhere(b *bytes.Buffer, row []interface{}, cf *types.CommonFormatEvent, seqno uint64, seqnop string) error {
	bufWrite(b, " ")
	bufWrite(b, e.quotedIdent("seqno"))
	bufWrite(b, seqnop)
	bufWrite(b, strconv.FormatUint(seqno, 10))
	var k int
	for i := 0; i < len(e.c.inSchema.Columns); i++ {
		if e.c.inSchema.Columns[i].Key != "PRI" {
			continue
		}
		if len(e.deletePrefix) != b.Len() {
			bufWrite(b, " AND ")
		}
		bufWrite(b, e.quotedIdent(e.c.inSchema.Columns[i].Name))
		_ = b.WriteByte('=')
		if row != nil {
			bufWrite(b, e.encodeSQLValue(row[i]))
		} else if k < len(cf.Key) {
			bufWrite(b, e.encodeSQLValue(cf.Key[k]))
			k++
		} else {
			return fmt.Errorf("broken event. key in the schema has more fields. event key len: %v", len(cf.Key))
		}
	}
	return nil
}

func (e *sqlEncoder) appendFieldNames(schema *types.TableSchema, filter []int, pk bool) string {
	var fieldNames string
	if !pk {
		fieldNames += e.quotedIdent("seqno")
	}
	for i := 0; i < len(schema.Columns); i++ {
		if pk && schema.Columns[i].Key != "PRI" {
			continue
		}
		if e.filter[i] == -1 {
			continue
		}
		if len(fieldNames) != 0 {
			fieldNames += ","
		}
		fieldNames += e.quotedIdent(schema.Columns[i].Name)
	}
	return fieldNames
}

func (e *sqlEncoder) appendSchemaFields(schema *types.TableSchema, filter []int) string {
	fieldNames := e.quotedIdent("seqno") + " BIGINT NOT NULL, "
	for i := 0; i < len(schema.Columns); i++ {
		if e.filter[i] == -1 {
			continue
		}
		fieldNames += e.quotedIdent(schema.Columns[i].Name) + " " + schema.Columns[i].Type
		if schema.Columns[i].IsNullable != "YES" {
			fieldNames += " NOT NULL"
		}
		fieldNames += ", "
	}
	return fieldNames
}

func (e *sqlEncoder) appendSchema(schema *types.TableSchema, filter []int) string {
	return "CREATE TABLE " + e.quotedIdent(e.c.Table) + " (" + e.appendSchemaFields(e.c.inSchema, e.c.filter) + "UNIQUE KEY(" + e.quotedIdent("seqno") + "), PRIMARY KEY (" + e.appendFieldNames(e.c.inSchema, e.c.filter, true) + "));"
}

func (e *sqlEncoder) appendFields(b *bytes.Buffer, row []interface{}, cf *types.CommonFormatEvent, seqno uint64) {
	bufWrite(b, strconv.FormatUint(seqno, 10))
	for i := 0; i < len(e.c.inSchema.Columns); i++ {
		if e.filter[i] == -1 {
			continue
		}
		_ = b.WriteByte(',')
		if row == nil {
			bufWrite(b, e.encodeSQLValue((*cf.Fields)[i].Value))
		} else {
			bufWrite(b, e.encodeSQLValue(row[i]))
		}
	}
}

func (e *sqlEncoder) appendIdempotentInsert(b *bytes.Buffer, row []interface{}, cf *types.CommonFormatEvent, seqno uint64, ts time.Time) error {
	if err := e.appendStmt(b, types.Insert, row, cf, seqno, ts); err != nil {
		return err
	}
	bufWrite(b, " ON DUPLICATE KEY UPDATE ")
	e.appendSetFields(b, seqno)
	return nil
}

func (e *sqlEncoder) appendStmt(b *bytes.Buffer, tp int, row []interface{}, cf *types.CommonFormatEvent, seqno uint64, _ time.Time) error {
	switch tp {
	case types.Insert:
		bufWrite(b, e.insertPrefix)
		e.appendFields(b, row, cf, seqno)
		bufWrite(b, ")")
		if !e.idempotentInsert {
			bufWrite(b, ";")
		}
	case types.Delete:
		bufWrite(b, e.deletePrefix)
		if err := e.appendPKWhere(b, row, cf, seqno, "="); err != nil {
			return err
		}
		_ = b.WriteByte(';')
	case types.Schema:
		bufWrite(b, e.appendSchema(e.c.inSchema, e.c.filter))
	default:
		return fmt.Errorf("unknown event type %v", tp)
	}
	return nil
}

//Row encodes row into CommonFormat
func (e *sqlEncoder) rowLow(tp int, row []interface{}, cf *types.CommonFormatEvent, seqno uint64, ts time.Time) ([]byte, error) {
	var b bytes.Buffer
	var err error
	if tp == types.Insert && e.idempotentInsert {
		err = e.appendIdempotentInsert(&b, row, cf, seqno, ts)
	} else {
		err = e.appendStmt(&b, tp, row, cf, seqno, ts)
	}
	return b.Bytes(), err
}

//Row encodes row into CommonFormat
func (e *sqlEncoder) Row(tp int, row *[]interface{}, seqno uint64, ts time.Time) ([]byte, error) {
	var r []interface{}
	if row != nil {
		r = *row
	}
	return e.rowLow(tp, r, nil, seqno, ts)
}

// CommonFormatEncode encodes CommonFormatEvent into byte array based on the message pack
// encoding system
// By overriding these 2 methods we get full functionality of jsonEncoder
// that implements MessagePack
func (e *sqlEncoder) CommonFormatEncode(c *types.CommonFormatEvent) ([]byte, error) {
	var tp int
	switch c.Type {
	case "insert":
		tp = types.Insert
	case "delete":
		tp = types.Delete
	case "schema":
		//FIXME: Encode schema from c instead of sqlEncoder current schema
		return []byte(e.appendSchema(e.c.inSchema, e.c.filter)), nil
	default:
		return nil, fmt.Errorf("unknown event type %v", c.Type)
	}
	return e.rowLow(tp, nil, c, c.SeqNo, time.Unix(c.Timestamp, 0))
}

// UnwrapEvent splits the event header and payload
// cfEvent is populated with the 'header' information aka the first decoding.
// Data after the header returned in the payload parameter
func (e *sqlEncoder) UnwrapEvent(data []byte, cfEvent *types.CommonFormatEvent) (payload []byte, err error) {
	return nil, fmt.Errorf("SQL encoder doesn't support decoding")
}

//DecodeEvent decodes Sql encoded array into CommonFormatEvent struct
func (e *sqlEncoder) DecodeEvent(b []byte) (*types.CommonFormatEvent, error) {
	return nil, fmt.Errorf("SQL encoder doesn't support decoding")
}
