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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
)

func init() {
	registerPlugin("json", initJSONEncoder)
}

//jsonEncoder implements Encoder interface into common format
type jsonEncoder struct {
	encoder

	inSchema  *types.TableSchema
	outSchema *types.CommonFormatEvent
}

//GenTimeFunc created to be able to return deterministic timestamp in test
/*TODO: Come up with better day of doing this*/
type GenTimeFunc func() int64

func genTime() int64 {
	return time.Now().UnixNano()
}

//GenTime is polymorphic function to be able to replace common format timestamp
//generator
var GenTime GenTimeFunc = genTime

//ZeroTime in UTC
var ZeroTime = time.Time{}.In(time.UTC)

func initJSONEncoder(service, db, table, input string, output string, version int, filtering bool) (Encoder, error) {
	return &jsonEncoder{encoder: encoder{Service: service, Db: db, Table: table, Input: input, Output: output, Version: version, filterEnabled: filtering}}, nil
}

//Type returns this encoder type
func (e *jsonEncoder) Type() string {
	return "json"
}

//Schema returns table schema
func (e *jsonEncoder) Schema() *types.TableSchema {
	return e.inSchema
}

//EncodeSchema encodes current output schema
func (e *jsonEncoder) EncodeSchema(seqno uint64) ([]byte, error) {
	return e.Row(types.Schema, nil, seqno, time.Time{})
}

//Row encodes row into CommonFormat
func (e *jsonEncoder) Row(tp int, row *[]interface{}, seqno uint64, _ time.Time) ([]byte, error) {
	cf := e.convertRowToCommonFormat(tp, row, e.inSchema, seqno, e.filter)
	return e.CommonFormatEncode(cf)
}

func filterCommonFormat(filter []int, cf *types.CommonFormatEvent) (*types.CommonFormatEvent, error) {
	if cf.Fields == nil || len(*cf.Fields) == 0 {
		return cf, nil
	}

	if len(*cf.Fields) != len(filter) {
		return nil, fmt.Errorf("number of fields in input event(%v) should be equal to number of fields in the input schema(%v)", len(*cf.Fields), len(filter))
	}

	c := &types.CommonFormatEvent{SeqNo: cf.SeqNo, Type: cf.Type, Timestamp: cf.Timestamp, Key: cf.Key, Fields: new([]types.CommonFormatField)}
	for i := 0; i < len(*cf.Fields); i++ {
		if filter[i] == -1 {
			continue
		}
		*c.Fields = append(*c.Fields, (*cf.Fields)[i])
	}

	return c, nil
}

//CommonFormat encodes common format event into byte array
func (e *jsonEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.UpdateCodec()
		if err != nil {
			return nil, err
		}
	}
	var err error
	cf, err = filterCommonFormat(e.filter, cf)
	if err != nil {
		return nil, err
	}
	return e.CommonFormatEncode(cf)
}

/*UpdateCodec refreshes the schema from state DB */
func (e *jsonEncoder) UpdateCodec() error {
	schema, err := state.GetSchema(e.Service, e.Db, e.Table, e.Input, e.Output, e.Version)
	if err != nil {
		return err
	}

	e.inSchema = schema
	n, err := GetOutputSchemaName(e.Service, e.Db, e.Table, e.Input, e.Output, e.Version)
	if err != nil {
		return err
	}
	s := state.GetOutputSchema(n, "json")
	if s != "" {
		c, err := e.schemaDecode([]byte(s))
		if err != nil {
			return err
		}
		if c.Type != "schema" && c.Type != "record" {
			return nil
			//return fmt.Errorf("Broken schema in state for %v,%v,%v", e.Service, e.Db, e.Table)
		}
		e.outSchema = c
	}

	e.filter = prepareFilter(e.inSchema, nil, e.outSchema, e.filterEnabled)

	return err
}

func fixFieldType(f interface{}, dt string) (interface{}, error) {
	var err error
	switch v := f.(type) {
	case float64:
		switch dt {
		case "bigint", "enum", "set":
			f = int64(v)
		case "int", "integer", "tinyint", "smallint", "mediumint", "year":
			f = int32(v)
		case "float":
			f = float32(v)
		}
	case string:
		switch dt {
		case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
			f, err = base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, err
			}
		case "timestamp", "datetime":
			t := ZeroTime
			//MySQL binlog reader library return string intread of time.Time for
			//"zero" time.
			if !strings.HasPrefix(v, "0000-00-00 00:00:00") {
				//t.UnmashalJSON can't be used, because it uses RFC3339 to parse while
				//MarshalJSON uses RFC3339Nano format.
				t, err = time.Parse(time.RFC3339Nano, v)
				if err != nil {
					return nil, err
				}
				if dt == "timestamp" && !t.Equal(time.Time{}) {
					//If the local time is UTC while marshalling, then timezone info is
					//not included in result, in this case when parsing back, timezone is
					//not matched to local timezone. We forcibly set zone to Local if
					//local zone is UTC and parsed time in UTC.
					name, _ := time.Now().Zone() //TODO: cache this
					if t.Location().String() == "UTC" && name == "UTC" {
						t = t.In(time.Local)
					}
				} else {
					t = t.In(time.UTC)
				}
			}
			f = t
		}
	}
	return f, nil
}

func (e *jsonEncoder) fixFieldTypes(res *types.CommonFormatEvent) (err error) {
	k := 0

	//Restore field types according to schema
	if e.inSchema != nil && res.Type != "schema" && res.Type != "record" {
		for i := 0; i < len(e.inSchema.Columns); i++ {
			if e.filter[i] == -1 {
				continue
			}

			if res.Fields != nil {
				f := &(*res.Fields)[e.filter[i]]
				f.Value, err = fixFieldType(f.Value, e.inSchema.Columns[i].DataType)
				if err != nil {
					return err
				}
			}

			if e.inSchema.Columns[i].Key == "PRI" && k < len(res.Key) {
				f := &res.Key[k]
				*f, err = fixFieldType(*f, e.inSchema.Columns[i].DataType)
				if err != nil {
					return err
				}
				k++
			}
		}
	}
	return nil
}

func (e *jsonEncoder) jsonDecode(b []byte) (*types.CommonFormatEvent, error) {
	//FIXME: to properly handle integer need to use decoder and json.UseNumber
	res := &types.CommonFormatEvent{}
	err := json.Unmarshal(b, res)
	if err != nil {
		return nil, err
	}

	return res, e.fixFieldTypes(res)
}

func (e *jsonEncoder) schemaDecode(b []byte) (*types.CommonFormatEvent, error) {
	return e.jsonDecode(b)
}

//CommonFormatEncode encodes CommonFormatEvent into byte array
func (e *jsonEncoder) CommonFormatEncode(c *types.CommonFormatEvent) ([]byte, error) {
	return json.Marshal(c)
}

func fillCommonFormatKey(e *types.CommonFormatEvent, row *[]interface{}, s *types.TableSchema) {
	e.Key = make([]interface{}, 0)
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].Key == "PRI" {
			if row == nil {
				e.Key = append(e.Key, s.Columns[i].Name)
			} else {
				e.Key = append(e.Key, (*row)[i])
				//FIXME: Fix datatypes same as in fields
			}
		}
	}
}

func fillCommonFormatFields(c *types.CommonFormatEvent, row *[]interface{}, schema *types.TableSchema, filter []int) {
	f := make([]types.CommonFormatField, 0, len(schema.Columns))
	for i := 0; i < len(schema.Columns); i++ {
		if filter[i] == -1 {
			continue
		}
		var v interface{}
		if row == nil {
			v = schema.Columns[i].Type
		} else {
			s := schema.Columns[i].DataType
			if schema.Columns[i].Type == types.MySQLBoolean {
				b, ok := (*row)[i].(int8)
				if ok {
					if b != 0 {
						v = true
					} else {
						v = false
					}
				} else {
					v = (*row)[i]
				}
			} else if s == "text" || s == "tinytext" || s == "mediumtext" || s == "longtext" || s == "json" {
				b, ok := (*row)[i].([]byte)
				if ok {
					v = string(b)
				} else {
					v = (*row)[i]
				}
			} else if s == "binary" || s == "varbinary" {
				b, ok := (*row)[i].(string)
				if ok {
					v = []byte(b)
				} else {
					v = (*row)[i]
				}
			} else {
				v = (*row)[i]
			}
		}
		f = append(f, types.CommonFormatField{Name: schema.Columns[i].Name, Value: v})
	}
	c.Fields = &f
}

func (e *jsonEncoder) convertRowToCommonFormat(tp int, row *[]interface{}, schema *types.TableSchema, seqNo uint64, filter []int) *types.CommonFormatEvent {
	var c types.CommonFormatEvent

	//log.Debugf("cf %+v %+v %+v %+v", tp, row, s, seqNo)

	c.SeqNo = seqNo
	c.Timestamp = GenTime()

	switch tp {
	case types.Insert:
		c.Type = "insert"
		fillCommonFormatKey(&c, row, schema)
		fillCommonFormatFields(&c, row, schema, filter)
	case types.Delete:
		c.Type = "delete"
		fillCommonFormatKey(&c, row, schema)
	case types.Schema:
		c.Type = "schema"
		fillCommonFormatKey(&c, nil, schema)
		fillCommonFormatFields(&c, nil, schema, filter)
	default:
		panic("unknown event type")
	}

	return &c
}

// UnwrapEvent splits the event header and payload
// cfEvent is populated with the 'header' information aka the first decoding.
// Data after the header returned in the payload parameter
func (e *jsonEncoder) UnwrapEvent(data []byte, cfEvent *types.CommonFormatEvent) (payload []byte, err error) {
	/* cfEvent prepends the payload, decode it here */
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	err = dec.Decode(cfEvent)
	if err != nil {
		return
	}

	s, ok := cfEvent.Key[0].(string)
	if len(cfEvent.Key) > 1 || !ok || cfEvent.Type == "insert" || cfEvent.Type == "delete" {
		if err = e.fixFieldTypes(cfEvent); err != nil {
			return
		}
	} else if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		cfEvent.Key[0] = string(b)
	} else {
		cfEvent.Key[0] = s
	}

	if e.inSchema != nil && cfEvent.Type == "schema" {
		if err = e.UpdateCodec(); err != nil {
			return
		}
	}
	/* Return everything after cfEvent as a payload */
	/* Append cached in json decoder */
	var buf1 bytes.Buffer
	_, err = buf1.ReadFrom(dec.Buffered())
	if err != nil {
		return
	}
	/* Append remainder of the original buffer not read by json decoder */
	_, err = buf1.ReadFrom(buf)
	if err != nil {
		return
	}
	return buf1.Bytes(), nil
}

//DecodeEvent decodes JSON encoded array into CommonFormatEvent struct
func (e *jsonEncoder) DecodeEvent(b []byte) (*types.CommonFormatEvent, error) {
	return e.jsonDecode(b)
}
