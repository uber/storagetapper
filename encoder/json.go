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
	"testing"
	"time"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

func init() {
	registerPlugin("json", initJSONEncoder)
}

//jsonEncoder implements Encoder interface into common format
type jsonEncoder struct {
	Service   string
	Db        string
	Table     string
	inSchema  *types.TableSchema
	filter    []int //Contains indexes of fields which are not in output schema
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

func initJSONEncoder(service string, db string, table string) (Encoder, error) {
	return &jsonEncoder{Service: service, Db: db, Table: table}, nil
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
	return e.Row(types.Schema, nil, seqno)
}

//Row encodes row into CommonFormat
func (e *jsonEncoder) Row(tp int, row *[]interface{}, seqno uint64) ([]byte, error) {
	cf := e.convertRowToCommonFormat(tp, row, e.inSchema, seqno, e.filter)
	return e.CommonFormatEncode(cf)
}

//CommonFormat encodes common format event into byte array
func (e *jsonEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.UpdateCodec()
		if err != nil {
			return nil, err
		}
	}
	//FIXME: Assume that cf is in input schema format, so we need to filter it
	//to conform to output schema
	return e.CommonFormatEncode(cf)
}

/*UpdateCodec refreshes the schema from state DB */
func (e *jsonEncoder) UpdateCodec() error {
	schema, err := state.GetSchema(e.Service, e.Db, e.Table)
	if err != nil {
		return err
	}

	e.inSchema = schema

	s := state.GetOutputSchema(GetOutputSchemaName(e.Service, e.Db, e.Table))
	if s != "" {
		c, err := e.schemaDecode([]byte(s))
		if err != nil {
			return err
		}
		if c.Type != "schema" {
			return nil
			//return fmt.Errorf("Broken schema in state for %v,%v,%v", e.Service, e.Db, e.Table)
		}
		e.outSchema = c
	}

	e.prepareFilter()

	return err
}

func jsonDecode(b []byte) (*types.CommonFormatEvent, error) {
	res := &types.CommonFormatEvent{}
	err := json.Unmarshal(b, res)
	return res, err
}

func (e *jsonEncoder) schemaDecode(b []byte) (*types.CommonFormatEvent, error) {
	return jsonDecode(b)
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
			}
		}
	}
}

func fillCommonFormatFields(c *types.CommonFormatEvent, row *[]interface{}, schema *types.TableSchema, filter []int) {
	f := make([]types.CommonFormatField, 0, len(schema.Columns))
	var j int
	for i := 0; i < len(schema.Columns); i++ {
		//Skip fields which are not present in output schema
		if j < len(filter) && filter[j] == i {
			log.Debugf("Skipping field %v", j)
			j++
			continue
		}
		var v interface{}
		if row == nil {
			v = schema.Columns[i].Type
		} else {
			v = (*row)[i]
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

func (e *jsonEncoder) prepareFilter() {
	if e.outSchema == nil {
		return
	}

	nfiltered := len(e.inSchema.Columns)
	if e.outSchema.Fields != nil {
		nfiltered = nfiltered - len(*e.outSchema.Fields)
	}
	if nfiltered == 0 {
		return
	}

	f := *e.outSchema.Fields
	e.filter = make([]int, nfiltered)
	var j int
	for i := 0; i < len(e.inSchema.Columns); i++ {
		if (i-j) >= len(f) || e.inSchema.Columns[i].Name != f[i-j].Name {
			e.filter[j] = i
			j++
		}
	}
	log.Debugf("n=%v filter=(%v)", nfiltered, e.filter)
}

// UnwrapEvent splits the event header and payload
// cfEvent is populated with the 'header' information aka the first decoding.
// Data after the header returned in the payload parameter
func (e *jsonEncoder) UnwrapEvent(data []byte, cfEvent *types.CommonFormatEvent) (payload []byte, err error) {
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	err = dec.Decode(cfEvent)
	if err != nil {
		return
	}
	_, err = buf.ReadFrom(dec.Buffered())
	if err != nil {
		return
	}
	return buf.Bytes(), nil
}

//DecodeEvent decodes JSON encoded array into CommonFormatEvent struct
func (e *jsonEncoder) DecodeEvent(b []byte) (*types.CommonFormatEvent, error) {
	return jsonDecode(b)
}

//ChangeCfFields is used by tests
// This method is used to convert the resulting Field and Key interfaces
// to float64 to match up the field values with floats rather than keeping
// them back as ints. Reason is MsgPack has ability for numbers to be ints
// while json decodes back into float64. DeepEqual fails unless types
// are also equal.
func ChangeCfFields(tp string, cf *types.CommonFormatEvent, ref *types.CommonFormatEvent, t *testing.T) {
	if tp == "msgpack" {
		// Fix to ensure that msgpack does float64
		for i := 0; i < len(cf.Key); i++ {
			switch v := (cf.Key[i]).(type) {
			case int64:
				cf.Key[i] = float64(v)
			}
		}

		if cf.Fields != nil {
			for f := range *cf.Fields {
				switch val := ((*cf.Fields)[f].Value).(type) {
				case int64:
					(*cf.Fields)[f].Value = float64(val)
				}
			}
		}
	} else if tp == "json" {
		for i := 0; i < len(cf.Key); i++ {
			switch (ref.Key[i]).(type) {
			case []byte:
				d, err := base64.StdEncoding.DecodeString(cf.Key[i].(string))
				test.CheckFail(err, t)
				cf.Key[i] = d
			}
		}

		if cf.Fields != nil {
			for f := range *cf.Fields {
				switch (*ref.Fields)[f].Value.(type) {
				case []byte:
					v := &(*cf.Fields)[f]
					var err error
					v.Value, err = base64.StdEncoding.DecodeString(v.Value.(string))
					test.CheckFail(err, t)
				}
			}
		}
	}
}
