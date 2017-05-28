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
	"encoding/json"
	"time"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
)

func init() {
	registerPlugin("json", initCommonFormatEncoder)
}

//commonFormatEncoder implements Encoder interface into common format
type commonFormatEncoder struct {
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

func initCommonFormatEncoder(service string, db string, table string) (Encoder, error) {
	return &commonFormatEncoder{Service: service, Db: db, Table: table}, nil
}

//Type returns this encoder type
func (e *commonFormatEncoder) Type() string {
	return "json"
}

//Schema returns table schema
func (e *commonFormatEncoder) Schema() *types.TableSchema {
	return e.inSchema
}

//Row encodes row into CommonFormat
func (e *commonFormatEncoder) Row(tp int, row *[]interface{}, seqno uint64) ([]byte, error) {
	cf := convertRowToCommonFormat(tp, row, e.inSchema, seqno, e.filter)
	return CommonFormatEncode(cf)
}

//CommonFormat encodes common format event into byte array
func (e *commonFormatEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.UpdateCodec()
		if err != nil {
			return nil, err
		}
	}
	//FIXME: Assume that cf is in input schema format, so we need to filter it
	//to conform to output schema
	return CommonFormatEncode(cf)
}

/*UpdateCodec refreshes the schema from state DB */
func (e *commonFormatEncoder) UpdateCodec() error {
	schema, err := state.GetSchema(e.Service, e.Db, e.Table)
	if err != nil {
		return err
	}

	e.inSchema = schema

	s := state.GetOutputSchema(GetOutputSchemaName(e.Service, e.Db, e.Table))
	if s != "" {
		c, err := CommonFormatDecode([]byte(s))
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

func (e *commonFormatEncoder) updateCodecFromDB() error {
	schema, err := schema.Get(&db.Loc{Service: e.Service, Name: e.Db}, e.Table)
	if err == nil {
		e.inSchema = schema
	}
	return err
}

//CommonFormatUpdateCodecFromDB is used in test to get schema from original database
//instread of reading it from state as UpdateCodec does
func CommonFormatUpdateCodecFromDB(enc Encoder) error {
	return enc.(*commonFormatEncoder).updateCodecFromDB()
}

//CommonFormatDecode decodes CommonFormat event from byte array
func CommonFormatDecode(c []byte) (*types.CommonFormatEvent, error) {
	res := &types.CommonFormatEvent{}
	err := json.Unmarshal(c, res)
	return res, err
}

//CommonFormatEncode encodes CommonFormatEvent into byte array
func CommonFormatEncode(c *types.CommonFormatEvent) ([]byte, error) {
	bd, err := json.Marshal(c)
	return bd, err
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

func convertRowToCommonFormat(tp int, row *[]interface{}, schema *types.TableSchema, seqNo uint64, filter []int) *types.CommonFormatEvent {
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

func (e *commonFormatEncoder) prepareFilter() {
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
