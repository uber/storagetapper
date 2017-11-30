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
	"fmt"
	"strings"

	"github.com/linkedin/goavro"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
)

const numMetadataFields = 3

func init() {
	registerPlugin("avro", initAvroEncoder)
}

//avroEncoder implements Encoder interface for Avro format
type avroEncoder struct {
	Service   string
	Db        string
	Table     string
	codec     goavro.Codec
	setter    *goavro.RecordSetter
	inSchema  *types.TableSchema
	filter    []int
	outSchema *types.AvroSchema
}

func initAvroEncoder(service string, db string, table string) (Encoder, error) {
	return &avroEncoder{Service: service, Db: db, Table: table}, nil
}

//Type returns type of the encoder interface (faster then type assertion?)
func (e *avroEncoder) Type() string {
	return "avro"
}

//Schema return structured schema of the table
func (e *avroEncoder) Schema() *types.TableSchema {
	return e.inSchema
}

//EncodeSchema - encodes schema
//Avro format doesn't support schema in the stream
func (e *avroEncoder) EncodeSchema(seqno uint64) ([]byte, error) {
	return nil, nil
}

//Row convert raw binary log event into Avro record
func (e *avroEncoder) Row(tp int, row *[]interface{}, seqno uint64) ([]byte, error) {
	r, err := goavro.NewRecord(*e.setter)
	if err != nil {
		return nil, err
	}
	convertRowToAvroFormat(tp, row, e.inSchema, seqno, r, e.filter)
	return encodeAvroRecord(e.codec, r)
}

//CommonFormat encodes CommonFormat event into Avro record
func (e *avroEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.UpdateCodec()
		if err != nil {
			return nil, err
		}
		e.prepareCommonFormatFilter(cf)
		return nil, nil
	}

	//TODO: Explore using reader/writer interface
	r, err := goavro.NewRecord(*e.setter)
	if err != nil {
		return nil, err
	}
	convertCommonFormatToAvroRecord(*e.setter, cf, r, e.filter)
	return encodeAvroRecord(e.codec, r)
}

// convertCommonFormatToAvroRecord creates a new Avro record from the common format event, adding the necessary
// metadata of row_key, ref_key and is_deleted.
func convertCommonFormatToAvroRecord(rs goavro.RecordSetter, cfEvent *types.CommonFormatEvent, rec *goavro.Record, filter []int) {
	//FIXME: Check errors?
	_ = rec.Set("row_key", []byte(GetCommonFormatKey(cfEvent))) //TODO: Revisit row_key from primary_key
	_ = rec.Set("ref_key", int64(cfEvent.SeqNo))
	_ = rec.Set("is_deleted", strings.EqualFold(cfEvent.Type, "delete"))

	if cfEvent.Fields == nil {
		return
	}

	var j int
	for i := 0; i < len(*cfEvent.Fields); i++ {
		if j < len(filter) && filter[j] == i {
			j++
			continue
		}
		field := (*cfEvent.Fields)[i]
		_ = rec.Set(field.Name, field.Value)
	}
}

//UpdateCodec updates encoder schema
func (e *avroEncoder) UpdateCodec() error {
	var err error
	log.Debugf("Schema codec updating")
	//Schema from state is used to encode from row format, whether in
	//binlog reader or when pipe type is local, so schema is always
	//corresponds to the message schema
	e.inSchema, err = state.GetSchema(e.Service, e.Db, e.Table)
	if log.E(err) {
		return err
	}

	e.outSchema, err = GetLatestSchema(namespace, GetOutputSchemaName(e.Service, e.Db, e.Table), "avro")
	if log.E(err) {
		return err
	}

	if len(e.inSchema.Columns)-(len(e.outSchema.Fields)-numMetadataFields) < 0 {
		err = fmt.Errorf("Input schema has less fields than output schema")
		log.E(err)
		return err
	}

	e.codec, e.setter, err = SchemaCodecHelper(e.outSchema)
	if log.E(err) {
		return err
	}

	e.prepareRowFilter()

	log.Debugf("Schema codec updated")

	return err
}

//encodeAvroRecord serializes(encodes) Avro record into byte array
func encodeAvroRecord(codec goavro.Codec, r *goavro.Record) ([]byte, error) {
	w := new(bytes.Buffer)
	err := codec.Encode(w, r)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

//fillAvroKey fills Avro records row_key from primary key of the row
func fillAvroKey(e *goavro.Record, row *[]interface{}, s *types.TableSchema) {
	var rowKey string
	//	rowKey := make([]interface{}, 0)
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].Key == "PRI" {
			if row == nil {
				//rowKey = append(rowKey, s.Columns[i].Name)
				k := fmt.Sprintf("%v", s.Columns[i].Name)
				rowKey += fmt.Sprintf("%v%v", len(k), k)
			} else {
				//rowKey = append(rowKey, (*row)[i])
				k := fmt.Sprintf("%v", (*row)[i])
				rowKey += fmt.Sprintf("%v%v", len(k), k)
			}
		}
	}
	_ = e.Set("row_key", []byte(rowKey))
}

//fillAvroFields fill fields of the Avro record from the row
//TODO: Remove ability to encode schema, so as receiver should have schema to decode
//the record, so no point in pushing schema into stream
func fillAvroFields(r *goavro.Record, row *[]interface{}, s *types.TableSchema, filter []int) {
	var j int
	for i := 0; i < len(s.Columns); i++ {
		//Skip fields which are not present in output schema
		if j < len(filter) && filter[j] == i {
			j++
			continue
		}
		var v interface{}
		if row == nil {
			v = s.Columns[i].Type
		} else {
			switch b := (*row)[i].(type) {
			case int8:
				v = int32(b)
			case uint8:
				v = int32(b)
			case int16:
				v = int32(b)
			case uint16:
				v = int32(b)
			default:
				v = b
			}
		}
		//TODO: Consider passing primary key as fields in delete event, instead
		//of separate row_key field
		//if keyOnly && s.Columns[i].Key != "PRI" {
		//	continue
		//}
		_ = r.Set(s.Columns[i].Name, v)
	}
}

//convertRowToAvroFormat uses fillAvroKey and fillAvroFields to convert
//the complete Avro record from row
func convertRowToAvroFormat(tp int, row *[]interface{}, s *types.TableSchema, seqNo uint64, r *goavro.Record, filter []int) {
	_ = r.Set("ref_key", int64(seqNo))

	switch tp {
	case types.Insert:
		fillAvroKey(r, row, s)
		fillAvroFields(r, row, s, filter)
		_ = r.Set("is_deleted", false)
	case types.Delete:
		fillAvroKey(r, row, s)
		//		fillAvroFields(r, row, s, filter, true)
		_ = r.Set("is_deleted", true)
	default:
		panic("unknown event type")
	}
}

func (e *avroEncoder) prepareCommonFormatFilter(inSchema *types.CommonFormatEvent) {
	nfiltered := len(*inSchema.Fields) - (len(e.outSchema.Fields) - numMetadataFields)
	log.Debugf("prepareCommonFormatFilter %v", nfiltered)
	if nfiltered == 0 {
		return
	}

	f := *inSchema.Fields
	e.filter = make([]int, nfiltered)
	var j int
	for i := 0; i < len(f); i++ {
		if (i-j) >= len(e.outSchema.Fields) || f[i].Name != e.outSchema.Fields[i-j].Name {
			e.filter[j] = i
			j++
		}
	}
	log.Debugf("n=%v filter=(%v)", nfiltered, e.filter)
}

func (e *avroEncoder) prepareRowFilter() {
	nfiltered := len(e.inSchema.Columns) - (len(e.outSchema.Fields) - numMetadataFields)
	log.Debugf("prepareRowFilter %v", nfiltered)
	if nfiltered == 0 {
		return
	}

	e.filter = make([]int, nfiltered)
	var j int
	for i := 0; i < len(e.inSchema.Columns); i++ {
		if (i-j) >= len(e.outSchema.Fields) || e.inSchema.Columns[i].Name != e.outSchema.Fields[i-j].Name {
			e.filter[j] = i
			j++
		}
	}

	log.Debugf("n=%v filter=(%v)", nfiltered, e.filter)
}

func (e *avroEncoder) UnwrapEvent(data []byte, cfEvent *types.CommonFormatEvent) (payload []byte, err error) {
	return nil, fmt.Errorf("Avro encoder doesn't support decoding")
}

func (e *avroEncoder) decodeEventFields(c *types.CommonFormatEvent, r *goavro.Record) error {
	hasNonNil := false
	c.Fields = new([]types.CommonFormatField)

	for i := 0; i < len(e.inSchema.Columns); i++ {
		n := e.inSchema.Columns[i].Name
		v, err := r.Get(e.inSchema.Columns[i].Name)
		if err != nil {
			return err
		}
		if v != nil {
			hasNonNil = true
		}
		*c.Fields = append(*c.Fields, types.CommonFormatField{Name: n, Value: v})
		if e.inSchema.Columns[i].Key == "PRI" && v != nil {
			c.Key = append(c.Key, v)
		}
	}

	if !hasNonNil {
		c.Fields = nil
	}

	return nil
}

func (e *avroEncoder) DecodeEvent(b []byte) (*types.CommonFormatEvent, error) {
	var c types.CommonFormatEvent

	rec, err := e.codec.Decode(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	r := rec.(*goavro.Record)

	c.Type = "insert"
	c.Key = make([]interface{}, 0)

	del, err := r.Get("is_deleted")
	if err != nil {
		return nil, err
	}
	if v, ok := del.(bool); ok && v {
		c.Type = "delete"
		// row key is needed by delete only
		rowKey, err := r.Get("row_key")
		if err != nil {
			return nil, err
		}
		if v, ok := rowKey.([]uint8); ok {
			c.Key = append(c.Key, string(v))
		} else {
			return nil, fmt.Errorf("type of row_key field should be []uint8")
		}
	} else if !ok {
		return nil, fmt.Errorf("type of is_deleted field should be bool")
	}

	seqno, err := r.Get("ref_key")
	if err != nil {
		return nil, err
	}
	if v, ok := seqno.(int64); ok {
		c.SeqNo = uint64(v)
	} else {
		return nil, fmt.Errorf("type of ref_key field should be int64")
	}
	c.Timestamp = 0

	err = e.decodeEventFields(&c, r)

	return &c, err
}
