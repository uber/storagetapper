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
	"time"

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
	Input     string
	Output    string
	Version   int
	codec     goavro.Codec
	setter    *goavro.RecordSetter
	inSchema  *types.TableSchema
	filter    []int
	outSchema *types.AvroSchema
}

func initAvroEncoder(service, db, table, input string, output string, version int) (Encoder, error) {
	return &avroEncoder{Service: service, Db: db, Table: table, Input: input, Output: output, Version: version}, nil
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
func (e *avroEncoder) Row(tp int, row *[]interface{}, seqno uint64, _ time.Time) ([]byte, error) {
	r, err := goavro.NewRecord(*e.setter)
	if err != nil {
		return nil, err
	}
	err = convertRowToAvroFormat(tp, row, e.inSchema, seqno, r, e.filter)
	if err != nil {
		return nil, err
	}
	return encodeAvroRecord(e.codec, r)
}

//CommonFormat encodes CommonFormat event into Avro record
func (e *avroEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.UpdateCodec()
		return nil, err
	}

	//TODO: Explore using reader/writer interface
	r, err := goavro.NewRecord(*e.setter)
	if err != nil {
		return nil, err
	}
	err = convertCommonFormatToAvroRecord(*e.setter, cf, r, e.filter)
	if err != nil {
		return nil, err
	}
	return encodeAvroRecord(e.codec, r)
}

// convertCommonFormatToAvroRecord creates a new Avro record from the common format event, adding the necessary
// metadata of row_key, ref_key and is_deleted.
func convertCommonFormatToAvroRecord(rs goavro.RecordSetter, cfEvent *types.CommonFormatEvent, rec *goavro.Record, filter []int) error {
	err := rec.Set("row_key", []byte(GetCommonFormatKey(cfEvent))) //TODO: Revisit row_key from primary_key
	if err != nil {
		return err
	}
	err = rec.Set("ref_key", int64(cfEvent.SeqNo))
	if err != nil {
		return err
	}
	err = rec.Set("is_deleted", strings.EqualFold(cfEvent.Type, "delete"))
	if err != nil {
		return err
	}

	if cfEvent.Fields == nil {
		return nil
	}

	for i, j := 0, 0; i < len(*cfEvent.Fields); i++ {
		if filteredField(filter, i, &j) {
			continue
		}
		field := (*cfEvent.Fields)[i]

		/* If the field is integer convert it from JSON's float number */
		switch r := field.Value.(type) {
		case float64:
			s, err := rec.GetFieldSchema(field.Name)
			if err != nil {
				return err
			}
			for _, t := range s.(map[string]interface{})["type"].([]interface{}) {
				switch t.(string) {
				case "int":
					field.Value = int32(r)
				case "long":
					field.Value = int64(r)
				}
			}
		case time.Time:
			if !r.Equal(time.Time{}) {
				field.Value = r.UnixNano() / 1000000
			} else {
				field.Value = nil
			}
		}
		err = rec.Set(field.Name, field.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

//UpdateCodec updates encoder schema
func (e *avroEncoder) UpdateCodec() error {
	var err error
	log.Debugf("Schema codec (%v) updating", e.Type())
	//Schema from state is used to encode from row format, whether in
	//binlog reader or when pipe type is local, so schema is always
	//corresponds to the message schema
	e.inSchema, err = state.GetSchema(e.Service, e.Db, e.Table, e.Input, e.Output, e.Version)
	if log.E(err) {
		return err
	}
	n, err := GetOutputSchemaName(e.Service, e.Db, e.Table, e.Input, e.Output, e.Version)
	if log.E(err) {
		return err
	}
	e.outSchema, err = GetLatestSchema("production", n, "avro")
	if log.E(err) {
		return err
	}

	if len(e.inSchema.Columns)-(len(e.outSchema.Fields)-numMetadataFields) < 0 {
		err = fmt.Errorf("input schema has less fields than output schema")
		log.E(err)
		return err
	}

	e.codec, e.setter, err = SchemaCodecHelper(e.outSchema)
	if log.E(err) {
		return err
	}

	e.filter = prepareFilter(e.inSchema, e.outSchema, numMetadataFields)

	log.Debugf("Schema codec (%v) updated", e.Type())

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
func fillAvroKey(e *goavro.Record, row *[]interface{}, s *types.TableSchema) error {
	var rowKey string
	//	rowKey := make([]interface{}, 0)
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].Key == "PRI" {
			/* if row == nil {
				//rowKey = append(rowKey, s.Columns[i].Name)
				k := fmt.Sprintf("%v", s.Columns[i].Name)
				rowKey += fmt.Sprintf("%v%v", len(k), k)
			} else { */
			//rowKey = append(rowKey, (*row)[i])
			k := fmt.Sprintf("%v", (*row)[i])
			rowKey += fmt.Sprintf("%v%v", len(k), k)
			//}
		}
	}
	return e.Set("row_key", []byte(rowKey))
}

func convertTime(b string, dtype string) (interface{}, error) {
	var v interface{}
	//mysql binlog reader library returns string for binary type
	if dtype == "timestamp" || dtype == "datetime" {
		if strings.HasPrefix(b, "0000-00-00 00:00:00") {
			return nil, nil
		}
		t, err := time.Parse(time.RFC3339Nano, b)
		if err != nil {
			return nil, err
		}
		v = t.UnixNano() / 1000000
	} else if dtype == "binary" || dtype == "varbinary" {
		v = []byte(b)
	} else {
		v = b
	}
	return v, nil
}

func convertText(b []byte, d string) interface{} {
	//mysql binlog reader library returns []byte for text type
	if d == "text" || d == "tinytext" || d == "mediumtext" || d == "longtext" || d == "json" {
		return string(b)
	}
	return b
}

func fixAvroFieldType(i interface{}, dtype string, ftype string) (interface{}, error) {
	var err error
	var v interface{}
	/*if row == nil {
		v = s.Columns[i].Type
	} else { */
	switch b := i.(type) {
	case int8:
		if ftype == types.MySQLBoolean {
			v = false
			if b != 0 {
				v = true
			}
		} else {
			v = int32(b)
		}
	case uint8:
		v = int32(b)
	case int16:
		v = int32(b)
	case uint16:
		v = int32(b)
	case time.Time:
		v = b.UnixNano() / 1000000 //milliseconds
	case string:
		v, err = convertTime(b, dtype)
		if err != nil {
			return nil, err
		}
	case []byte:
		v = convertText(b, dtype)
	default:
		v = b
	}
	return v, nil
}

//fillAvroFields fill fields of the Avro record from the row
//TODO: Remove ability to encode schema, so as receiver should have schema to decode
//the record, so no point in pushing schema into stream
func fillAvroFields(r *goavro.Record, row *[]interface{}, s *types.TableSchema, filter []int) error {
	for i, j := 0, 0; i < len(s.Columns); i++ {
		//Skip fields which are not present in output schema
		if filteredField(filter, i, &j) {
			continue
		}
		v, err := fixAvroFieldType((*row)[i], s.Columns[i].DataType, s.Columns[i].Type)
		if err != nil {
			return err
		}
		//}
		//TODO: Consider passing primary key as fields in delete event, instead
		//of separate row_key field
		//if keyOnly && s.Columns[i].Key != "PRI" {
		//	continue
		//}
		err = r.Set(s.Columns[i].Name, v)
		if err != nil {
			return err
		}
	}
	return nil
}

//convertRowToAvroFormat uses fillAvroKey and fillAvroFields to convert
//the complete Avro record from row
func convertRowToAvroFormat(tp int, row *[]interface{}, s *types.TableSchema, seqNo uint64, r *goavro.Record, filter []int) error {
	err := r.Set("ref_key", int64(seqNo))
	if err != nil {
		return err
	}

	switch tp {
	case types.Insert:
		err = fillAvroKey(r, row, s)
		if err != nil {
			return err
		}
		err = fillAvroFields(r, row, s, filter)
		if err != nil {
			return err
		}
		err = r.Set("is_deleted", false)
		if err != nil {
			return err
		}
	case types.Delete:
		err = fillAvroKey(r, row, s)
		if err != nil {
			return err
		}
		//		fillAvroFields(r, row, s, filter, true)
		err = r.Set("is_deleted", true)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown event type: %v", tp)
	}
	return nil
}

func prepareFilter(in *types.TableSchema, out *types.AvroSchema, numMetaFields int) []int {
	if out == nil {
		return nil
	}

	nfiltered := len(in.Columns)
	if out.Fields != nil {
		nfiltered = nfiltered - (len(out.Fields) - numMetaFields)
	}
	if nfiltered == 0 {
		return nil
	}

	f := out.Fields
	filter := make([]int, 0)
	var j int
	for i := 0; i < len(in.Columns); i++ {
		//Primary key cannot be filtered
		if (i-j) >= len(f) || in.Columns[i].Name != f[i-j].Name {
			if in.Columns[i].Key != "PRI" {
				log.Debugf("Field %v will be filtered", in.Columns[i].Name)
				filter = append(filter, i)
			}
			j++
		}
	}

	log.Debugf("len=%v, filtered fields (%v)", len(filter), filter)

	return filter
}

func (e *avroEncoder) UnwrapEvent(data []byte, cfEvent *types.CommonFormatEvent) (payload []byte, err error) {
	return nil, fmt.Errorf("avro encoder doesn't support wrapping")
}

func (e *avroEncoder) decodeEventFields(c *types.CommonFormatEvent, r *goavro.Record) error {
	hasNonNil := false
	c.Fields = new([]types.CommonFormatField)

	for i, j := 0, 0; i < len(e.inSchema.Columns); i++ {
		if filteredField(e.filter, i, &j) {
			continue
		}
		n := e.inSchema.Columns[i].Name
		v, err := r.Get(n)
		if err != nil && !strings.Contains(err.Error(), "no such field") {
			return err
		}
		if v != nil {
			hasNonNil = true

			dt := e.inSchema.Columns[i].DataType
			if dt == "timestamp" || dt == "datetime" {
				ms, ok := v.(int64)
				if !ok {
					return fmt.Errorf("timestamp and datetime formats expected to be int64")
				}
				t := time.Unix(ms/1000, (ms%1000)*1000000)
				if dt == "datetime" {
					t = t.In(time.UTC)
				}
				v = t
			}
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

	return &c, e.decodeEventFields(&c, r)
}
