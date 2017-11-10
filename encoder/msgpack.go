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

	"github.com/tinylib/msgp/msgp"
	"github.com/uber/storagetapper/types"
)

func init() {
	registerPlugin("msgpack", initMsgPackEncoder)
}

// msgPackEncoder implements Encoder interface into message pack format.
// It inherits the methods from jsonEnocder.
type msgPackEncoder struct {
	c jsonEncoder
}

func initMsgPackEncoder(service string, db string, table string) (Encoder, error) {
	return &msgPackEncoder{c: jsonEncoder{Service: service, Db: db, Table: table}}, nil
}

//Schema returns table schema
func (e *msgPackEncoder) Schema() *types.TableSchema {
	return e.c.inSchema
}

//EncodeSchema encodes current output schema
func (e *msgPackEncoder) EncodeSchema(seqno uint64) ([]byte, error) {
	return e.Row(types.Schema, nil, seqno)
}

//Row encodes row into CommonFormat
func (e *msgPackEncoder) Row(tp int, row *[]interface{}, seqno uint64) ([]byte, error) {
	cf := e.c.convertRowToCommonFormat(tp, row, e.c.inSchema, seqno, e.c.filter)
	return e.CommonFormatEncode(cf)
}

/*UpdateCodec refreshes the schema from state DB */
func (e *msgPackEncoder) UpdateCodec() error {
	return e.c.UpdateCodec()
}

//CommonFormat encodes common format event into byte array
func (e *msgPackEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.c.UpdateCodec()
		if err != nil {
			return nil, err
		}
	}
	//FIXME: Assume that cf is in input schema format, so we need to filter it
	//to conform to output schema
	return e.CommonFormatEncode(cf)
}

//Type returns this encoder type
func (e *msgPackEncoder) Type() string {
	return "msgpack"
}

// CommonFormatEncode encodes CommonFormatEvent into byte array based on the message pack
// encoding system
// By overriding these 2 methods we get full functionality of jsonEncoder
// that implements MessagePack
func (e *msgPackEncoder) CommonFormatEncode(c *types.CommonFormatEvent) ([]byte, error) {
	return c.MarshalMsg(nil)
	// return msgpack.Marshal(c)
}

func (e *msgPackEncoder) fixFieldTypes(cf *types.CommonFormatEvent) (err error) {
	k := 0

	//Restore field types according to schema
	//MsgPack doesn't preserve int type size, so fix it
	if e.c.inSchema != nil && cf.Type != "schema" {
		for i := 0; i < len(e.c.inSchema.Columns); i++ {
			if cf.Fields != nil && i < len(*cf.Fields) {
				f := &(*cf.Fields)[i]
				switch v := f.Value.(type) {
				case int64:
					switch e.c.inSchema.Columns[i].DataType {
					case "int", "integer", "tinyint", "smallint", "mediumint", "year":
						f.Value = int32(v)
					}
				}
			}

			if e.c.inSchema.Columns[i].Key == "PRI" && k < len(cf.Key) {
				f := &cf.Key[k]
				switch v := (*f).(type) {
				case float64:
					switch e.c.inSchema.Columns[i].DataType {
					case "int", "integer", "tinyint", "smallint", "mediumint", "year":
						*f = int32(v)
					}
				}
				k++
			}
		}
	}
	return err
}

func (e *msgPackEncoder) msgPackDecode(b []byte) (*types.CommonFormatEvent, []byte, error) {
	cf := &types.CommonFormatEvent{}
	rem, err := cf.UnmarshalMsg(b)
	if err != nil {
		return nil, nil, err
	}

	return cf, rem, e.fixFieldTypes(cf)
}

/*
func (e *msgPackEncoder) schemaDecode(b []byte) (*types.CommonFormatEvent, error) {
	return msgPackDecode(b)
}
*/

// UnwrapEvent splits the event header and payload
// cfEvent is populated with the 'header' information aka the first decoding.
// Data after the header returned in the payload parameter
func (e *msgPackEncoder) UnwrapEvent(data []byte, cfEvent *types.CommonFormatEvent) (payload []byte, err error) {
	err = cfEvent.DecodeMsg(msgp.NewReader(bytes.NewBuffer(data)))
	if err != nil {
		return
	}
	err = e.fixFieldTypes(cfEvent)
	if err != nil {
		return
	}
	return msgp.Skip(data)
}

//DecodeEvent decodes MsgPack encoded array into CommonFormatEvent struct
func (e *msgPackEncoder) DecodeEvent(b []byte) (*types.CommonFormatEvent, error) {
	cf, _, err := e.msgPackDecode(b)
	return cf, err
}
