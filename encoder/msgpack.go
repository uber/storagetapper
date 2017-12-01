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
	jsonEncoder
}

func initMsgPackEncoder(service string, db string, table string) (Encoder, error) {
	return &msgPackEncoder{jsonEncoder{Service: service, Db: db, Table: table}}, nil
}

//Row encodes row into CommonFormat
func (e *msgPackEncoder) Row(tp int, row *[]interface{}, seqno uint64) ([]byte, error) {
	cf := e.convertRowToCommonFormat(tp, row, e.inSchema, seqno, e.filter)
	return cf.MarshalMsg(nil)
}

//EncodeSchema encodes current output schema
func (e *msgPackEncoder) EncodeSchema(seqno uint64) ([]byte, error) {
	return e.Row(types.Schema, nil, seqno)
}

//CommonFormat encodes common format event into byte array
func (e *msgPackEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.UpdateCodec()
		if err != nil {
			return nil, err
		}
	}
	cf = filterCommonFormat(e.filter, cf)
	return cf.MarshalMsg(nil)
}

//Type returns this encoder type
func (e *msgPackEncoder) Type() string {
	return "msgpack"
}

func (e *msgPackEncoder) fixFieldTypes(cf *types.CommonFormatEvent) (err error) {
	k := 0

	//Restore field types according to schema
	//MsgPack doesn't preserve int type size, so fix it
	if e.inSchema != nil && cf.Type != "schema" {
		for i, j := 0, 0; i < len(e.inSchema.Columns); i++ {
			if filteredField(e.filter, i, &j) {
				continue
			}
			if cf.Fields != nil && i-j < len(*cf.Fields) {
				f := &(*cf.Fields)[i-j]
				switch v := f.Value.(type) {
				case int64:
					switch e.inSchema.Columns[i].DataType {
					case "int", "integer", "tinyint", "smallint", "mediumint", "year":
						f.Value = int32(v)
					}
				}
			}

			if e.inSchema.Columns[i].Key == "PRI" && k < len(cf.Key) {
				f := &cf.Key[k]
				switch v := (*f).(type) {
				case float64:
					switch e.inSchema.Columns[i].DataType {
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
