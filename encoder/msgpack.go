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
	"time"

	"github.com/tinylib/msgp/msgp"
	"github.com/uber/storagetapper/types"
)

func init() {
	registerPlugin("msgpack", initMsgPackEncoder)
}

// msgPackEncoder implements Encoder interface into message pack format.
// It inherits the methods from jsonEncoder.
type msgPackEncoder struct {
	jsonEncoder
}

func initMsgPackEncoder(service, db, table, input string, output string, version int) (Encoder, error) {
	return &msgPackEncoder{jsonEncoder{Service: service, DB: db, Table: table, Input: input, Output: output, Version: version}}, nil
}

//Row encodes row into CommonFormat
func (e *msgPackEncoder) Row(tp int, row *[]interface{}, seqno uint64, _ time.Time) ([]byte, error) {
	cf := e.convertRowToCommonFormat(tp, row, e.inSchema, seqno, e.filter)
	return cf.MarshalMsg(nil)
}

//EncodeSchema encodes current output schema
func (e *msgPackEncoder) EncodeSchema(seqno uint64) ([]byte, error) {
	return e.Row(types.Schema, nil, seqno, time.Time{})
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

func (e *msgPackEncoder) fixFieldType(dtype string, f interface{}) interface{} {
	switch v := f.(type) {
	case int64:
		switch dtype {
		case "int", "integer", "tinyint", "smallint", "mediumint", "year":
			return int32(v)
		}
	case time.Time:
		switch dtype {
		case "datetime":
			return v.In(time.UTC)
		case "timestamp":
			if v.Equal(time.Time{}) {
				return v.In(time.UTC)
			}
			return v.In(time.Local)
		}
	//There is one corner case when time can be encoded as string it's 0000-00-00 00:00:00
	case string:
		switch dtype {
		case "datetime":
			return ZeroTime
		case "timestamp":
			return ZeroTime
		}
	}
	return f
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
				f.Value = e.fixFieldType(e.inSchema.Columns[i].DataType, f.Value)
			}

			if e.inSchema.Columns[i].Key == "PRI" && k < len(cf.Key) {
				cf.Key[k] = e.fixFieldType(e.inSchema.Columns[i].DataType, cf.Key[k])
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
	b, ok := cfEvent.Key[0].([]uint8)
	if len(cfEvent.Key) > 1 || !ok || cfEvent.Type == "insert" || cfEvent.Type == "delete" {
		if err = e.fixFieldTypes(cfEvent); err != nil {
			return
		}
	} else {
		cfEvent.Key[0] = string(b)
	}
	return msgp.Skip(data)
}

//DecodeEvent decodes MsgPack encoded array into CommonFormatEvent struct
func (e *msgPackEncoder) DecodeEvent(b []byte) (*types.CommonFormatEvent, error) {
	cf, _, err := e.msgPackDecode(b)
	return cf, err
}
