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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tinylib/msgp/msgp"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

//encoderConstructor initializes encoder plugin
type encoderConstructor func(service string, db string, table string) (Encoder, error)

//plugins insert their constructors into this map
var encoders map[string]encoderConstructor

var defaultEncoderType = config.Get().EncoderType

//registerPlugin should be called from plugin's init
func registerPlugin(name string, init encoderConstructor) {
	if encoders == nil {
		encoders = make(map[string]encoderConstructor)
	}
	encoders[name] = init
}

//Encoder is unified interface to encode data from transit formats(row, common)
type Encoder interface {
	Row(tp int, row *[]interface{}, seqNo uint64) ([]byte, error)
	CommonFormat(cf *types.CommonFormatEvent) ([]byte, error)
	UpdateCodec() error
	Type() string
	Schema() *types.TableSchema
}

// BufferedDecoder is a struct that allows us to enscapulate type of internal
// encoding system
type BufferedDecoder struct {
	JSONDec *json.Decoder
	MsgDec  *msgp.Reader
}

//Create is a factory which create encoder of given type for given service, db,
//table
func Create(encType string, s string, d string, t string) (Encoder, error) {
	init := encoders[strings.ToLower(encType)]

	enc, err := init(s, d, t)
	if err != nil {
		return nil, err
	}

	err = enc.UpdateCodec()

	return enc, err
}

//GetRowKey concatenates row primary key fields into string
//TODO: Should we encode into byte array instead?
func GetRowKey(s *types.TableSchema, row *[]interface{}) string {
	var key string
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].Key == "PRI" {
			if row == nil {
				k := fmt.Sprintf("%v", s.Columns[i].Name)
				key += fmt.Sprintf("%v%v", len(k), k)
			} else {
				k := fmt.Sprintf("%v", (*row)[i])
				key += fmt.Sprintf("%v%v", len(k), k)
			}
		}
	}
	return key
}

//GetCommonFormatKey concatenates common format key into string
func GetCommonFormatKey(cf *types.CommonFormatEvent) string {
	var key string
	for _, v := range cf.Key {
		s := fmt.Sprintf("%v", v)
		key += fmt.Sprintf("%v%v", len(s), s)
	}
	return key
}

// CommonFormatEncode encodes a CommonFormatEvent into the given
// encoding type
func CommonFormatEncode(c *types.CommonFormatEvent) ([]byte, error) {
	if defaultEncoderType == "json" {
		return json.Marshal(c)
	} else if defaultEncoderType == "msgpack" {
		return c.MarshalMsg(nil)
	} else {
		return nil, fmt.Errorf("Use supported encoders")
	}
}

// DecodeToCommonFormat decodes a byte array into a
// CommonFormatEvent based on the given encoding
func DecodeToCommonFormat(b []byte) (*types.CommonFormatEvent, error) {
	res := &types.CommonFormatEvent{}
	var err error
	if defaultEncoderType == "json" {
		err = json.Unmarshal(b, res)
	} else if defaultEncoderType == "msgpack" {
		_, err = res.UnmarshalMsg(b)
	}
	return res, err
}

// GetBufferedDecoder gets a buffered decoder for the various encoding types possible based on
// the default encoding type
// cfEvent is populated with the 'header' information aka the first decoding.
// The decoders/readers are stored in bd for future use
// buf contains data with offset of after first cfEvent has been read
func GetBufferedDecoder(data []byte, cfEvent *types.CommonFormatEvent) (bd *BufferedDecoder, buf *bytes.Buffer, err error) {
	bd = &BufferedDecoder{}
	buf = bytes.NewBuffer(data)
	if defaultEncoderType == "json" {
		dec := json.NewDecoder(buf)
		err = dec.Decode(cfEvent)
		if err != nil {
			log.Errorf("Issue in GetBufferedDecoder")
			return
		}
		bd.JSONDec = dec
	} else if defaultEncoderType == "msgpack" {
		dec := msgp.NewReader(buf)
		err = cfEvent.DecodeMsg(dec)
		if err != nil {
			return
		}
		bd.MsgDec = dec
		// to ensure no error shadowing
		var payload []byte
		payload, err = msgp.Skip(data)
		if err != nil {
			return
		}
		buf = bytes.NewBuffer(payload)
	} else {
		err = fmt.Errorf("Unsupported defaulted encoder type")
	}
	return
}

// BufferedReadFrom reads the buffered input from the decoder
// in the case of message pack we just continue since the buffering
// has been done when getting the decoder
func BufferedReadFrom(buf *bytes.Buffer, bd *BufferedDecoder) (err error) {
	if defaultEncoderType == "json" {
		bufReader := bd.JSONDec.Buffered()
		_, err = buf.ReadFrom(bufReader)
	} else {
		err = fmt.Errorf("Unsupported defaulted encoder type")
	}
	return
}

// GetDefaultEncoderType returns the type of the default internal encoder
// currently only json and message pack eligible
func GetDefaultEncoderType() string {
	return defaultEncoderType
}

//CommonFormatUpdateCodecFromDB is used in test to get schema from original database
//instread of reading it from state as UpdateCodec does
func CommonFormatUpdateCodecFromDB(enc Encoder) error {
	switch enc.(type) {
	case *commonFormatEncoder:
		return enc.(*commonFormatEncoder).updateCodecFromDB()
	case *msgPackEncoder:
		return enc.(*msgPackEncoder).c.updateCodecFromDB()
	}
	return fmt.Errorf("Encoder not supported")
}
