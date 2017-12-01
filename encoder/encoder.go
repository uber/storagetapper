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
	"fmt"
	"strings"

	"github.com/uber/storagetapper/types"
)

//encoderConstructor initializes encoder plugin
type encoderConstructor func(service string, db string, table string) (Encoder, error)

//plugins insert their constructors into this map
var encoders map[string]encoderConstructor

//Internal is encoder for intermediate buffer messages and message wrappers. Initialized in z.go
var Internal Encoder

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
	EncodeSchema(seqNo uint64) ([]byte, error)
	UpdateCodec() error
	Type() string
	Schema() *types.TableSchema

	UnwrapEvent(data []byte, cfEvent *types.CommonFormatEvent) (payload []byte, err error)
	DecodeEvent(b []byte) (*types.CommonFormatEvent, error)
}

//Encoders return the list of encoders names
func Encoders() []string {
	r := make([]string, len(encoders))
	i := 0
	for k := range encoders {
		r[i] = k
		i++
	}
	return r
}

//InitEncoder constructs encoder without updating schema
func InitEncoder(encType string, s string, d string, t string) (Encoder, error) {
	init := encoders[strings.ToLower(encType)]

	if init == nil {
		return nil, fmt.Errorf("Unsupported encoder: %s", strings.ToLower(encType))
	}

	enc, err := init(s, d, t)
	if err != nil {
		return nil, err
	}

	return enc, nil
}

//Create is a factory which create encoder of given type for given service, db,
//table
func Create(encType string, s string, d string, t string) (Encoder, error) {
	enc, err := InitEncoder(encType, s, d, t)
	if err != nil {
		return nil, err
	}
	return enc, enc.UpdateCodec()
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

func filteredField(filter []int, i int, j *int) bool {
	if *j < len(filter) && filter[*j] == i {
		(*j)++
		return true
	}
	return false
}
