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

//Encoder interface implementation types
const (
	Unknown int = iota
	Avro    int = iota
	Common  int = iota
)

//Encoder is unified interface to encode data from transit formats(row, common)
type Encoder interface {
	Row(tp int, row *[]interface{}, seqNo uint64) ([]byte, error)
	CommonFormat(cf *types.CommonFormatEvent) ([]byte, error)
	UpdateCodec() error
	Type() int
	Schema() *types.TableSchema
}

//TypeFromStr converts type name to type
func TypeFromStr(s string) int {
	if strings.ToLower(s) == "avro" {
		return Avro
	} else if strings.ToLower(s) == "json" {
		return Common
	}
	return Unknown
}

//StrFromType converts encoder type to type name
func StrFromType(t int) string {
	switch t {
	case Avro:
		return "avro"
	case Common:
		return "json"
	}
	return "unknown"
}

//Create is a factory which create encoder of given type for given service, db,
//table
func Create(format int, s string, d string, t string) (Encoder, error) {
	var enc Encoder

	if format == Avro {
		enc = &AvroEncoder{Service: s, Db: d, Table: t}
	} else {
		enc = &CommonFormatEncoder{Service: s, Db: d, Table: t}
	}

	err := enc.UpdateCodec()

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
