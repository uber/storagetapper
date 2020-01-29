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

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

type encoder struct {
	Service string
	Db      string
	Table   string
	Input   string
	Output  string
	Version int

	filter        []int //Contains indexes of fields which are not in output schema
	filterEnabled bool
}

//encoderConstructor initializes encoder plugin
type encoderConstructor func(service, db, table, input string, output string, version int, filtering bool) (Encoder, error)

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
	Row(tp int, row *[]interface{}, seqNo uint64, ts time.Time) ([]byte, error)
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
func InitEncoder(encType, svc, sdb, tbl, input string, output string, version int, filtering bool) (Encoder, error) {
	init := encoders[strings.ToLower(encType)]

	if init == nil {
		return nil, fmt.Errorf("unsupported encoder: %s", strings.ToLower(encType))
	}

	enc, err := init(svc, sdb, tbl, input, output, version, filtering)
	if err != nil {
		return nil, err
	}

	return enc, nil
}

//Create is a factory which create encoder of given type for given service, db,
//table, input, output, version
func Create(encType, svc, sdb, tbl, input string, output string, version int, filtering bool) (Encoder, error) {
	enc, err := InitEncoder(encType, svc, sdb, tbl, input, output, version, filtering)
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

//WrapEvent prepend provided payload with CommonFormat like event
func WrapEvent(outputFormat string, key string, bd []byte, seqno uint64) ([]byte, error) {
	akey := make([]interface{}, 1)
	akey[0] = []byte(key)

	cfw := types.CommonFormatEvent{
		Type:      outputFormat,
		Key:       akey,
		SeqNo:     seqno,
		Timestamp: time.Now().UnixNano(),
		Fields:    nil,
	}

	cfb, err := Internal.CommonFormat(&cfw)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(cfb)
	_, err = buf.Write(bd)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type fmapItem struct {
	inPos  int
	outPos int
}

func prepareFilter(in *types.TableSchema, out *types.AvroSchema, cf *types.CommonFormatEvent, filterEnabled bool) []int {
	log.Debugf("prepareFilter in_len=%v out=%v cf=%v", len(in.Columns), out, cf)
	filter := make([]int, len(in.Columns))
	fmap := make(map[string]*fmapItem, len(in.Columns))

	for i := 0; i < len(in.Columns); i++ {
		fmap[in.Columns[i].Name] = &fmapItem{i, -1}
	}

	if filterEnabled && out != nil && out.Fields != nil {
		for i := 0; i < len(out.Fields); i++ {
			t := fmap[out.Fields[i].Name]
			if t != nil {
				t.outPos = i
			}
		}
	} else if filterEnabled && cf != nil && cf.Fields != nil {
		for i := 0; i < len(*cf.Fields); i++ {
			t := fmap[(*cf.Fields)[i].Name]
			if t != nil {
				t.outPos = i
			}
		}
	} else { // If schema is not defined or filter disabled produce all the fields as is
		for i := 0; i < len(in.Columns); i++ {
			t := fmap[in.Columns[i].Name]
			t.outPos = i
		}
	}

	for _, v := range fmap {
		filter[v.inPos] = v.outPos
	}

	log.Debugf("prepareFilter filter=%+v", filter)

	return filter
}

/*
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
*/
