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
	"fmt"

	"github.com/linkedin/goavro"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

const (
	namespace  = types.MySvcName
	websterURL = "http://localhost:14040/webster/api/v2/"
)

//GetLatestSchemaFunc is a type to implement schema resolver polymorphism
type GetLatestSchemaFunc func(namespace string, schemaName string, typ string) (*types.AvroSchema, error)

//GetLatestSchema is the pointer to schema resolver
var GetLatestSchema GetLatestSchemaFunc = GetSchemaWebster

// GetSchemaWebster makes a GET HTTP call to webster schema service to get the latest schema version
// for a given namespace and schema name.
func GetSchemaWebster(namespace string, schemaName string, typ string) (*types.AvroSchema, error) {
	resp, err := util.HTTPGet(websterURL + fmt.Sprintf("%s/%s/", namespace, schemaName))
	if err != nil {
		return nil, err
	}
	a := &types.AvroSchema{}
	err = json.Unmarshal(resp, a)
	return a, err
}

//SchemaCodecHelper gets Avro codec and Avro record setter from schema structure
func SchemaCodecHelper(avroSchema *types.AvroSchema) (goavro.Codec, *goavro.RecordSetter, error) {
	sch, err := json.Marshal(avroSchema)
	if err != nil {
		return nil, nil, err
	}
	schStr := util.BytesToString(sch)

	codec, err := goavro.NewCodec(schStr)
	if err != nil {
		return nil, nil, err
	}
	recSch := goavro.RecordSchema(schStr)
	return codec, &recSch, nil
}

//GetLatestSchemaCodec resolves schema and converts it to Avro codec and setter
func GetLatestSchemaCodec(service string, db string, table string, typ string) (goavro.Codec, *goavro.RecordSetter, error) {
	avroSchema, err := GetLatestSchema(namespace, GetOutputSchemaName(service, db, table), typ)
	if err != nil {
		return nil, nil, err
	}
	return SchemaCodecHelper(avroSchema)
}

//GetOutputSchemaName combines parameter into output topic name
func GetOutputSchemaName(service string, db string, table string) string {
	return fmt.Sprintf("hp-tap-%s-%s-%s", service, db, table)
}
