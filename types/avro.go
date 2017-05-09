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

package types

//AvroPrimitiveType is declared to improve readability AvroSchema/AvroFields
//declarations
type AvroPrimitiveType string

//AvroComplexType is declared to improve readability AvroSchema/AvroFields
type AvroComplexType string

//Avro data types
const (
	AvroNULL    AvroPrimitiveType = "null"
	AvroBOOLEAN AvroPrimitiveType = "boolean"
	AvroINT     AvroPrimitiveType = "int"
	AvroLONG    AvroPrimitiveType = "long"
	AvroFLOAT   AvroPrimitiveType = "float"
	AvroDOUBLE  AvroPrimitiveType = "double"
	AvroBYTES   AvroPrimitiveType = "bytes"
	AvroSTRING  AvroPrimitiveType = "string"

	AvroRECORD AvroComplexType = "record"
)

// AvroSchema represents the structure of Avro schema format
type AvroSchema struct {
	Fields        []AvroField     `json:"fields"`
	Name          string          `json:"name"`
	Namespace     string          `json:"namespace"`
	Owner         string          `json:"owner"`
	SchemaID      int             `json:"schema_id"`
	SchemaVersion int             `json:"schemaVersion"`
	Type          AvroComplexType `json:"type"`
	Doc           string          `json:"doc"`
	LastModified  string          `json:"last_modified"`
}

// AvroField represents structure of each of the fields in the schema
type AvroField struct {
	Name    string              `json:"name"`
	Type    []AvroPrimitiveType `json:"type"`
	Default interface{}         `json:"default"`
	Doc     string              `json:"doc"`
}
