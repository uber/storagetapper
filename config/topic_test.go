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

package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var testFile = `
{test_replace}_topic_name_template_default: "default.{{.Service}}.db.{{.DB}}.table.{{.Table}}"
{test_replace}_topic_name_template:
    mysql:
        kafka: "topic.{{.Service}}.db.{{.DB}}.table.{{.Table}}{{if .Version}}.v{{.Version}}{{end}}"
        file: "{{.Service}}/{{.DB}}/{{.Table}}/{{.Version}}/"
        hdfs: "no-parameters"
    mysqlts:
        hdfs: 'topic.{{.Service}}.db.{{.DB}}.table.{{.Table}}{{if .Version}}.v{{.Version}}{{end}}.{{.Timestamp.Format "20060102150405"}}'
    fail_execute:
        fail_execute: "{{.no_such_field}}"
    file:
`

var testFileNeg = `{test_replace}: "{{.Service}"`

func checkFail(t *testing.T, err error) {
	if err != nil {
		fmt.Printf("%v\n", err)
		t.FailNow()
	}
}

func prepare(t *testing.T, typ string, content string, replace string) (func(svc string, db string, tbl string, input string, output string, ver int, ts time.Time) (string, error), error) {
	c := strings.Replace(content, "{test_replace}", replace, -1)
	resetStdLoader(func(_ interface{}, file string) ([]byte, error) {
		return []byte(c), nil
	})

	err := Load()
	if err != nil {
		return nil, err
	}

	cfg := Get()

	getName := cfg.GetChangelogTopicName
	if typ == "output" {
		getName = cfg.GetOutputTopicName
	}

	return getName, nil
}

func testTopicNameFormat(t *testing.T, typ string) {
	getName, err := prepare(t, typ, testFile, typ)
	checkFail(t, err)

	ts, err := time.Parse("Jan 2 15:04:05 2006", "Jul 27 22:08:10 2018")
	checkFail(t, err)

	res, err := getName("svc1", "db1", "t1", "mysql", "kafka", 1, ts)
	checkFail(t, err)
	expected := "topic.svc1.db.db1.table.t1.v1"
	if res != expected {
		t.Fatalf("got %v, expected %v", res, expected)
	}

	res, err = getName("svc1", "db1", "t1", "mysql", "kafka", 0, ts)
	checkFail(t, err)
	expected = "topic.svc1.db.db1.table.t1"
	if res != expected {
		t.Fatalf("got %v, expected: %v", res, expected)
	}

	//Test default template
	res, err = getName("svc2", "db2", "t2", "not_in_input_map", "file", 1, ts)
	checkFail(t, err)
	expected = "default.svc2.db.db2.table.t2"
	if res != expected {
		t.Fatalf("got %v, expected: %v", res, expected)
	}

	res, err = getName("svc2", "db2", "t2", "mysql", "not_in_output_map", 1, ts)
	checkFail(t, err)
	expected = "default.svc2.db.db2.table.t2"
	if res != expected {
		t.Fatalf("got %v, expected: %v", res, expected)
	}

	//Test empty input map
	res, err = getName("svc2", "db2", "t2", "file", "file", 1, ts)
	checkFail(t, err)
	expected = "default.svc2.db.db2.table.t2"
	if res != expected {
		t.Fatalf("got %v, expected: %v", res, expected)
	}

	//Test no parameters
	res, err = getName("svc2", "db2", "t2", "mysql", "hdfs", 1, ts)
	checkFail(t, err)
	expected = "no-parameters"
	if res != expected {
		t.Fatalf("got %v, expected: %v", res, expected)
	}

	_, err = getName("na", "na", "na", "fail_execute", "fail_execute", 0, ts)
	if err == nil {
		t.Fatalf("should fail execute template with unknown field in it")
	}

	res, err = getName("svc1", "db1", "t1", "mysqlts", "hdfs", 0, ts)
	checkFail(t, err)
	expected = "topic.svc1.db.db1.table.t1.20180727220810"
	if res != expected {
		t.Fatalf("got %v, expected: %v", res, expected)
	}

}

func TestChangelogTopicNameFormat(t *testing.T) {
	testTopicNameFormat(t, "changelog")
}

func TestOutputTopicNameFormat(t *testing.T) {
	testTopicNameFormat(t, "output")
}

func TestOutputTopicNameFormatNegative(t *testing.T) {
	_, err := prepare(t, "output", testFileNeg, "output_topic_name_template_default")
	if err == nil {
		t.Fatalf("should fail to parse the broken file")
	}

	_, err = prepare(t, "changelog", testFileNeg, "changelog_topic_name_template_default")
	if err == nil {
		t.Fatalf("should fail to parse the broken file")
	}

	_, err = prepare(t, "output", testFileNeg, "output_topic_name_template:\n  mysql:\n    kafka")
	if err == nil || err.Error() != `template: mysql.kafka:1: unexpected "}" in operand` {
		t.Fatalf("should fail to parse the broken file")
	}

	_, err = prepare(t, "changelog", testFileNeg, "changelog_topic_name_template:\n  mysql:\n    kafka")
	if err == nil || err.Error() != `template: mysql.kafka:1: unexpected "}" in operand` {
		t.Fatalf("should fail to parse the broken file")
	}
}
