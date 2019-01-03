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
	"bytes"
	"text/template"
	"time"

	"github.com/uber/storagetapper/types"
)

type topicParams struct {
	types.TableLoc
	Timestamp time.Time
}

func getTopicName(template *template.Template, tp *topicParams) (string, error) {
	buf := &bytes.Buffer{}
	err := template.Execute(buf, tp)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

// GetOutputTopicName returns output topic name
func (c *AppConfig) GetOutputTopicName(svc string, db string, tbl string, input string, output string, ver int, ts time.Time) (string, error) {
	tmpl := c.OutputTopicNameTemplateDefaultParsed

	inp := c.OutputTopicNameTemplateParsed[input]
	if inp != nil {
		out := inp[output]
		if out != nil {
			tmpl = out
		}
	}

	return getTopicName(tmpl, &topicParams{types.TableLoc{Service: svc, Cluster: "", Db: db, Table: tbl, Input: input, Output: output, Version: ver}, ts})
}

// GetChangelogTopicName returns output topic name
func (c *AppConfig) GetChangelogTopicName(svc string, db string, tbl string, input string, output string, ver int, ts time.Time) (string, error) {
	tmpl := c.ChangelogTopicNameTemplateDefaultParsed

	inp := c.ChangelogTopicNameTemplateParsed[input]
	if inp != nil {
		out := inp[output]
		if out != nil {
			tmpl = out
		}
	}

	return getTopicName(tmpl, &topicParams{types.TableLoc{Service: svc, Cluster: "", Db: db, Table: tbl, Input: input, Output: output, Version: ver}, ts})
}
