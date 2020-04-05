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

package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	yaml "gopkg.in/yaml.v2"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

//TODO: More tests

func configsTableInit(t *testing.T) {
	conn := state.GetDB()
	if conn == nil {
		t.FailNow()
	}
	_, err := conn.Exec("TRUNCATE TABLE " + types.MyDBName + ".config")
	if err != nil {
		log.Errorf("Error truncating table: %v", err)
	}
}

/*
func configJSONRequest(cmd configReq, code int, t *testing.T) {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest("POST", "/config", bytes.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	configCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK: code=%v", res.Code)
}
*/

func configFormRequest(typ string, cmd configReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body := url.Values{}
	body.Add("cmd", cmd.Cmd)
	body.Add("body", cmd.Body)
	req, err := http.NewRequest("GET", "/config?"+body.Encode(), nil)
	if typ == "POST" {
		req, err = http.NewRequest("POST", "/config", strings.NewReader(body.Encode()))
	}
	test.Assert(t, err == nil, "Failed: %v", err)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	res := httptest.NewRecorder()
	configCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK: code=%v", res.Code)

	return res
}

/*
func configRequest(cmd configReq, code int, t *testing.T) {
	configJSONRequest(cmd, code, t)
	configFormRequest("POST", cmd, code, t)
	configFormRequest("GET", cmd, code, t)
}
*/

func TestConfigCommands(t *testing.T) {
	c := config.Get()
	configsTableInit(t)
	defer func() {
		configsTableInit(t)
		err := config.Set(&c.AppConfigODS)
		log.E(err)
	}()
	get := configReq{
		Cmd: "get",
	}
	resp := configFormRequest("GET", get, http.StatusOK, t)
	g := &config.AppConfigODS{}
	err := yaml.Unmarshal(resp.Body.Bytes(), &g)
	test.CheckFail(err, t)
	test.Assert(t, g.LogType == "zap", "expected default log_type is zap. got %v", g.LogType)
	g.LogType = "otherlogtype"
	b, err := yaml.Marshal(g)
	test.CheckFail(err, t)
	set := configReq{
		Cmd:  "set",
		Body: string(b),
	}
	configFormRequest("POST", set, http.StatusOK, t)
	g.LogType = ""
	resp = configFormRequest("GET", get, http.StatusOK, t)
	err = yaml.Unmarshal(resp.Body.Bytes(), &g)
	test.CheckFail(err, t)
	test.Assert(t, g.LogType == "otherlogtype", "expected default log_type is uber")
}
