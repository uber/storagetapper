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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

func clustersTableInit(t *testing.T) {
	conn := state.ConnectLow(cfg, true)
	if conn == nil {
		t.FailNow()
	}
	_, err := conn.Exec("TRUNCATE TABLE " + types.MyDbName + ".clusters")
	test.CheckFail(err, t)
	err = conn.Close()
	test.CheckFail(err, t)
}

func clusterJsonRequest(cmd clusterInfoReq, code int, t *testing.T) {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest("POST", "/cluster", bytes.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	clusterInfoCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK: code=%v", res.Code)
}

func clusterFormRequest(typ string, cmd clusterInfoReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body := url.Values{}
	body.Add("cmd", cmd.Cmd)
	body.Add("name", cmd.Name)
	if cmd.Name != "" && cmd.Cmd != "list" {
		body.Set("name", cmd.Name+"_"+typ)
	}
	body.Add("host", cmd.Host)
	body.Add("port", fmt.Sprintf("%v", cmd.Port))
	body.Add("offset", fmt.Sprintf("%v", cmd.Offset))
	body.Add("limit", fmt.Sprintf("%v", cmd.Limit))
	body.Add("user", cmd.User)
	body.Add("pw", cmd.Pw)
	req, err := http.NewRequest("GET", "/cluster?"+body.Encode(), nil)
	if typ == "POST" {
		req, err = http.NewRequest("POST", "/cluster", strings.NewReader(body.Encode()))
	}
	test.Assert(t, err == nil, "Failed: %v", err)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	res := httptest.NewRecorder()
	clusterInfoCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK: code=%v", res.Code)

	return res
}

func clusterGETRequest(cmd clusterInfoReq, code int, t *testing.T) {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest("POST", "/cluster", bytes.NewReader(body))
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	clusterInfoCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK: code=%v", res.Code)
}

func clusterRequest(cmd clusterInfoReq, code int, t *testing.T) {
	clusterJsonRequest(cmd, code, t)
	clusterFormRequest("POST", cmd, code, t)
	clusterFormRequest("GET", cmd, code, t)
}

func TestClusterInfoAddDelCommands(t *testing.T) {
	clustersTableInit(t)

	add := clusterInfoReq{
		Cmd:  "add",
		Name: "test_cluster_name1",
		Host: "localhost",
		Port: 0,
		User: "ttt_usr1",
		Pw:   "ttt_pwd1",
	}

	clusterRequest(add, http.StatusOK, t)
	clusterRequest(add, http.StatusInternalServerError, t)

	del := clusterInfoReq{
		Cmd:  "del",
		Name: "test_cluster_name1",
	}

	clusterRequest(del, http.StatusOK, t)
	clusterRequest(del, http.StatusOK, t)
}

func TestClusterInfoListCommands(t *testing.T) {
	clustersTableInit(t)

	addrs := []db.Addr{
		db.Addr{Name: "clst1", Host: "host1", Port: 1, User: "user1"},
		db.Addr{Name: "clst2", Host: "host2", Port: 2, User: "user2"},
		db.Addr{Name: "clst3", Host: "host3", Port: 3, User: "user3"},
		db.Addr{Name: "clst4", Host: "host4", Port: 4, User: "user4"},
	}
	for _, v := range addrs {
		state.InsertClusterInfo(v.Name, &v)
	}

	req := clusterInfoReq{
		Cmd: "list",
	}

	resp := clusterFormRequest("GET", req, http.StatusOK, t)
	raddrs := strings.Split(resp.Body.String(), "\n")
	if len(addrs) != len(raddrs)-1 {
		t.Fatal("Should return all 4 the rows")
	}
	for i, v := range raddrs {
		if v == "" {
			continue
		}
		var a db.Addr
		log.Debugf("%v", v)
		err := json.Unmarshal([]byte(v), &a)
		test.CheckFail(err, t)
		if !reflect.DeepEqual(a, addrs[i]) {
			t.Fatal("%v: %v != %v", i, a, addrs[i])
		}
	}

	for i := range addrs {
		req := clusterInfoReq{
			Cmd: "list",
		}
		switch i {
		case 0:
			req.Name = "clst1"
		case 1:
			req.Host = "host2"
		case 2:
			req.Port = 3
		case 3:
			req.User = "user4"
		}
		resp := clusterFormRequest("GET", req, http.StatusOK, t)
		b, err := json.Marshal(addrs[i])
		test.CheckFail(err, t)
		if string(b)+"\n" != resp.Body.String() {
			t.Fatalf("Should return 1 record at index %v. got: %v", i, resp.Body.String())
		}
	}

	req = clusterInfoReq{
		Cmd:    "list",
		Offset: 2,
	}
	resp = clusterFormRequest("GET", req, http.StatusOK, t)
	b, err := json.Marshal(addrs[2])
	test.CheckFail(err, t)
	rec := string(b) + "\n"
	b, err = json.Marshal(addrs[3])
	test.CheckFail(err, t)
	rec += string(b) + "\n"

	if rec != resp.Body.String() {
		t.Fatalf("Should return 2 records starting from index 2. got: %v", resp.Body.String())
	}

	req = clusterInfoReq{
		Cmd:   "list",
		Limit: 2,
	}
	resp = clusterFormRequest("GET", req, http.StatusOK, t)
	b, err = json.Marshal(addrs[0])
	test.CheckFail(err, t)
	rec = string(b) + "\n"
	b, err = json.Marshal(addrs[1])
	test.CheckFail(err, t)
	rec += string(b) + "\n"

	if rec != resp.Body.String() {
		t.Fatalf("Should return 2 records starting from index 0. got: %v", resp.Body.String())
	}

	req = clusterInfoReq{
		Cmd:    "list",
		Offset: 1,
		Limit:  2,
	}
	resp = clusterFormRequest("GET", req, http.StatusOK, t)
	b, err = json.Marshal(addrs[1])
	test.CheckFail(err, t)
	rec = string(b) + "\n"
	b, err = json.Marshal(addrs[2])
	test.CheckFail(err, t)
	rec += string(b) + "\n"

	if rec != resp.Body.String() {
		t.Fatalf("Should return 2 records starting from index 1. got: %v", resp.Body.String())
	}
}

func TestClusterInfoCmdInvalidInput(t *testing.T) {
	clustersTableInit(t)
	add := clusterInfoReq{
		Cmd:  "add",
		Name: "test_cluster_name1",
		Host: "localhost",
		Port: 0,
		User: "ttt_usr1",
		Pw:   "ttt_pwd1",
	}

	add.User = ""
	clusterRequest(add, http.StatusInternalServerError, t)

	add.User = "ttt_user1"
	add.Host = ""
	clusterRequest(add, http.StatusInternalServerError, t)

	add.Host = "localhost"
	add.Name = ""
	clusterRequest(add, http.StatusInternalServerError, t)

	add.Name = "test_cluster_name2"
	add.Cmd = "update"
	clusterRequest(add, http.StatusInternalServerError, t)

	req, err := http.NewRequest("POST", "/cluster", bytes.NewReader([]byte("this is supposed to be garbage formatted json")))
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	clusterInfoCmd(res, req)
	test.Assert(t, res.Code == http.StatusUnsupportedMediaType, "Not OK: code=%v", res.Code)

	req.Header.Add("Content-Type", "application/json")
	res = httptest.NewRecorder()
	clusterInfoCmd(res, req)
	test.Assert(t, res.Code == http.StatusInternalServerError, "Not OK: code=%v", res.Code)
}
