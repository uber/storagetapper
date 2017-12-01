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
	"net/http"
	"net/http/httptest"
	"testing"

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

func clusterRequest(cmd clusterInfoReq, code int, t *testing.T) *httptest.ResponseRecorder {
	body, _ := json.Marshal(cmd)
	req, err := http.NewRequest("POST", "/cluster", bytes.NewReader(body))
	test.Assert(t, err == nil, "Failed: %v", err)
	res := httptest.NewRecorder()
	clusterInfoCmd(res, req)
	test.Assert(t, res.Code == code, "Not OK")
	return res
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

func TestClusterInfoNegative(t *testing.T) {
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
	test.Assert(t, res.Code == http.StatusInternalServerError, "Not OK")
}
