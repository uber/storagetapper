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

package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/test"
)

//Structure for input of TestConnectInfoGet test
type testConnectInfoGetInput struct {
	clusterName string
	connType    db.ConnectionType
}

func TestConnectInfoGet(t *testing.T) {
	tests := []struct {
		name   string
		input  testConnectInfoGetInput
		output *db.Addr
	}{
		{
			"Test row exists and master type",
			testConnectInfoGetInput{
				"test_cluster_db1",
				db.Master,
			},
			&db.Addr{
				Name: "test_cluster_db1",
				Host: "MasterHost",
				Port: 123,
				User: "maximus",
				Pwd:  "pass123",
				DB:   "test_db",
			},
		},
		{
			"Test row exists and slave type",
			testConnectInfoGetInput{
				"test_cluster_db1",
				db.Slave,
			},
			&db.Addr{
				Name: "test_cluster_db1",
				Host: "SlaveHost",
				Port: 123,
				User: "maximus",
				Pwd:  "pass123",
				DB:   "test_db",
			},
		},
		{
			"Test row exists and query for slave but only master type exists",
			testConnectInfoGetInput{
				"test_cluster_db2",
				db.Slave,
			},
			&db.Addr{
				Name: "test_cluster_db2",
				Host: "MasterHost",
				Port: 123,
				User: "maximus",
				Pwd:  "pass123",
				DB:   "test_db",
			},
		},
		{
			"Test row exists and query for master but only slave type exists",
			testConnectInfoGetInput{
				"test_cluster_db3",
				db.Master,
			},
			&db.Addr{
				Name: "test_cluster_db3",
				Host: "SlaveHost",
				Port: 123,
				User: "maximus",
				Pwd:  "pass123",
				DB:   "test_db",
			},
		},
		{
			"Test no row exists",
			testConnectInfoGetInput{
				"test_cluster_db4",
				db.Master,
			},
			nil,
		},
	}

	resetState(t)

	//Schema(name, host, port, user, password, type)
	test.ExecSQL(mgr.conn, t, `INSERT INTO clusters(name, host, port, user, password, type) values(?,?,?,?,?,?)`, "test_cluster_db1", "MasterHost", 123, "maximus", "pass123", db.Master.String())
	test.ExecSQL(mgr.conn, t, `INSERT INTO clusters(name, host, port, user, password, type) values(?,?,?,?,?,?)`, "test_cluster_db1", "SlaveHost", 123, "maximus", "pass123", db.Slave.String())
	test.ExecSQL(mgr.conn, t, `INSERT INTO clusters(name, host, port, user, password, type) values(?,?,?,?,?,?)`, "test_cluster_db2", "MasterHost", 123, "maximus", "pass123", db.Master.String())
	test.ExecSQL(mgr.conn, t, `INSERT INTO clusters(name, host, port, user, password, type) values(?,?,?,?,?,?)`, "test_cluster_db3", "SlaveHost", 123, "maximus", "pass123", db.Slave.String())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &db.Loc{Service: "test_svc1", Name: "test_db", Cluster: tt.input.clusterName}
			dbInfo, _ := ConnectInfoGet(l, tt.input.connType)
			if tt.output == nil {
				assert.Equal(t, dbInfo, tt.output)
			} else {
				assert.Equal(t, dbInfo.Host, tt.output.Host)
				assert.Equal(t, dbInfo.Name, tt.output.Name)
			}
		})
	}
}
