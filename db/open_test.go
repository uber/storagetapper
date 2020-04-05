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

package db

import (
	"os"
	"testing"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/types"

	"github.com/stretchr/testify/require"
)

var cfg *config.AppConfig

func connectInfoGetForTest(l *Loc, connType ConnectionType) (*Addr, error) {
	return &Addr{Host: "localhost", Port: 3306, User: types.TestMySQLUser, Pwd: types.TestMySQLPassword, DB: l.Name}, nil
}

func TestBasic(t *testing.T) {
	d, err := Open(&Addr{Host: "localhost", Port: 3306, User: types.TestMySQLUser, Pwd: types.TestMySQLPassword})
	if err != nil {
		t.Skip("No MySQL connection")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close failed")
	}

	_, err = Open(&Addr{Host: "localhost", Port: 3307, User: "root"})
	if err == nil {
		t.Fatalf("Open not failed")
	}

	_, err = OpenService(&Loc{Service: "test_svc1", Name: "test_db1"}, "", types.InputMySQL)
	if err == nil {
		t.Fatalf("OpenService not failed")
	}

	_, err = OpenService(&Loc{Service: "test_svc1", Name: "test_db2"}, "subst", types.InputMySQL)
	if err == nil {
		t.Fatalf("OpenService not failed")
	}
}

func TestGetEnumerator(t *testing.T) {
	// First test that GetEnumerator fails when service name is not provided
	_, err := GetEnumerator("", "storagetapper", "test_db2", "test_table1", "mysql")
	require.Error(t, err)

	// Next we check that GetEnumerator fails for invalid input type
	_, err = GetEnumerator("test_svc1", "test_cluster1", "test_db2", "test_table1", "unknown")
	require.Error(t, err)

	// Let's check correct flow now
	e, err := GetEnumerator("test_svc1", "test_cluster1", "test_db2", "test_table1", "mysql")
	require.NoError(t, err)
	require.NotNil(t, e)

	// Test that enumerator returns expected number of items
	var locs []*Loc
	for e.Next() {
		loc := e.Value()
		locs = append(locs, loc)
		require.Equal(t, "test_svc1", loc.Service)
		require.Equal(t, "test_db2", loc.Name)
	}

	require.Equal(t, 1, len(locs))

	// Now test resetting of enumerator
	e.Reset()
	for e.Next() {
		loc := e.Value()
		require.Equal(t, "test_svc1", loc.Service)
		require.Equal(t, "test_db2", loc.Name)
	}

	//Constructing with empty cluster name
	e, err = GetEnumerator("test_svc1", "test_clst1", "test_db2", "test_table1", "mysql")
	require.NoError(t, err)
	require.NotNil(t, e)

	locs = make([]*Loc, 0)
	for e.Next() {
		loc := e.Value()
		locs = append(locs, loc)
		require.Equal(t, "test_svc1", loc.Service)
		require.Equal(t, "test_db2", loc.Name)
	}

	require.Equal(t, 1, len(locs))
}

func TestMain(m *testing.M) {
	cfg = config.Get()

	log.Configure(cfg.LogType, cfg.LogLevel, config.Environment() == config.EnvProduction)

	BuiltinResolveCluster = connectInfoGetForTest

	err := metrics.Init()
	log.F(err)

	os.Exit(m.Run())
}
