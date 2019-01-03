package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/types"
)

func TestGetCurrentGTID(t *testing.T) {
	addr := &Addr{Host: "localhost", Port: 3306, User: types.TestMySQLUser, Pwd: types.TestMySQLPassword}
	d, err := Open(addr)
	if err != nil {
		t.Skip("No MySQL connection")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close failed")
	}

	gtid, err := GetCurrentGTID(addr)
	require.NoError(t, err)
	require.NotEmpty(t, gtid)
}

func TestGetCurrentGTIDForDB(t *testing.T) {
	addr := &Addr{Host: "localhost", Port: 3306, User: types.TestMySQLUser, Pwd: types.TestMySQLPassword}
	d, err := Open(addr)
	if err != nil {
		t.Skip("No MySQL connection")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close failed")
	}

	//FIXME: Set correct user and password so the it can connect to MySQL and
	//return correct gtid set
	gtid, err := GetCurrentGTIDForDB(&Loc{Service: "test_svc1", Cluster: "test_cluster3", Name: "test_db2"}, "mysql")
	require.Error(t, err)
	require.Empty(t, gtid)
}
