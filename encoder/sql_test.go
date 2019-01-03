package encoder

import (
	"database/sql"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

const (
	testsvc   = "sqlenctestsvc"
	testdb    = "sqlenctestdb"
	testdbout = "sqlenctestdbout"
	testclst  = "sqlenctestclst"
	testtbl   = "sqlencodertest"
)

func prepareTestSQLTopics(t *testing.T, outType string, sqlType string) *sql.DB {
	ci, err := db.GetConnInfo(&db.Loc{Cluster: testclst, Service: testsvc}, db.Slave, outType)
	test.CheckFail(err, t)

	sqlMode := db.SQLMode
	if sqlType == "ansisql" {
		sqlMode += ",ANSI_QUOTES"
	}

	conn, err := db.OpenModeType(ci, "mysql", sqlMode)
	test.CheckFail(err, t)

	test.ExecSQL(conn, t, "DROP DATABASE IF EXISTS "+testdb)
	test.ExecSQL(conn, t, "DROP DATABASE IF EXISTS "+testdbout)
	test.ExecSQL(conn, t, "CREATE DATABASE "+testdb)
	test.ExecSQL(conn, t, "CREATE DATABASE "+testdbout)

	err = conn.Close()
	test.CheckFail(err, t)

	ci, err = db.GetConnInfo(&db.Loc{Cluster: testclst, Service: testsvc, Name: testdb}, db.Slave, outType)
	test.CheckFail(err, t)

	conn, err = db.OpenModeType(ci, "mysql", sqlMode)
	test.CheckFail(err, t)

	test.ExecSQL(conn, t, "CREATE TABLE "+testtbl+" (f1 int, f2 varchar(20), f3 float, primary key(f1,f2))")

	err = conn.Close()
	test.CheckFail(err, t)

	ci, err = db.GetConnInfo(&db.Loc{Cluster: testclst, Service: testsvc, Name: testdbout}, db.Slave, outType)
	test.CheckFail(err, t)

	conn, err = db.OpenModeType(ci, "mysql", sqlMode)
	test.CheckFail(err, t)

	return conn
}

func testSQLEncoderBasic(t *testing.T, outType string, sqlType string, idempotent bool) {
	conn := prepareTestSQLTopics(t, outType, sqlType)
	defer func() { log.E(conn.Close()) }()

	b := state.RegisterTableInState(&db.Loc{Service: testsvc, Cluster: testclst, Name: testdb}, testtbl, outType, outType, 0, sqlType, "", 0)
	require.True(t, b)

	sqlEnc, err := Create(sqlType, testsvc, testdb, testtbl, outType, outType, 0)
	test.CheckFail(err, t)

	if idempotent {
		sqlEnc.(*sqlEncoder).idempotentInsert = true
	}

	s, err := sqlEnc.EncodeSchema(1)
	test.CheckFail(err, t)
	test.ExecSQL(conn, t, string(s))

	for i := 0; i < 10; i++ {
		s, err := sqlEnc.CommonFormat(&types.CommonFormatEvent{Type: "insert", SeqNo: uint64(100 + i), Key: []interface{}{i, strconv.Itoa(i + 1)}, Fields: &[]types.CommonFormatField{{Name: "f1", Value: i}, {Name: "f2", Value: strconv.Itoa(i + 1)}, {Name: "f3", Value: float64(i + 2)}}})
		test.CheckFail(err, t)
		test.ExecSQL(conn, t, string(s))
	}

	if idempotent {
		// This inserts should be ignored and values should stay intact
		for i := 0; i < 10; i++ {
			s, err := sqlEnc.CommonFormat(&types.CommonFormatEvent{Type: "insert", SeqNo: uint64(100 + i), Key: []interface{}{i, strconv.Itoa(i + 1)}, Fields: &[]types.CommonFormatField{{Name: "f1", Value: i}, {Name: "f2", Value: strconv.Itoa(i + 1)}, {Name: "f3", Value: float64(10000 + i + 2)}}})
			test.CheckFail(err, t)
			test.ExecSQL(conn, t, string(s))
		}

		//This inserts have higher sequence numbers and should replace original
		//rows
		for i := 0; i < 5; i++ {
			s, err := sqlEnc.CommonFormat(&types.CommonFormatEvent{Type: "insert", SeqNo: uint64(200 + i), Key: []interface{}{i, strconv.Itoa(i + 1)}, Fields: &[]types.CommonFormatField{{Name: "f1", Value: i}, {Name: "f2", Value: strconv.Itoa(i + 1)}, {Name: "f3", Value: float64(20000 + i + 2)}}})
			test.CheckFail(err, t)
			test.ExecSQL(conn, t, string(s))
		}
	}

	rows, err := util.QuerySQL(conn, "SELECT * from "+testtbl)
	test.CheckFail(err, t)

	var i, f1 int
	var f2 string
	var f3 float64
	var seqno int64
	for rows.Next() {
		err := rows.Scan(&seqno, &f1, &f2, &f3)
		test.CheckFail(err, t)
		require.Equal(t, i, f1)
		require.Equal(t, strconv.Itoa(i+1), f2)
		if idempotent && i < 5 {
			require.Equal(t, int64(i+200), seqno)
			require.Equal(t, float64(i+2+20000), f3)
		} else {
			require.Equal(t, int64(i+100), seqno)
			require.Equal(t, float64(i+2), f3)
		}
		i++
	}
	test.CheckFail(rows.Err(), t)
	test.CheckFail(rows.Close(), t)

	for i := 0; i < 10; i++ {
		sn := uint64(100 + i)
		if idempotent && i < 5 {
			sn = uint64(200 + i)
		}
		s, err := sqlEnc.CommonFormat(&types.CommonFormatEvent{Type: "delete", SeqNo: sn, Key: []interface{}{int64(i), strconv.Itoa(i + 1)}, Fields: nil})
		test.CheckFail(err, t)
		test.ExecSQL(conn, t, string(s))
	}
	r := 100
	err = util.QueryRowSQL(conn, "SELECT COUNT(*) from "+testtbl).Scan(&r)
	test.CheckFail(err, t)
	require.Equal(t, 0, r)
}

func TestSQLEncoderBasic(t *testing.T) {
	testSQLEncoderBasic(t, types.InputMySQL, "mysql", false)
}

func TestSQLEncoderAnsi(t *testing.T) {
	testSQLEncoderBasic(t, types.InputMySQL, "ansisql", false)
}

func TestSQLEncoderIdempotent(t *testing.T) {
	testSQLEncoderBasic(t, types.InputMySQL, "ansisql", true)
}
