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
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var cfg *config.AppConfig

func checkStateEqual(m1 Type, m2 Type) bool {
	return reflect.DeepEqual(m1, m2)
}

var refST1 = Type{
	{1, "clst1", "svc1", "db1_state", "table1", "", "", "", 0, "", "", true},
	{2, "clst1", "svc1", "db1_state", "table2", "", "", "", 0, "", "", true},
	{3, "clst1", "svc1", "db2_state", "table1", "", "", "", 0, "", "", true},
	{4, "clst1", "svc2", "db3_state", "table1", "", "", "", 0, "", "", true},
	{5, "clst2", "svc2", "db2_state", "table1", "", "", "", 0, "", "", true},
}

var refST2 = Type{
	{6, "clst1", "svc1", "db1_state", "table3", "", "", "", 0, "", "", true},
	{7, "clst1", "svc2", "db2_state", "table2", "", "", "", 0, "", "", true},
}

var refST3 = Type{
	{5, "clst2", "svc2", "db2_state", "table1", "", "", "", 0, "", "", true},
}

func insertStateRows(s Type, t1 *testing.T) {
	var i int64
	for _, t := range s {
		if i == 0 {
			i = t.ID
		} else {
			i++
		}
		if i != t.ID {
			log.Errorf("service and dbs need to be sorted in reference struture")
			t1.FailNow()
		}
		err := util.ExecSQL(conn, "INSERT INTO state(service,cluster,db,tableName,gtid,input,output,rawSchema,schemaGTID) VALUES (?,?,?,?,?,'','','','')", t.Service, t.Cluster, t.Db, t.Table, t.Gtid)
		test.CheckFail(err, t1)
	}
}

func readStateCond(cond string) (Type, error) {
	rows, err := util.QuerySQL(conn, "SELECT * FROM state WHERE "+cond)
	if err != nil {
		return nil, err
	}
	return parseRows(rows)
}

func initState(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	conn := ConnectLow(cfg, true)
	if conn == nil {
		t.Fatal("Failed to connect to db")
	}
	_, err := conn.Exec("DROP DATABASE IF EXISTS " + types.MyDbName)
	test.CheckFail(err, t)
	log.E(conn.Close())

	if !Init(cfg) {
		t.Fatal("Failed to initialize")
	}
}

func TestGetCount(t *testing.T) {
	initState(t)

	c, err := GetCount()
	test.CheckFail(err, t)
	if c != 0 {
		t.Fatalf("There should be nothing in the state yet")
	}

	insertStateRows(refST1, t)

	c, err = GetCount()
	test.CheckFail(err, t)
	if c != 5 {
		t.Fatalf("There should be %v rows from the refST1", len(refST1))
	}
}

func TestGetTable(t *testing.T) {
	initState(t)

	c, err := GetTableByID(3)
	test.CheckFail(err, t)
	if len(c) != 0 {
		t.Fatalf("There should be nothing in the state yet")
	}

	insertStateRows(refST1, t)

	c, err = GetTableByID(3)
	test.CheckFail(err, t)
	if len(c) != 1 {
		t.Fatalf("There should be exactly one record for the table")
	}
	if !reflect.DeepEqual(c[0], refST1[2]) {
		log.Errorf("Reference: %v", refST1[2])
		log.Errorf("Read     : %v", c[0])
		t.FailNow()
	}
}

func testGTIDs(t *testing.T) {
	log.Debugf("Check GTID handling")

	dbloc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	gts, err := GetGTID(dbloc1)
	test.CheckFail(err, t)
	if gts != "" {
		t.Fatalf(gts)
	}

	exgts := `0b59c905-b790-11e5-ba7b-a0369f7a1a38:1-55209742,
	160c927a-b0bc-11e6-9350-2c600cd7b2df:1-268964356,
	607d16a2-e503-11e5-a2e0-a0369f7763e8:1-57531690,
	871a5fc8-aff2-11e6-8e2d-2c600ccd8ebf:1-53392746,
	a7c7f66d-1f35-11e6-9e5e-a0369f776224:1-724441547,
	c4ba8574-004d-11e5-8f81-2c600c7300c2:1-227694298`

	_, err = mysql.ParseGTIDSet("mysql", exgts)
	test.CheckFail(err, t)

	err = SetGTID(dbloc1, exgts)
	test.CheckFail(err, t)

	gts, err = GetGTID(dbloc1)
	test.CheckFail(err, t)

	if gts != exgts {
		t.Fatalf(gts)
	}

	log.Debugf("Check that StateGetGTID return non-empty GTID")
	err = util.ExecSQL(conn, "INSERT INTO state(service,cluster,db,tableName,gtid,input,output,rawSchema,schemaGTID) VALUES ('svc1', 'clst1', 'db1_state', 'table5', '', '','','','')")
	test.CheckFail(err, t)
	err = util.ExecSQL(conn, "INSERT INTO state(service,cluster,db,tableName,gtid,input,output,rawSchema,schemaGTID) VALUES ('svc1', 'clst1', 'db1_state', 'table0', '', '','','','')")
	test.CheckFail(err, t)

	gts, err = GetGTID(dbloc1)
	test.CheckFail(err, t)

	if gts != exgts {
		t.Fatalf(gts)
	}

	log.Debugf("Check if we can read back data of particular cluster and gtid of it is not affected by above SetGTID for clst1")
	st3, err := GetForCluster("clst2")
	test.CheckFail(err, t)

	if !checkStateEqual(refST3, st3) {
		log.Errorf("Reference: %v", refST3)
		log.Errorf("Read     : %v", st3)
		t.FailNow()
	}

	gts1 := "c4ba8574-004d-11e5-8f81-2c600c7300c2:1-227694298"
	var seqno1 uint64 = 282823744
	err = SaveBinlogState(dbloc1, gts1, seqno1)
	test.CheckFail(err, t)
	st4, err := readStateCond("service='svc1' and db='db1_state' limit 1")
	test.CheckFail(err, t)

	if st4[0].Gtid != gts1 || st4[0].SeqNo != seqno1 {
		log.Errorf("Expected: %v, %v", gts1, seqno1)
		log.Errorf("Got: %v, %v %+v", st4[0].Gtid, st4[0].SeqNo, st4)
		t.FailNow()
	}
}

func TestStateBasic(t *testing.T) {
	initState(t)

	v1 := GetVersion()

	insertStateRows(refST1, t)

	v6 := GetVersion()
	log.Debugf("State version: %v", v1)
	log.Debugf("State version: %v", v6)
	if v1 != (int64(1)<<24) || v6 != (int64(6)<<24)|int64(5) {
		t.Fatalf("State version: %v %v", v6, v1)
	}

	log.Debugf("Check table registration")

	reg, err := TableRegistered(0)
	test.CheckFail(err, t)
	test.Assert(t, !reg, "Table with 0 id should never be registered")

	for i := 1; i <= len(refST1); i++ {
		if reg, err = TableRegistered(int64(i)); err != nil || !reg {
			log.Errorf("Table with id=%v isn't registered", i)
			test.CheckFail(err, t)
		}
	}

	reg, err = TableRegistered(6)
	test.CheckFail(err, t)
	if reg {
		t.Fatalf("Table with id=%v is registered", 6)
	}

	log.Debugf("Check that state is equal to what we wrote earlier")

	st1, err := Get()
	test.CheckFail(err, t)

	if !checkStateEqual(refST1, st1) {
		log.Errorf("Reference: %v", refST1)
		log.Errorf("Read     : %v", st1)
		t.FailNow()
	}

	log.Debugf("Check state update")

	insertStateRows(refST2, t)

	v8 := GetVersion()
	if v8 != int64(8)<<24|int64(7) {
		t.Fatalf("V8")
	}
	log.Debugf("State version=%v", v8)

	log.Debugf("Check table deregistration")

	/* Deregistration doesn't change state version */
	err = util.ExecSQL(conn, "DELETE FROM state WHERE id=4")
	test.CheckFail(err, t)

	reg, err = TableRegistered(4)
	test.CheckFail(err, t)

	if reg {
		t.Fatalf("reg")
	}

	testGTIDs(t)

	err = Close()
	test.CheckFail(err, t)
}

func TestStateSchema(t *testing.T) {
	initState(t)

	dbloc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	err := util.ExecSQL(conn, `DROP DATABASE IF EXISTS db1_state`)
	test.CheckFail(err, t)
	err = util.ExecSQL(conn, `CREATE DATABASE IF NOT EXISTS db1_state`)
	test.CheckFail(err, t)

	err = util.ExecSQL(conn, `CREATE TABLE db1_state.SCHEMA_TEST1 (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)
	test.CheckFail(err, t)

	refs := types.TableSchema{
		DBName:    "db1_state",
		TableName: "SCHEMA_TEST1",
		Columns: []types.ColumnSchema{
			{
				Name:                   "field1",
				OrdinalPosition:        1,
				IsNullable:             "NO",
				DataType:               "bigint",
				CharacterMaximumLength: sql.NullInt64{Int64: 0, Valid: false},
				NumericPrecision:       sql.NullInt64{Int64: 19, Valid: true},
				NumericScale:           sql.NullInt64{Int64: 0, Valid: true},
				Type:                   "bigint(20)",
				Key:                    "PRI",
			},
			{
				Name:                   "field2",
				OrdinalPosition:        2,
				IsNullable:             "NO",
				DataType:               "bigint",
				CharacterMaximumLength: sql.NullInt64{Int64: 0, Valid: false},
				NumericPrecision:       sql.NullInt64{Int64: 19, Valid: true},
				NumericScale:           sql.NullInt64{Int64: 0, Valid: true},
				Type:                   "bigint(20)",
				Key:                    "MUL",
			},
		},
	}

	/* Negative test. Non existent table */
	ss, err := schema.Get(dbloc1, "SCHEMA_TEST2")
	if err == nil {
		t.Fatalf("Should fail with error, Got some schema instead: %v", ss)
	}

	ts, err := schema.Get(dbloc1, "SCHEMA_TEST1")
	test.CheckFail(err, t)

	if !reflect.DeepEqual(&refs, ts) {
		log.Debugf("Expected: %+v", &refs)
		log.Debugf("Received: %+v", ts)
		t.FailNow()
	}

	tsRaw, err := schema.GetRaw(dbloc1, "`db1_state`.`SCHEMA_TEST1`")
	test.CheckFail(err, t)

	tsRawRef := "(\n" +
		"  `field1` bigint(20) NOT NULL,\n" +
		"  `field2` bigint(20) NOT NULL DEFAULT '0',\n" +
		"  PRIMARY KEY (`field1`),\n" +
		"  UNIQUE KEY `field2` (`field2`,`field1`)\n" +
		") ENGINE=InnoDB"
	if !strings.HasPrefix(tsRaw, tsRawRef) {
		log.Errorf("Unexpected raw schema: '%+v'", tsRaw)
		log.Errorf("Reference raw schema: '%+v'", tsRawRef)
		t.FailNow()
	}

	_, err = GetSchema("svc1", "db1_state", "SCHEMA_TEST1")
	if err == nil || reflect.TypeOf(err).String() != "*schema.ErrNoTable" {
		log.Errorf("Table shouldn't exist in the state yet")
		if err != nil {
			log.Errorf("Got wrong error instead: %v", err.Error())
		}
		t.FailNow()
	}

	sgtid, err := GetCurrentGTIDForDB(dbloc1)
	if log.E(err) {
		t.FailNow()
	}

	if !ReplaceSchema("svc1", "clst1", ts, tsRaw, "", sgtid, "", "") {
		t.Fatalf("%+v %+v", ts, err)
	}

	ts1, err := GetSchema("svc1", "db1_state", "SCHEMA_TEST1")
	test.CheckFail(err, t)

	if !reflect.DeepEqual(&refs, ts1) {
		log.Errorf("Expected: %+v", &refs)
		log.Errorf("Received: %+v", ts1)
		t.FailNow()
	}

	/* Delete last column from ts */
	//	ts.Columns = append([]types.ColumnSchema{}, ts.Columns[0:1]...)

	sgtid1, err := GetCurrentGTIDForDB(dbloc1)
	if log.E(err) {
		t.FailNow()
	}

	if !schema.MutateTable(nodbconn, "svc1", "db1_state", "SCHEMA_TEST1", "drop field2", ts, &tsRaw) {
		t.FailNow()
	}
	log.Debugf("New schema: %+v", ts)
	log.Debugf("New raw schema: %+v", tsRaw)

	if !ReplaceSchema("svc1", "clst1", ts, tsRaw, sgtid, sgtid1, "", "") {
		t.FailNow()
	}

	sts, err := GetSchema("svc1", "db1_state", "SCHEMA_TEST1")
	test.CheckFail(err, t)

	refs1 := types.TableSchema{
		DBName:    "db1_state",
		TableName: "SCHEMA_TEST1",
		Columns: []types.ColumnSchema{{
			Name:                   "field1",
			OrdinalPosition:        1,
			IsNullable:             "NO",
			DataType:               "bigint",
			CharacterMaximumLength: sql.NullInt64{Int64: 0, Valid: false},
			NumericPrecision:       sql.NullInt64{Int64: 19, Valid: true},
			NumericScale:           sql.NullInt64{Int64: 0, Valid: true},
			Type:                   "bigint(20)",
			Key:                    "PRI",
		}},
	}

	if !reflect.DeepEqual(&refs1, sts) {
		log.Errorf("Expected: %+v", &refs1)
		log.Errorf("Received: %+v", ts)
		t.FailNow()
	}

	err = Close()
	test.CheckFail(err, t)
}

func TestTableRegister(t *testing.T) {
	initState(t)

	err := util.ExecSQL(conn, `DROP DATABASE IF EXISTS db1_state`)
	test.CheckFail(err, t)
	err = util.ExecSQL(conn, `CREATE DATABASE IF NOT EXISTS db1_state`)
	test.CheckFail(err, t)

	err = util.ExecSQL(conn, `CREATE TABLE db1_state.REG_SCHEMA_TEST1 (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)
	test.CheckFail(err, t)

	/*
		TODO: Check schema of registered table
			refs := types.TableSchema{"db1_state", "REG_SCHEMA_TEST1", []types.ColumnSchema{{"field1", 1, "NO", "bigint", sql.NullInt64{0, false}, sql.NullInt64{19, true}, sql.NullInt64{0, true}, "bigint(20)", "PRI"},
				{"field2", 2, "NO", "bigint", sql.NullInt64{0, false}, sql.NullInt64{19, true}, sql.NullInt64{0, true}, "bigint(20)", "MUL"}}}
	*/
	dbloc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	if !RegisterTable(dbloc1, "REG_SCHEMA_TEST1", "", "") {
		t.Fatalf("Fail register db1_state 1")
	}

	if !RegisterTable(dbloc1, "REG_SCHEMA_TEST1", "", "") {
		t.Fatalf("Fail register db1_state 2")
	}

	reg, err := TableRegistered(1)
	if log.E(err) || !reg {
		t.Fatalf("Table was registered")
	}

	if !DeregisterTable("svc1", "db1_state", "REG_SCHEMA_TEST1") {
		log.Debugf("Fail deregister db1_state 1")
		log.Debugf("3asdf")
		t.FailNow()
	}

	if !DeregisterTable("svc1", "db1_state", "REG_SCHEMA_TEST1") {
		t.Fatalf("Fail deregister db1_state 2")
	}

	err = Close()
	test.CheckFail(err, t)
}

func TestClusterInfo(t *testing.T) {
	initState(t)
	refa := db.Addr{Host: "state_test_host1", Port: 1234, User: "state_test_user1", Pwd: "state_test_pw1", Db: "state_test_db1"}
	err := InsertClusterInfo("state_test_cluster1", &refa)
	test.CheckFail(err, t)
	err = InsertClusterInfo("state_test_cluster2", &db.Addr{Host: "state_test_host1", Port: 1234, User: "state_test_user1", Pwd: "state_test_pw1", Db: "state_test_db1"})
	test.CheckFail(err, t)
	err = InsertClusterInfo("state_test_cluster1", &db.Addr{Host: "state_test_host1", Port: 1234, User: "state_test_user1", Pwd: "state_test_pw1", Db: "state_test_db1"})
	if err == nil {
		t.FailNow()
	}
	a := ConnectInfoGet(&db.Loc{Cluster: "state_test_cluster1", Name: "state_test_db1"}, db.Master)
	if a == nil || !reflect.DeepEqual(a, &refa) {
		log.Errorf("Expected: %+v", &refa)
		log.Errorf("Received: %+v", a)
		t.FailNow()
	}
	a = ConnectInfoGet(&db.Loc{Cluster: "state_test_cluster5"}, db.Master)
	if a != nil {
		t.FailNow()
	}
	err = DeleteClusterInfo("state_test_cluster2")
	test.CheckFail(err, t)

	log.Debugf("Test that we can resolve cluster name from service and db")

	err = util.ExecSQL(conn, "INSERT INTO state(service,cluster,db,tableName,gtid,input,output,rawSchema,schemaGTID) VALUES (?,?,?,?,'','','','','')", "state_test_service1", "state_test_cluster1", "state_test_db1", "state_test_table1")
	test.CheckFail(err, t)

	a = ConnectInfoGet(&db.Loc{Service: "state_test_service1", Name: "state_test_db1"}, db.Master)
	if a == nil || !reflect.DeepEqual(a, &refa) {
		log.Errorf("Expected: %+v", &refa)
		log.Errorf("Received: %+v", a)
		t.FailNow()
	}

	err = DeleteClusterInfo("state_test_cluster1")
	test.CheckFail(err, t)
	err = DeleteClusterInfo("state_test_cluster1")
	test.CheckFail(err, t)
}

func TestOutputSchema(t *testing.T) {
	initState(t)
	err := InsertSchema("state_schema1", "test_schema_body1")
	test.CheckFail(err, t)
	err = InsertSchema("state_schema2", "test_schema_body2")
	test.CheckFail(err, t)
	err = InsertSchema("state_schema1", "test_schema_body1")
	if err == nil {
		t.Fatalf("Duplicate names shouldn't be allowed")
	}
	if GetOutputSchema("state_schema1") != "test_schema_body1" {
		t.Fatalf("Content is wrong 1")
	}
	if GetOutputSchema("state_schema2") != "test_schema_body2" {
		t.Fatalf("Content is wrong 2")
	}
	if GetOutputSchema("state_schema3") != "" {
		t.Fatalf("Content is wrong 3")
	}
	err = UpdateSchema("state_schema1", "test_schema_body1_updated")
	test.CheckFail(err, t)

	if GetOutputSchema("state_schema1") != "test_schema_body1_updated" {
		t.Fatalf("Content is wrong 4")
	}
	if GetOutputSchema("state_schema2") != "test_schema_body2" {
		t.Fatalf("Content is wrong 5")
	}
	err = DeleteSchema("state_schema1")
	test.CheckFail(err, t)
	err = DeleteSchema("state_schema1")
	test.CheckFail(err, t)
	err = DeleteSchema("state_schema2")
	test.CheckFail(err, t)
	err = DeleteSchema("state_schema3")
	test.CheckFail(err, t)
}

func TestNewFlag(t *testing.T) {
	initState(t)
	insertStateRows(refST1, t)

	n, err := GetTableNewFlag("svc1", "db1_state", "table1")
	test.CheckFail(err, t)
	if !n {
		t.FailNow()
	}

	err = SetTableNewFlag("svc1", "db1_state", "table1", false)
	test.CheckFail(err, t)

	n, err = GetTableNewFlag("svc1", "db1_state", "table1")
	test.CheckFail(err, t)
	if n {
		t.FailNow()
	}
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	os.Exit(m.Run())
}
