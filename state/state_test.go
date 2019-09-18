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
	"sync"
	"testing"

	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

var cfg *config.AppConfig

func checkStateEqual(m1 Type, m2 Type) bool {
	for i := 0; i < len(m2); i++ {
		if m2[i].Params == &config.Get().TableParams {
			m2[i].Params = nil
		}
		m2[i].GtidUpdatedAt = time.Time{}
	}
	return reflect.DeepEqual(m1, m2)
}

var refST1 = Type{
	{
		TableLoc: types.TableLoc{
			Service: "svc1", Cluster: "clst1", Db: "db1_state", Table: "table1",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 1, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
	{
		TableLoc: types.TableLoc{
			Service: "svc1", Cluster: "clst1", Db: "db1_state", Table: "table2",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 2, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
	{
		TableLoc: types.TableLoc{
			Service: "svc1", Cluster: "clst1", Db: "db2_state", Table: "table1",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 3, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
	{
		TableLoc: types.TableLoc{
			Service: "svc2", Cluster: "clst1", Db: "db3_state", Table: "table1",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 4, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
	{
		TableLoc: types.TableLoc{
			Service: "svc2", Cluster: "clst2", Db: "db2_state", Table: "table1",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 5, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
}

var refST2 = Type{
	{
		TableLoc: types.TableLoc{
			Service: "svc1", Cluster: "clst1", Db: "db1_state", Table: "table3",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 6, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
	{
		TableLoc: types.TableLoc{
			Service: "svc2", Cluster: "clst1", Db: "db2_state", Table: "table2",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 7, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
}

var refST3 = Type{
	{
		TableLoc: types.TableLoc{
			Service: "svc2", Cluster: "clst2", Db: "db2_state", Table: "table1",
			Input: "mysql", Output: "", Version: 0,
		},

		ID: 5, OutputFormat: "", Gtid: "", GtidUpdatedAt: time.Time{},
		SeqNo: 0, SchemaGtid: "", RawSchema: "", SnapshottedAt: time.Time{},
		CreatedAt: time.Time{}, UpdatedAt: time.Time{},
		NeedSnapshot: false, Deleted: false, ParamsRaw: "{}", Params: nil,
	},
}

func execSQL(t *testing.T, query string, param ...interface{}) {
	test.ExecSQL(mgr.conn, t, query, param...)
}

func insertStateRowForTest(id int64, t *types.TableLoc, gtid string, params string, t1 *testing.T) {
	if params == "" {
		params = "{}"
	}
	execSQL(t1, "INSERT INTO registrations(id,service,cluster,db,table_name,input,output,version,output_format,params) VALUES (?,?,?,?,?,?,?,?,'',?)", id, t.Service, t.Cluster, t.Db, t.Table, t.Input, t.Output, t.Version, params)
	execSQL(t1, "INSERT INTO state(id,service,cluster,db,table_name,input,output,version,output_format,reg_id,params) VALUES (?,?,?,?,?,?,?,?,'',?,?)", id, t.Service, t.Cluster, t.Db, t.Table, t.Input, t.Output, t.Version, id, params)

	execSQL(t1, "INSERT IGNORE INTO cluster_state(cluster, gtid) VALUES (?, ?)", t.Cluster, gtid)

	execSQL(t1, "INSERT INTO raw_schema(state_id, schema_gtid, raw_schema) VALUES (?, '', '')", id)
}

func insertStateRowForTests(s Type, t1 *testing.T) {
	var i int64
	for _, t := range s {
		if i == 0 {
			i = t.ID
		} else {
			i++
		}
		if i != t.ID {
			log.Errorf("service and dbs need to be sorted in reference structure")
			t1.FailNow()
		}

		insertStateRowForTest(t.ID, &types.TableLoc{Service: t.Service, Cluster: t.Cluster, Db: t.Db, Table: t.Table, Input: t.Input, Output: t.Output, Version: 0}, t.Gtid, "", t1)
	}
}

func mixinSchemaColumns(table string, column string, t *testing.T) {
	execSQL(t, "ALTER TABLE "+table+" ADD first_column INT FIRST, ADD middle_column INT AFTER "+column+",ADD last_column INT")
}

func resetState(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	// cleanup the state tables before running the tests
	require.True(t, Reset())

	//Test forward compatibility of state schema
	mixinSchemaColumns("state", "table_name", t)
	mixinSchemaColumns("cluster_state", "gtid", t)
	mixinSchemaColumns("raw_schema", "schema_gtid", t)
	mixinSchemaColumns("columns", "data_type", t)
	mixinSchemaColumns("clusters", "user", t)
	mixinSchemaColumns("output_schema", "type", t)
	mixinSchemaColumns("registrations", "table_name", t)
}

func TestGetCount(t *testing.T) {
	resetState(t)

	c, err := GetCount(true)
	test.CheckFail(err, t)
	if c != 0 {
		t.Fatalf("There should be nothing in the state yet")
	}

	insertStateRowForTests(refST1, t)

	c, err = GetCount(false)
	test.CheckFail(err, t)
	if c != 5 {
		t.Fatalf("There should be %v rows from the refST1. got %v", len(refST1), c)
	}
}

func TestGetTable(t *testing.T) {
	resetState(t)

	c, err := GetTableByID(3)
	require.NoError(t, err)
	require.Nil(t, c)

	insertStateRowForTests(refST1, t)

	c, err = GetTableByID(3)
	require.NoError(t, err)
	require.True(t, c != nil)

	refST1[2].Params = &config.Get().TableParams //point to default
	c.GtidUpdatedAt = time.Time{}
	if !reflect.DeepEqual(*c, refST1[2]) {
		log.Errorf("Reference: %v", refST1[2])
		log.Errorf("Read     : %v", c)
		t.FailNow()
	}
	refST1[2].Params = nil
}

func testGTIDs(t *testing.T) {
	log.Debugf("Check GTID handling")

	dbloc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	gts, err := GetGTID(dbloc1.Cluster)
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

	err = SetGTID(dbloc1.Cluster, exgts)
	test.CheckFail(err, t)

	gts, err = GetGTID(dbloc1.Cluster)
	test.CheckFail(err, t)

	if gts != exgts {
		t.Fatalf(gts)
	}

	log.Debugf("Check that StateGetGTID return non-empty GTID")
	tables := []string{"table5", "table0"}
	for i, v := range tables {
		insertStateRowForTest(int64(100+i), &types.TableLoc{Service: "svc1", Cluster: "clst1", Db: "db1_state", Table: v, Input: "mysql", Output: "kafka", Version: 0}, "", "", t)
	}

	gts, err = GetGTID(dbloc1.Cluster)
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
	err = SaveBinlogState(dbloc1.Cluster, gts1, seqno1)
	test.CheckFail(err, t)

	rows, err := util.QuerySQL(mgr.conn, stateSQL+" WHERE service='svc1' and db='db1_state' limit 1")
	test.CheckFail(err, t)
	st4, err := parseRows(rows)
	test.CheckFail(err, t)

	if st4[0].Gtid != gts1 || st4[0].SeqNo != seqno1 {
		log.Errorf("Expected: %v, %v", gts1, seqno1)
		log.Errorf("Got: %v, %v %+v", st4[0].Gtid, st4[0].SeqNo, st4)
		t.FailNow()
	}
}

func TestStateTableParams(t *testing.T) {
	resetState(t)

	s := config.Get().Pipe.Compression
	defer func() {
		config.Get().Pipe.Compression = s
	}()
	config.Get().Pipe.Compression = false

	execSQL(t, `CREATE DATABASE IF NOT EXISTS db11`)
	execSQL(t, `CREATE TABLE IF NOT EXISTS db11.table11(field1 BIGINT PRIMARY KEY)`)
	execSQL(t, `CREATE TABLE IF NOT EXISTS db11.table22(field1 BIGINT PRIMARY KEY)`)

	res := RegisterTable("clst11", "svc11", "db11", "table11", types.InputMySQL, "output11", 0, "json11", `{"Pipe":{"Compression":true}}`)
	require.Equal(t, res, true)
	res = RegisterTable("clst11", "svc11", "db11", "table22", types.InputMySQL, "output11", 0, "json11", ``)
	require.Equal(t, res, true)

	SyncRegisteredTables()

	tbl1, err := GetTable("svc11", "clst11", "db11", "table11", types.InputMySQL, "output11", 0)
	require.NoError(t, err)
	require.True(t, tbl1 != nil)
	require.Equal(t, tbl1.Params.Pipe.Compression, true)

	tbl2, err := GetTable("svc11", "clst11", "db11", "table22", types.InputMySQL, "output11", 0)
	require.NoError(t, err)
	require.True(t, tbl2 != nil)
	require.Equal(t, tbl2.Params.Pipe.Compression, false)

	st, err := Get()
	require.NoError(t, err)
	require.Equal(t, len(st), 2)
	require.Equal(t, st[1].Params, &config.Get().TableParams)

	config.Get().Pipe.Compression = true

	require.Equal(t, st[0].Params, &config.Get().TableParams)

	tbl1, err = GetTable("svc11", "clst11", "db11", "table11", types.InputMySQL, "output11", 0)
	require.NoError(t, err)
	require.True(t, tbl1 != nil)
	require.Equal(t, tbl1.Params.Pipe.Compression, true)

	tbl2, err = GetTable("svc11", "clst11", "db11", "table22", types.InputMySQL, "output11", 0)
	require.NoError(t, err)
	require.True(t, tbl2 != nil)
	require.Equal(t, tbl2.Params.Pipe.Compression, true)
}

func TestStateBasic(t *testing.T) {
	resetState(t)

	insertStateRowForTests(refST1, t)

	log.Debugf("Check table registration")

	reg, err := TableRegisteredInState(0)
	test.CheckFail(err, t)
	test.Assert(t, !reg, "Table with 0 id should never be registered")

	for i := 1; i <= len(refST1); i++ {
		if reg, err = TableRegisteredInState(int64(i)); err != nil || !reg {
			log.Errorf("Table with id=%v isn't registered", i)
			test.CheckFail(err, t)
		}
	}

	reg, err = TableRegisteredInState(6)
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

	insertStateRowForTests(refST2, t)

	log.Debugf("Check table deregistration")

	/* Deregistration doesn't change state version */
	execSQL(t, "DELETE FROM state WHERE id=4")

	reg, err = TableRegisteredInState(4)
	test.CheckFail(err, t)

	if reg {
		t.Fatalf("reg")
	}

	testGTIDs(t)
}

func TestStateSchema(t *testing.T) {
	resetState(t)

	dbloc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	execSQL(t, `DROP DATABASE IF EXISTS db1_state`)
	execSQL(t, `CREATE DATABASE IF NOT EXISTS db1_state`)

	execSQL(t, `CREATE TABLE db1_state.SCHEMA_TEST1 (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)

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
	ss, err := schema.Get(dbloc1, "SCHEMA_TEST2", "mysql")
	if err == nil {
		t.Fatalf("Should fail with error, Got some schema instead: %v", ss)
	}

	ts, err := schema.Get(dbloc1, "SCHEMA_TEST1", "mysql")
	test.CheckFail(err, t)

	if !reflect.DeepEqual(&refs, ts) {
		log.Debugf("Expected: %+v", &refs)
		log.Debugf("Received: %+v", ts)
		t.FailNow()
	}

	tsRaw, err := schema.GetRaw(dbloc1, "`db1_state`.`SCHEMA_TEST1`", "mysql")
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

	_, err = GetSchema("svc1", "db1_state", "SCHEMA_TEST1", "mysql", "kafka", 0)
	if err == nil || !strings.Contains(err.Error(), "no schema columns") {
		log.Errorf("Table shouldn't exist in the state yet")
		if err != nil {
			log.Errorf("Got wrong error instead: %v", err.Error())
		}
		t.FailNow()
	}

	sgtid, err := db.GetCurrentGTIDForDB(dbloc1, types.InputMySQL)
	if log.E(err) {
		t.FailNow()
	}

	if !ReplaceSchema("svc1", "clst1", ts, tsRaw, "", sgtid, "mysql", "kafka", 0, "", "") {
		t.Fatalf("%+v %+v", ts, err)
	}

	ts1, err := GetSchema("svc1", "db1_state", "SCHEMA_TEST1", "mysql", "kafka", 0)
	test.CheckFail(err, t)

	if !reflect.DeepEqual(&refs, ts1) {
		log.Errorf("Expected: %+v", &refs)
		log.Errorf("Received: %+v", ts1)
		t.FailNow()
	}

	/* Delete last column from ts */
	//	ts.Columns = append([]types.ColumnSchema{}, ts.Columns[0:1]...)

	sgtid1, err := db.GetCurrentGTIDForDB(dbloc1, types.InputMySQL)
	if log.E(err) {
		t.FailNow()
	}

	if !schema.MutateTable(mgr.nodbconn, "svc1", "db1_state", "SCHEMA_TEST1", "drop field2", ts, &tsRaw) {
		t.FailNow()
	}
	log.Debugf("New schema: %+v", ts)
	log.Debugf("New raw schema: %+v", tsRaw)

	if !ReplaceSchema("svc1", "clst1", ts, tsRaw, sgtid, sgtid1, "mysql", "kafka", 0, "", "") {
		t.FailNow()
	}

	sts, err := GetSchema("svc1", "db1_state", "SCHEMA_TEST1", "mysql", "kafka", 0)
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
		log.Errorf("Received: %+v", sts)
		t.FailNow()
	}
}

func TestRegisterDeregisterTable(t *testing.T) {
	resetState(t)

	tests := []struct {
		name         string
		cluster      string
		svc          string
		sdb          string
		table        string
		input        string
		output       string
		version      int
		outputFormat string
	}{
		{
			name:         "mysql_input_type",
			cluster:      "cluster1",
			svc:          "svc1",
			sdb:          "db1_state",
			table:        "REG_SCHEMA_TEST1",
			input:        "mysql",
			output:       "kafka",
			version:      0,
			outputFormat: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := RegisterTable(tt.cluster, tt.svc, tt.sdb, tt.table, tt.input, tt.output, tt.version, tt.outputFormat, "")
			require.True(t, res, "Failed to register table")

			res, err := TableRegistered(tt.svc, tt.cluster, tt.sdb, tt.table, tt.input, tt.output, tt.version, false)
			require.NoError(t, err)
			require.True(t, res)

			res = DeregisterTable(tt.cluster, tt.svc, tt.sdb, tt.table, tt.input, tt.output, tt.version)
			require.True(t, res, "Failed to deregister table")
		})
	}
}

func TestValidateRegistration(t *testing.T) {
	tests := []struct {
		name    string
		svc     string
		sdb     string
		table   string
		input   string
		output  string
		wantRes bool
	}{
		{
			name:    "incorrect_input_type",
			input:   "foo",
			wantRes: false,
		},
		{
			name:    "missing_output_type",
			svc:     "svc1",
			sdb:     "db1",
			input:   types.InputMySQL,
			table:   "tbl1",
			wantRes: false,
		},
		{
			name:    "missing_params_for_mysql_input",
			svc:     "svc1",
			input:   types.InputMySQL,
			table:   "tbl1",
			output:  "kafka",
			wantRes: false,
		},
		{
			name:    "successful_validation_of_mysql_input",
			svc:     "svc1",
			sdb:     "db1",
			input:   types.InputMySQL,
			table:   "tbl1",
			output:  "kafka",
			wantRes: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := ValidateRegistration(tt.svc, tt.sdb, tt.table, tt.input, tt.output, 0)
			require.Equal(t, tt.wantRes, res)
		})
	}
}

func TestTableRegisterInState(t *testing.T) {
	resetState(t)

	execSQL(t, `DROP DATABASE IF EXISTS db1_state`)
	execSQL(t, `CREATE DATABASE IF NOT EXISTS db1_state`)

	execSQL(t, `CREATE TABLE db1_state.REG_SCHEMA_TEST1 (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)

	dbLoc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	for i := 1; i <= 2; i++ {
		res := RegisterTableInState(dbLoc1, "REG_SCHEMA_TEST1", "mysql", "kafka", i, "", "", 0)
		require.True(t, res)

		tbl, err := GetTableByID(int64(i))
		require.NoError(t, err)
		require.True(t, tbl != nil)
		require.Equal(t, "REG_SCHEMA_TEST1", tbl.Table)
		require.Equal(t, i, tbl.Version)
	}

	for i := 1; i <= 2; i++ {
		res := DeregisterTableFromState(dbLoc1, "REG_SCHEMA_TEST1", "mysql", "kafka", i, 0)
		require.True(t, res)
	}

	tbl, err := GetTable(dbLoc1.Service, dbLoc1.Cluster, dbLoc1.Name, "REG_SCHEMA_TEST1", "mysql", "kafka", 0)
	require.NoError(t, err)
	require.True(t, tbl == nil)
}

//Test that there is no deadlocks during parallel tables registration
func TestTableRegisterInStateParallel(t *testing.T) {
	resetState(t)

	execSQL(t, `DROP DATABASE IF EXISTS db1_state`)
	execSQL(t, `CREATE DATABASE IF NOT EXISTS db1_state`)

	execSQL(t, `CREATE TABLE db1_state.REG_SCHEMA_TEST1 (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)

	dbLoc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	var wg sync.WaitGroup
	wg.Add(20)

	for i := 1; i <= 20; i++ {
		go func(i int) {
			defer wg.Done()
			res := RegisterTableInState(dbLoc1, "REG_SCHEMA_TEST1", "mysql", "kafka", i, "", "", 0)
			require.True(t, res)
		}(i)
	}

	wg.Wait()

	for i := 1; i <= 20; i++ {
		tbl, err := GetTableByID(int64(i))
		require.NoError(t, err)
		require.True(t, tbl != nil)
		require.Equal(t, "REG_SCHEMA_TEST1", tbl.Table)
	}

	for i := 1; i <= 20; i++ {
		res := DeregisterTableFromState(dbLoc1, "REG_SCHEMA_TEST1", "mysql", "kafka", i, 0)
		require.True(t, res)
	}

	tbl, err := GetTable(dbLoc1.Service, dbLoc1.Cluster, dbLoc1.Name, "REG_SCHEMA_TEST1", "mysql", "kafka", 0)
	require.NoError(t, err)
	require.True(t, tbl == nil)
}

func testTableSynced(id int64, synced int, t *testing.T) {
	var syncState int
	err := util.QueryRowSQL(mgr.conn, "SELECT sync_state FROM registrations WHERE id=?", 1).Scan(&syncState)
	require.NoError(t, err)
	require.Equal(t, synced, syncState)
}

func TestSyncRegisteredDeregisteredTables(t *testing.T) {
	resetState(t)

	execSQL(t, `DROP DATABASE IF EXISTS db1_state`)
	execSQL(t, `CREATE DATABASE IF NOT EXISTS db1_state`)

	execSQL(t, `CREATE TABLE db1_state.REG_SCHEMA_TEST1 (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)

	res := RegisterTable("cluster1", "svc1", "db1_state", "REG_SCHEMA_TEST1", "mysql", "kafka", 1, "", "")
	require.True(t, res, "Failed to register db1_state in registrations")

	res, err := TableRegistered("svc1", "cluster1", "db1_state", "REG_SCHEMA_TEST1", "mysql", "kafka", 1, false)
	require.NoError(t, err)
	require.True(t, res)

	testTableSynced(1, regStateUnsynced, t)
	SyncRegisteredTables()
	testTableSynced(1, regStateSynced, t)

	tbl, err := GetTable("svc1", "cluster1", "db1_state", "REG_SCHEMA_TEST1", "mysql", "kafka", 1)
	require.NoError(t, err)
	require.True(t, tbl != nil)

	res = DeregisterTable("", "svc1", "db1_state", "REG_SCHEMA_TEST1", "mysql", "kafka", 1)
	require.True(t, res, "Failed to deregister db1_state")

	testTableSynced(1, regStateUnsynced, t)
	SyncDeregisteredTables()
	testTableSynced(1, regStateSynced, t)

	tbl, err = GetTable("svc1", "cluster1", "db1_state", "REG_SCHEMA_TEST1", "mysql", "kafka", 1)
	require.NoError(t, err)
	require.True(t, tbl == nil)

	tblObj, err := GetTableByID(1)
	require.NoError(t, err)
	require.True(t, tblObj == nil)
}

func TestTableVersions(t *testing.T) {
	resetState(t)

	test.ExecSQL(mgr.conn, t, `DROP DATABASE IF EXISTS db1_state`)
	test.ExecSQL(mgr.conn, t, `CREATE DATABASE IF NOT EXISTS db1_state`)
	test.ExecSQL(mgr.conn, t, `CREATE TABLE db1_state.REG_SCHEMA_TEST1 (
		field1 BIGINT PRIMARY KEY,
		field2 BIGINT NOT NULL DEFAULT 0,
		UNIQUE INDEX(field2, field1)
	)`)

	dbloc1 := &db.Loc{Cluster: "clst1", Service: "svc1", Name: "db1_state"}

	tests := []struct {
		input   string
		output  string
		version int
		real    int
	}{
		{input: "mysql", output: "kafka", version: 0, real: 1}, //1
		{input: "mysql", output: "kafka", version: 1, real: 1}, //2
		{input: "mysql", output: "kafka", version: 1, real: 0}, //duplicate
		{input: "mysql", output: "file", version: 1, real: 1},  //3
		{input: "mysql", output: "file", version: 2, real: 1},  //4
		{input: "mysql", output: "file", version: 2, real: 0},  //duplicate
		{input: "file", output: "kafka", version: 2, real: 1},  //5
	}

	for i := 0; i < len(tests); i++ {
		if !RegisterTableInState(dbloc1, "REG_SCHEMA_TEST1", tests[i].input, tests[i].output, tests[i].version, "", "", 0) {
			t.Fatalf("Fail to register %v, %v, %v", tests[i].input, tests[i].output, tests[i].version)
		}
	}

	n := 0
	for i := 0; i < len(tests); i++ {
		cnt, err := GetCount(false)
		log.Debugf("test: %+v cnt: %v", tests[i], cnt)

		if log.E(err) || cnt != 5-n {
			t.Fatalf("Expect %d rows in the state, got %d", 5-n, cnt)
		}

		n += tests[i].real

		_, err = GetSchema("svc1", "db1_state", "REG_SCHEMA_TEST1", tests[i].input, tests[i].output, tests[i].version)
		test.CheckFail(err, t)

		if !DeregisterTableFromState(&db.Loc{Service: "svc1", Cluster: "clst1", Name: "db1_state"}, "REG_SCHEMA_TEST1", tests[i].input, tests[i].output, tests[i].version, 0) {
			t.Fatalf("Fail to deregister %v, %v, %v", tests[i].input, tests[i].output, tests[i].version)
		}
	}
}

func TestClusterInfo(t *testing.T) {
	resetState(t)

	refa := db.Addr{Name: "state_test_cluster1", Host: "state_test_host1", Port: 1234, User: "state_test_user1", Pwd: "state_test_pw1", Db: "state_test_db1"}
	refb := db.Addr{Name: "state_test_cluster2", Host: "state_test_host2", Port: 4567, User: "state_test_user2", Pwd: "state_test_pw2", Db: "state_test_db2"}
	err := InsertClusterInfo(&refa)
	test.CheckFail(err, t)
	err = InsertClusterInfo(&refb)
	test.CheckFail(err, t)
	err = InsertClusterInfo(&refa)
	//Should fail because Name and Type is a primary key and we're inserting it twice
	if err == nil {
		t.FailNow()
	}
	a, err := ConnectInfoGet(&db.Loc{Cluster: "state_test_cluster1", Name: "state_test_db1"}, db.Master)
	test.CheckFail(err, t)
	if a == nil || !reflect.DeepEqual(a, &refa) {
		log.Errorf("Expected: %+v", &refa)
		log.Errorf("Received: %+v", a)
		t.FailNow()
	}
	a, err = ConnectInfoGet(&db.Loc{Cluster: "state_test_cluster5"}, db.Master)
	test.CheckFail(err, t)
	if a != nil {
		t.Fatalf("Cluster state_test_cluster5 should not exist")
	}

	addrs, err := GetClusterInfo("")
	test.CheckFail(err, t)
	test.Assert(t, len(addrs) == 2, "Should be two records. got %+v", addrs)
	refa.Db, refb.Db = "", ""
	if !reflect.DeepEqual(addrs[0], refa) || !reflect.DeepEqual(addrs[1], refb) {
		log.Errorf("Expected: %+v %+v", refa, refb)
		log.Errorf("Received: %+v %+v", addrs[0], addrs[1])
		t.FailNow()
	}
	addrs, err = GetClusterInfo("WHERE name='state_test_cluster1'")
	test.CheckFail(err, t)
	test.Assert(t, len(addrs) == 1, "Should be two records. got %+v", addrs)
	if !reflect.DeepEqual(addrs[0], refa) {
		log.Errorf("Expected: %+v", refa)
		log.Errorf("Received: %+v", addrs[0])
		t.FailNow()
	}
	refa.Db = "state_test_db1"
	refb.Db = "state_test_db2"

	err = DeleteClusterInfo("state_test_cluster2")
	test.CheckFail(err, t)

	log.Debugf("Test that we can resolve cluster name from service and db")

	insertStateRowForTest(1, &types.TableLoc{Service: "state_test_service1", Cluster: "state_test_cluster1", Db: "state_test_db1", Table: "state_test_table1", Input: "mysql", Output: "kafka", Version: 0}, "", "", t)

	a, err = ConnectInfoGet(&db.Loc{Service: "state_test_service1", Name: "state_test_db1"}, db.Master)
	test.CheckFail(err, t)
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
	resetState(t)

	err := InsertSchema("state_schema1", "test_type", "test_schema_body1")
	test.CheckFail(err, t)
	err = InsertSchema("state_schema2", "test_type", "test_schema_body2")
	test.CheckFail(err, t)
	err = InsertSchema("state_schema1", "test_type", "test_schema_body1")
	if err == nil {
		t.Fatalf("Duplicate names shouldn't be allowed")
	}
	if GetOutputSchema("state_schema1", "test_type") != "test_schema_body1" {
		t.Fatalf("Content is wrong 1")
	}
	if GetOutputSchema("state_schema2", "test_type") != "test_schema_body2" {
		t.Fatalf("Content is wrong 2")
	}
	if GetOutputSchema("state_schema3", "test_type") != "" {
		t.Fatalf("Content is wrong 3")
	}
	err = UpdateSchema("state_schema1", "test_type", "test_schema_body1_updated")
	test.CheckFail(err, t)

	if GetOutputSchema("state_schema1", "test_type") != "test_schema_body1_updated" {
		t.Fatalf("Content is wrong 4")
	}
	if GetOutputSchema("state_schema2", "test_type") != "test_schema_body2" {
		t.Fatalf("Content is wrong 5")
	}
	err = DeleteSchema("state_schema1", "test_type")
	test.CheckFail(err, t)
	err = DeleteSchema("state_schema1", "test_type")
	test.CheckFail(err, t)
	err = DeleteSchema("state_schema2", "test_type")
	test.CheckFail(err, t)
	err = DeleteSchema("state_schema3", "test_type")
	test.CheckFail(err, t)
}

func TestNewFlag(t *testing.T) {
	resetState(t)
	insertStateRowForTest(1, &types.TableLoc{Service: "sns_svc1", Cluster: "sns_clst1", Db: "sns_db1", Table: "sns_t1", Input: "mysql", Output: "kafka", Version: 0}, "", `{"Schedule":{"Interval" : 10}}`, t)

	row, err := GetTable("sns_svc1", "sns_clst1", "sns_db1", "sns_t1", "mysql", "kafka", 0)
	test.CheckFail(err, t)
	test.Assert(t, row != nil)

	test.Assert(t, !row.NeedSnapshot)
	test.Assert(t, row.SnapshottedAt.IsZero())
	tm := time.Now()
	//NULL snapshotted_at should indicate that it's time for snapshot
	test.Assert(t, row.TimeForSnapshot(tm))

	row, err = UpdateSnapshottedAt(row, tm)
	test.CheckFail(err, t)

	//Fields should be non empty after UpdateSnapshottedAt
	test.Assert(t, row.NeedSnapshot)
	test.Assert(t, row.SnapshottedAt == tm.Round(time.Second))

	//snapshotted_at is up-to-date, so not a time for snapshot
	test.Assert(t, !row.TimeForSnapshot(tm))

	//The time we know is different from snapshotted_at
	test.Assert(t, row.SnapshotTimeChanged(tm.Add(time.Second)))

	//Schedule interval is 10 sec, simulate 20 secs in future
	test.Assert(t, row.TimeForSnapshot(tm.Add(20*time.Second)))

	err = ClearNeedSnapshot(row.ID, row.SnapshottedAt)
	test.CheckFail(err, t)

	row, err = GetTable("sns_svc1", "sns_clst1", "sns_db1", "sns_t1", "mysql", "kafka", 0)
	test.CheckFail(err, t)
	test.Assert(t, row != nil)
	test.Assert(t, !row.NeedSnapshot)

	row, err = UpdateSnapshottedAt(row, tm.Add(-5*time.Second))
	test.CheckFail(err, t)
	test.Assert(t, row.SnapshottedAt == tm.Round(time.Second))
	test.Assert(t, !row.NeedSnapshot)
}

func TestSanitizeRegParams(t *testing.T) {
	cluster, svc, sdb, table := SanitizeRegParams("cluster1", "service1", "db1", "tbl1", "mysql")
	require.Equal(t, "cluster1", cluster)
	require.Equal(t, "service1", svc)
	require.Equal(t, "db1", sdb)
	require.Equal(t, "tbl1", table)

	cluster, _, sdb, _ = SanitizeRegParams("*", "service1", "*", "tbl1", "mysql")
	require.Equal(t, "", cluster)
	require.Equal(t, "", sdb)

	cluster, _, sdb, _ = SanitizeRegParams("cluster1", "service1", "db1", "tbl1", "schemaless")
	require.Equal(t, "cluster1", cluster)
	require.Equal(t, "db1", sdb)
}

func TestClearClusterState(t *testing.T) {
	resetState(t)

	tl := types.TableLoc{Service: "svc1", Cluster: "clst1", Db: "db1", Table: "t1", Input: types.InputMySQL, Output: "kafka", Version: 0}
	s := types.TableSchema{DBName: "db1", TableName: "t1", Columns: []types.ColumnSchema{}}

	//Test old cluster state is not cleared when non deleted table reinserted
	insertStateRowForTest(1, &tl, "some gtid", "", t)

	tx, err := mgr.conn.Begin()
	test.CheckFail(err, t)
	id, err := insertNewSchema(tx, "newgtid", &tl, 0, "json", "", "rawschema", &s)
	test.CheckFail(err, t)
	err = tx.Commit()
	test.CheckFail(err, t)
	test.Assert(t, id == 1)

	tbl, err := GetTableByID(1)
	test.CheckFail(err, t)
	test.Assert(t, tbl != nil)
	test.Assert(t, tbl.Gtid == "some gtid")

	tx, err = mgr.conn.Begin()
	test.CheckFail(err, t)
	_ = deleteStateRow(tx, &tl)
	err = tx.Commit()
	test.CheckFail(err, t)

	tx, err = mgr.conn.Begin()
	test.CheckFail(err, t)
	id, err = insertNewSchema(tx, "newgtid", &tl, 0, "json", "", "rawschema", &s)
	test.CheckFail(err, t)
	err = tx.Commit()
	test.CheckFail(err, t)
	test.Assert(t, id == 1)

	tbl, err = GetTableByID(1)
	test.CheckFail(err, t)
	test.Assert(t, tbl != nil)
	test.Assert(t, tbl.Gtid == "")

	//Test that newly inserted row clears existing cluster state
	tx, err = mgr.conn.Begin()
	test.CheckFail(err, t)
	_ = deleteStateRow(tx, &tl)
	err = tx.Commit()
	test.CheckFail(err, t)

	test.ExecSQL(mgr.conn, t, "UPDATE cluster_state SET gtid='some gtid 3' WHERE cluster='clst1'")
	tl.Table = "t2"

	tx, err = mgr.conn.Begin()
	test.CheckFail(err, t)
	id, err = insertNewSchema(tx, "newgtid", &tl, 0, "json", "", "rawschema", &s)
	test.CheckFail(err, t)
	err = tx.Commit()
	test.CheckFail(err, t)

	tbl, err = GetTableByID(id)
	test.CheckFail(err, t)
	test.Assert(t, tbl.Gtid == "", "tbl.Gtid=%v", tbl.Gtid)
}

func TestCheckGetVersion(t *testing.T) {
	resetState(t)

	require.True(t, CheckMySQLVersion("8.0"))
	require.True(t, CheckMySQLVersion("8"))
	require.False(t, CheckMySQLVersion("5"))
	require.False(t, CheckMySQLVersion("5.7"))
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	if err := InitManager(shutdown.Context, cfg); err != nil {
		log.Fatalf("Failed to init State")
		os.Exit(1)
	}
	defer Close()

	os.Exit(m.Run())
}
