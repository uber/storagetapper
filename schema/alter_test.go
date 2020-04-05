package schema

import (
	"reflect"
	"testing"

	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
	"github.com/uber/storagetapper/types"
)

func TestMutateTable(t *testing.T) {
	test.SkipIfNoMySQLAvailable(t)

	createTestSchemaTable(t)

	var tblSchema types.TableSchema

	rawSchema, err := GetRaw(&db.Loc{Service: TestSvc, Name: TestDB}, TestTbl, TestInput)
	test.CheckFail(err, t)

	if !MutateTable(conn, TestSvc, TestDB, TestTbl, ` ADD f111  BIGINT`, &tblSchema, &rawSchema) {
		t.Fatalf("MutateTable failed")
	}

	test.ExecSQL(conn, t, `ALTER TABLE `+types.MyDBName+`.`+TestTbl+` ADD f111  BIGINT`)

	tblSchemaRef, err := Get(&db.Loc{Service: TestSvc, Name: TestDB}, TestTbl, TestInput)
	test.CheckFail(err, t)

	log.Debugf("%+v", tblSchemaRef)
	log.Debugf("%+v", tblSchema)
	if !reflect.DeepEqual(tblSchemaRef, &tblSchema) {
		t.Fatalf("Wrong mutated schema")
	}

	dropTestSchemaTable(t)
}
