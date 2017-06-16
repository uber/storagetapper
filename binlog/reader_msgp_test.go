package binlog

import "testing"

func TestMsgPackBasic(t *testing.T) {
	CheckQueries("local", testBasicPrepare, testBasic, testBasicResult, "msgpack", t)
}

func TestMsgPackUseDB(t *testing.T) {
	CheckQueries("local", testBasicPrepare, testUseDB, testUseDBResult, "msgpack", t)
}

func TestMsgPackMultiColumn(t *testing.T) {
	CheckQueries("local", testMultiColumnPrepare, testMultiColumn, testMultiColumnResult, "msgpack", t)
}

func TestMsgPackMultiRow(t *testing.T) {
	CheckQueries("local", testMultiColumnPrepare, testMultiRow, testMultiRowResult, "msgpack", t)
}

func TestMsgPackCompoundKey(t *testing.T) {
	CheckQueries("local", testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult, "msgpack", t)
}

func TestMsgPackDDL(t *testing.T) {
	CheckQueries("local", testDDLPrepare, testDDL, testDDLResult, "msgpack", t)
}

func TestMsgPackMultiTable(t *testing.T) {
	testName = "TestMultiTable"
	CheckQueries("local", testMultiTablePrepare, testMultiTable, testMultiTableResult1, "msgpack", t)
	testName = ""
}

func TestMsgPackKafkaBasic(t *testing.T) {
	CheckQueries("kafka", testBasicPrepare, testBasic, testBasicResult, "msgpack", t)
}

func TestMsgPackKafkaUseDB(t *testing.T) {
	CheckQueries("kafka", testBasicPrepare, testUseDB, testUseDBResult, "msgpack", t)
}

func TestMsgPackKafkaMultiColumn(t *testing.T) {
	CheckQueries("kafka", testMultiColumnPrepare, testMultiColumn, testMultiColumnResult, "msgpack", t)
}

func TestMsgPackKafkaMultiRow(t *testing.T) {
	CheckQueries("kafka", testMultiColumnPrepare, testMultiRow, testMultiRowResult, "msgpack", t)
}

func TestMsgPackKafkaCompoundKey(t *testing.T) {
	CheckQueries("kafka", testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult, "msgpack", t)
}

func TestMsgPackKafkaDDL(t *testing.T) {
	CheckQueries("kafka", testDDLPrepare, testDDL, testDDLResult, "msgpack", t)
}
