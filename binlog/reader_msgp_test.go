package binlog

import (
	"testing"

	"github.com/uber/storagetapper/pipe"
)

func TestMsgPackBasic(t *testing.T) {
	CheckQueries(pipe.Local, testBasicPrepare, testBasic, testBasicResult, "msgpack", t)
}

func TestMsgPackUseDB(t *testing.T) {
	CheckQueries(pipe.Local, testBasicPrepare, testUseDB, testUseDBResult, "msgpack", t)
}

func TestMsgPackMultiColumn(t *testing.T) {
	CheckQueries(pipe.Local, testMultiColumnPrepare, testMultiColumn, testMultiColumnResult, "msgpack", t)
}

func TestMsgPackMultiRow(t *testing.T) {
	CheckQueries(pipe.Local, testMultiColumnPrepare, testMultiRow, testMultiRowResult, "msgpack", t)
}

func TestMsgPackCompoundKey(t *testing.T) {
	CheckQueries(pipe.Local, testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult, "msgpack", t)
}

func TestMsgPackDDL(t *testing.T) {
	CheckQueries(pipe.Local, testDDLPrepare, testDDL, testDDLResult, "msgpack", t)
}

func TestMsgPackMultiTable(t *testing.T) {
	testName = "TestMultiTable"
	CheckQueries(pipe.Local, testMultiTablePrepare, testMultiTable, testMultiTableResult1, "msgpack", t)
	testName = ""
}

func TestMsgPackKafkaBasic(t *testing.T) {
	CheckQueries(pipe.Kafka, testBasicPrepare, testBasic, testBasicResult, "msgpack", t)
}

func TestMsgPackKafkaUseDB(t *testing.T) {
	CheckQueries(pipe.Kafka, testBasicPrepare, testUseDB, testUseDBResult, "msgpack", t)
}

func TestMsgPackKafkaMultiColumn(t *testing.T) {
	CheckQueries(pipe.Kafka, testMultiColumnPrepare, testMultiColumn, testMultiColumnResult, "msgpack", t)
}

func TestMsgPackKafkaMultiRow(t *testing.T) {
	CheckQueries(pipe.Kafka, testMultiColumnPrepare, testMultiRow, testMultiRowResult, "msgpack", t)
}

func TestMsgPackKafkaCompoundKey(t *testing.T) {
	CheckQueries(pipe.Kafka, testCompoundKeyPrepare, testCompoundKey, testCompoundKeyResult, "msgpack", t)
}

func TestMsgPackKafkaDDL(t *testing.T) {
	CheckQueries(pipe.Kafka, testDDLPrepare, testDDL, testDDLResult, "msgpack", t)
}
