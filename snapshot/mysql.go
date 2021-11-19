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

package snapshot

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

const cancelCheckInterval = 180 * time.Second

// mysqlReader is a snapshot reader structure
type mysqlReader struct {
	dbl       *db.Loc
	connInfo  *db.Addr
	connType  db.ConnectionType
	conn      *sql.DB
	trx       *sql.Tx
	rows      *sql.Rows
	log       log.Logger
	nrecs     uint64
	ndone     uint64
	encoder   encoder.Encoder
	inputType string
	outMsg    []byte
	key       string
	err       error
	query     string
	metrics   *metrics.Snapshot
	sqlRow    []interface{}
	row       []interface{}
	schema    *types.TableSchema
	ticker    *time.Ticker

	partitionKey string
}

func init() {
	registerPlugin(types.InputMySQL, createMySQLReader)
}

func createMySQLReader(svc string, cluster string, dbs string, table string, params *config.TableParams, enc encoder.Encoder, m *metrics.Snapshot) (Reader, error) {
	query := "SELECT * FROM `<table_name>` <filter> FORCE INDEX (primary)"
	query = strings.Replace(query, "<filter>", FilterRow(types.InputMySQL, svc+"."+table, params), -1)
	r := &mysqlReader{query: query, inputType: types.InputMySQL, encoder: enc, connType: db.Slave, metrics: m}
	_, err := r.start(svc, cluster, dbs, table)
	return r, err
}

func (s *mysqlReader) startFromTx(svc string, _ string, dbs string, table string,
	tx *sql.Tx) (string, error) {

	var lastGtid string
	var err error

	s.log = log.WithFields(log.Fields{"service": svc, "db": dbs, "table": table})

	s.trx = tx

	// Get GTID which is earlier in time then any row we will read during snapshot
	err = s.trx.QueryRow("SELECT @@global.gtid_executed").Scan(&lastGtid)
	if log.EL(s.log, err) {
		return lastGtid, err
	}

	// Use approximate row count, so as it's for reporting progress only
	err = s.trx.QueryRow("SELECT table_rows FROM information_schema.tables WHERE table_schema=? AND "+
		"table_name=?", dbs, table).Scan(&s.nrecs)
	if log.EL(s.log, err) {
		return lastGtid, err
	}

	query := strings.Replace(s.query, "<table_name>", table, -1)
	s.log.Infof("Snapshot reader query: %s", query)
	s.rows, err = s.trx.Query(query)

	if log.EL(s.log, err) {
		return lastGtid, err
	}

	s.ndone = 0
	s.log.Infof("Snapshot reader started, will stream %v records", s.nrecs)

	var c []string
	c, s.err = s.rows.Columns()
	if log.EL(s.log, s.err) {
		return "", s.err
	}

	s.schema = s.encoder.Schema()

	if len(c) != len(s.schema.Columns) {
		return "", fmt.Errorf("rows column count(%v) should be equal to schema's column count(%v)", len(c), len(s.schema.Columns))
	}

	s.sqlRow = make([]interface{}, len(c))
	for i := 0; i < len(c); i++ {
		s.sqlRow[i] = util.MySQLToDriverType(s.schema.Columns[i].DataType, s.schema.Columns[i].Type)
	}
	s.row = make([]interface{}, len(c))

	s.ticker = time.NewTicker(cancelCheckInterval)

	return lastGtid, err
}

func (s *mysqlReader) startLow(svc string, cluster string, dbs string, table string) error {
	var err error
	s.dbl = &db.Loc{Cluster: cluster, Service: svc, Name: dbs}
	s.connInfo, err = db.GetConnInfo(s.dbl, s.connType, s.inputType)
	if log.E(err) {
		return err
	}

	s.conn, err = db.Open(s.connInfo)
	if err != nil {
		return err
	}

	// Do we need a transaction at all? We can use seqno to separate snapshot and binlog data. Binlog is always newer.
	// If we need it, we need to rely on MySQL instance transaction isolation level.
	// Uncomment later if we have go1.8
	// BeginTx since go1.8
	// s.trx, err = s.conn.BeginTx(shutdown.Context, sql.TxOptions{sql.LevelRepeatableRead, true})
	s.trx, err = s.conn.Begin()
	if log.E(err) {
		log.E(s.conn.Close())
		return err
	}

	return nil
}

// start connects to the db and starts snapshot for the table
func (s *mysqlReader) start(svc string, cluster string, dbs string, table string) (string, error) {
	if err := s.startLow(svc, cluster, dbs, table); err != nil {
		return "", err
	}

	g, err := s.startFromTx(svc, cluster, dbs, table, s.trx)
	if err != nil {
		log.E(s.trx.Rollback())
		log.E(s.conn.Close())
	}
	return g, err
}

func (s *mysqlReader) endFromTx() {
	s.ticker.Stop()

	if s.rows != nil {
		log.EL(s.log, s.rows.Close())
	}
}

// End uninitializes snapshot reader
func (s *mysqlReader) End() {
	s.endFromTx()

	if s.trx != nil {
		log.EL(s.log, s.trx.Rollback())
	}
	if s.conn != nil {
		log.EL(s.log, s.conn.Close())
	}

	s.log.Infof("Snapshot reader finished")
}

// isValidConn checks the validity of the connection to the DB to make sure we are connected to the right DB
func (s *mysqlReader) isValidConn() bool {
	select {
	case <-s.ticker.C:
		if !db.IsValidConn(s.dbl, s.connType, s.connInfo, s.inputType) {
			return false
		}
	default:
	}
	return true
}

func driverTypeToGoTypeLow(p interface{}, schema *types.ColumnSchema) (v interface{}, size int64) {
	switch f := p.(type) {
	case *sql.NullInt64:
		if f.Valid {
			if schema.DataType != "bigint" {
				v = int32(f.Int64)
				size = 4
			} else {
				v = f.Int64
				size = 8
			}
		}
	case *sql.NullBool:
		if f.Valid {
			v = f.Bool
			size = int64(1)
		}
	case *sql.NullString:
		if f.Valid {
			v = f.String
			size = int64(len(f.String))
		}
	case *sql.NullTime:
		if f.Valid {
			v = f.Time
			size = 20
		}
	case *sql.NullFloat64:
		if f.Valid {
			if schema.DataType == "float" {
				v = float32(f.Float64)
				size = 4
			} else {
				v = f.Float64
				size = 8
			}
		}
	case *sql.RawBytes:
		if f != nil {
			b := []byte(*f)
			v = b
			size = int64(len(b))
		}
	}
	return
}

func driverTypeToGoType(v []interface{}, p []interface{}, schema *types.TableSchema) int64 {
	var size, s int64

	for i := 0; i < len(p); i++ {
		v[i], s = driverTypeToGoTypeLow(p[i], &schema.Columns[i])
		size += s
	}

	return size
}

//Pop pops record fetched by FetchNext
func (s *mysqlReader) Pop() (string, string, []byte, error) {
	return s.key, s.partitionKey, s.outMsg, s.err
}

func (s *mysqlReader) fetchRow() []interface{} {
	if !s.isValidConn() {
		s.err = fmt.Errorf("cluster topology has changed")
		return nil
	}

	if !s.rows.Next() {
		if s.err = s.rows.Err(); log.EL(s.log, s.err) {
			return nil
		}
		if s.ndone == s.nrecs {
			s.log.Infof("Finished. Done %v(%v%%) of %v", s.ndone, 100, s.nrecs)
		}
		return nil
	}

	s.err = s.rows.Scan(s.sqlRow...)
	if log.EL(s.log, s.err) {
		return nil
	}

	size := driverTypeToGoType(s.row, s.sqlRow, s.schema)
	s.metrics.BytesRead.Inc(size)
	s.metrics.EventsRead.Inc(1)
	return s.row
}

func (s *mysqlReader) encodeRow(v []interface{}) {
	s.outMsg, s.err = s.encoder.Row(types.Insert, &v, ^uint64(0), time.Time{})
	if log.EL(s.log, s.err) {
		return
	}
	s.key = encoder.GetRowKey(s.encoder.Schema(), &v)
	s.partitionKey = s.key
}

func (s *mysqlReader) reportStat() {
	//Statistics maybe inaccurate so we can have some rows even if we got 0 when
	//read rows count
	if s.nrecs == 0 {
		s.nrecs = 1
	}
	pctdone := s.ndone * 100 / s.nrecs
	var o uint64
	if s.nrecs%10 != 0 {
		o = 1
	}
	if s.ndone%(s.nrecs/10+o) == 0 {
		s.log.Infof("Snapshot ... Done %v(%v%%) of %v", s.ndone, pctdone, s.nrecs)
	}
	s.ndone++
}

//FetchNext fetches the record from MySQL and encodes using encoder provided when
//reader created
func (s *mysqlReader) FetchNext() bool {
	s.err = nil

	v := s.fetchRow()

	if s.err != nil {
		return true
	}

	if v == nil {
		return false
	}

	s.encodeRow(v)

	s.reportStat()

	return true
}
