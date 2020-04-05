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

package pipe

import (
	"database/sql"
	"encoding/hex"
	"fmt" //"context"
	"strings"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/util"

	//_ "github.com/kshvakov/clickhouse"
	_ "github.com/lib/pq"
	_ "github.com/mailru/go-clickhouse"
)

const (
	mysql      = "mysql"
	postgres   = "postgres"
	clickhouse = "clickhouse"
)

type sqlPipe struct {
	cfg config.PipeConfig
}

type sqlProducer struct {
	*sqlPipe
	conn *sql.DB
	tx   *sql.Tx
}

type sqlConsumer struct {
	*sqlPipe
	conn   *sql.DB
	rows   *sql.Rows
	msg    []byte
	err    error
	row    []interface{}
	insert string
	topic  string
	inited bool
}

func init() {
	registerPlugin(mysql, initMySQLPipe)
	registerPlugin(postgres, initPostgresPipe)
	registerPlugin(clickhouse, initClickHousePipe)
}

func initSQLPipe(tp string, cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	p := &sqlPipe{*cfg}
	p.cfg.SQL.Type = tp
	return p, nil
}

func initMySQLPipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	return initSQLPipe(mysql, cfg, db)
}

func initPostgresPipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	return initSQLPipe(postgres, cfg, db)
}

func initClickHousePipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	return initSQLPipe(clickhouse, cfg, db)
}

// Type returns Pipe type as sql
func (p *sqlPipe) Type() string {
	return p.cfg.SQL.Type
}

// Config returns pipe configuration
func (p *sqlPipe) Config() *config.PipeConfig {
	return &p.cfg
}

// Close release resources associated with the pipe
func (p *sqlPipe) Close() error {
	return nil
}

//NewProducer registers a new sync producer
func (p *sqlPipe) NewProducer(topic string) (Producer, error) {
	t := p.cfg.SQL.Type
	if t == "" {
		t = mysql
	}
	var err error
	var conn *sql.DB
	if p.cfg.SQL.DSN != "" {
		conn, err = sql.Open(t, p.cfg.SQL.DSN)
		if err != nil {
			return nil, err
		}
	} else {
		ci, err := db.GetConnInfo(&db.Loc{Service: p.cfg.SQL.Service, Cluster: p.cfg.SQL.Cluster, Name: p.cfg.SQL.DB}, db.Master, p.cfg.SQL.Type)
		if err != nil {
			return nil, err
		}

		conn, err = db.OpenModeType(ci, p.cfg.SQL.Type, db.SQLMode)
		if err != nil {
			return nil, err
		}
	}
	return &sqlProducer{sqlPipe: p, conn: conn}, nil
}

//NewConsumer registers a new consumer with context
func (p *sqlPipe) NewConsumer(topic string) (Consumer, error) {
	t := p.cfg.SQL.Type
	if t == "" {
		t = mysql
	}
	conn, err := sql.Open(t, p.cfg.SQL.DSN)
	if err != nil {
		return nil, err
	}
	return &sqlConsumer{sqlPipe: p, conn: conn, topic: topic}, nil
}

func (p *sqlProducer) push(in interface{}) error {
	var bytes []byte
	switch b := in.(type) {
	case []byte:
		bytes = b
	default:
		return fmt.Errorf("SQL pipe can handle binary arrays only")
	}

	_, err := p.conn.Exec(string(bytes))
	return err
}

//PushK sends a keyed message
func (p *sqlProducer) PushK(key string, in interface{}) error {
	return p.push(in)
}

//Push produces a message
func (p *sqlProducer) Push(in interface{}) error {
	return p.push(in)
}

//PushBatch stashes a keyed message into batch which will be send to SQL by
//PushBatchCommit
func (p *sqlProducer) PushBatch(key string, in interface{}) (err error) {
	if p.tx == nil {
		p.tx, err = p.conn.Begin()
		if log.E(err) {
			return err
		}
	}
	var bytes []byte
	switch b := in.(type) {
	case []byte:
		bytes = b
	default:
		return fmt.Errorf("SQL pipe can handle binary arrays only")
	}
	_, err = p.tx.Exec(string(bytes))
	return err
}

//PushBatchCommit commits currently queued messages in the producer
func (p *sqlProducer) PushBatchCommit() error {
	if p.tx != nil {
		err := p.tx.Commit()
		p.tx = nil
		return err
	}
	return nil
}

func (p *sqlProducer) PushSchema(_ string, data []byte) (err error) {
	return util.ExecSQL(p.conn, string(data))
}

func pclose(conn *sql.DB, tx *sql.Tx, graceful bool) error {
	var err error
	if tx != nil {
		if graceful {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		tx = nil
	}
	err1 := conn.Close()
	conn = nil
	if err != nil {
		return err
	}
	return err1
}

// Close removes unfinished files
func (p *sqlProducer) Close() error {
	return pclose(p.conn, p.tx, true)
}

// CloseOnFailure removes unfinished files
func (p *sqlProducer) CloseOnFailure() error {
	return pclose(p.conn, p.tx, false)
}

//PartitionKey transforms input row key into partition key
func (p *sqlProducer) PartitionKey(source string, key string) string {
	if source == "snapshot" {
		return "snapshot"
	}
	return "log"
}

func (p *sqlProducer) SetFormat(_ string) {
}

func (p *sqlConsumer) initTable() error {
	var err error
	q := "`"
	if p.cfg.SQL.Type == postgres {
		q = `"`
	}
	tp := q + util.EscapeQuotes(p.topic, q[0]) + q
	for {
		p.rows, err = util.QuerySQL(p.conn, "SELECT * from "+tp)
		if err != nil {
			if strings.Contains(err.Error(), "doesn't exist") {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			return err
		}
		break
	}
	c, err := p.rows.Columns()
	if err != nil {
		return err
	}
	t, err := p.rows.ColumnTypes()
	if err != nil {
		return err
	}
	p.row = make([]interface{}, len(c))
	p.insert = "INSERT INTO " + tp + " ("
	for i, f := range c {
		if i != 0 {
			p.insert += ","
		}
		p.insert += f
		dt := strings.ToLower(t[i].DatabaseTypeName())
		if p.cfg.SQL.Type == postgres {
			p.row[i] = util.PostgresToDriverType(dt)
		} else if p.cfg.SQL.Type == clickhouse {
			p.row[i] = util.ClickHouseToDriverType(dt)
		} else { //default is mysql
			p.row[i] = util.MySQLToDriverType(dt, "")
		}
	}
	p.insert += ") VALUES ("
	p.inited = true
	return nil
}

func (p *sqlConsumer) Pop() (interface{}, error) {
	return p.msg, p.err
}

func (p *sqlConsumer) Close() error {
	if err := p.rows.Close(); err != nil {
		return err
	}
	return pclose(p.conn, nil, true)
}

func (p *sqlConsumer) CloseOnFailure() error {
	if err := p.rows.Close(); err != nil {
		return err
	}
	return pclose(p.conn, nil, false)
}

func encodeSQLValue(tp string, d interface{}) string {
	switch f := d.(type) {
	case *sql.NullInt64:
		if f.Valid {
			return fmt.Sprintf("%v", f.Int64)
		}
	case *sql.NullString:
		if f.Valid {
			p := "'"
			if tp == clickhouse {
				return p + f.String + p //scan returns already escaped string, probably driver issue
			} else if tp == postgres {
				p = "E" + p
			}
			return p + util.MySQLEscape(true, f.String) + "'"
		}
	case *sql.NullFloat64:
		if f.Valid {
			return fmt.Sprintf("%v", f.Float64)
		}
	case *sql.RawBytes:
		if f != nil {
			return "0x" + hex.EncodeToString([]byte(*f))
		}
	default:
		return fmt.Sprintf("%v", d)
	}
	return "NULL"
}

func (p *sqlConsumer) FetchNext() bool {
	if !p.inited {
		if p.err = p.initTable(); p.err != nil {
			return true
		}
	}
	if !p.rows.Next() {
		if p.err = p.rows.Err(); p.err != nil {
			return true
		}
		return false
	}
	if err := p.rows.Scan(p.row...); err != nil {
		p.err = err
		p.msg = nil
	}
	msg := p.insert
	for k, v := range p.row {
		if k != 0 {
			msg += ","
		}
		msg += encodeSQLValue(p.cfg.SQL.Type, v)
	}
	msg += ")"
	p.msg = []byte(msg)
	return true
}

func (p *sqlConsumer) SaveOffset() error {
	//TODO: implement
	return nil
}

func (p *sqlConsumer) SetFormat(_ string) {
}
