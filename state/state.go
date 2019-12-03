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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

// All the fields needed to be fetched from the state tables to prepare in-memory representation of state
const (
	stateSQL = "SELECT state.id, state.cluster, state.service, state.db, state.table_name, " +
		"state.input, state.output, state.output_format, state.version, cluster_state.gtid, cluster_state.seqno, cluster_state.updated_at cs_updated_at, " +
		"raw_schema.schema_gtid, raw_schema.raw_schema, state.snapshotted_at, state.deleted_at, state.need_snapshot, state.params " +
		"FROM state INNER JOIN cluster_state USING(cluster) INNER JOIN raw_schema ON raw_schema.state_id = state.id"
	regSQL        = "SELECT cluster, service, db, table_name, input, output, output_format, version, snapshotted_at, need_snapshot, created_at, updated_at, params FROM registrations WHERE deleted_at IS NULL"
	trxRetryCount = 5
)

//Row represents a row in the state table
type Row struct {
	types.TableLoc
	ID            int64
	OutputFormat  string
	Gtid          string
	GtidUpdatedAt time.Time
	SeqNo         uint64
	SchemaGtid    string
	RawSchema     string
	SnapshottedAt time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	NeedSnapshot  bool
	Deleted       bool
	ParamsRaw     string
	Params        *config.TableParams
}

//Type is in-memory representation of state
type Type []Row

// TimeForSnapshot determines if row requires snapshot to be taken
func (r *Row) TimeForSnapshot(now time.Time) bool {
	return r.SnapshottedAt.IsZero() || r.Params.Schedule.Interval != 0 && r.SnapshottedAt.Add(r.Params.Schedule.Interval).Before(now)
}

// SnapshotTimeChanged determines if row has SnapshottedAt updated
func (r *Row) SnapshotTimeChanged(prev time.Time) bool {
	return prev.Round(time.Second) != r.SnapshottedAt
}

func tableLocLog(t *types.TableLoc) log.Logger {
	return log.WithFields(log.Fields{"service": t.Service, "cluster": t.Cluster,
		"db": t.Db, "table": t.Table, "input": t.Input, "output": t.Output,
		"version": t.Version})
}

//GetDB returns active db connection to the state
func GetDB() *sql.DB {
	return mgr.conn
}

//GetDbAddr return low level address of the database: Host, Port, User, Password
func GetDbAddr() *db.Addr {
	return mgr.dbAddr
}

//GetNoDB returns active connection to the state database server
//Without connecting to any specific db
func GetNoDB() *sql.DB {
	return mgr.nodbconn
}

func readTableParams(rp sql.NullString, r *Row) error {
	r.ParamsRaw = ""
	if rp.Valid {
		r.ParamsRaw = rp.String
	}
	r.Params = &config.Get().TableParams //point to default
	if r.ParamsRaw != "" && r.ParamsRaw != "{}" {
		tp := config.Get().TableParams.CopyForMerge()
		if err := json.Unmarshal([]byte(r.ParamsRaw), &tp); err != nil {
			return err
		}
		tp.MergeCompound(r.Params)
		tp.Schedule.Interval *= time.Second
		r.Params = tp
	}
	return nil
}

func parseRows(rows *sql.Rows) (Type, error) {
	defer func() { log.E(rows.Close()) }()
	res := make(Type, 0)
	var r Row
	for rows.Next() {
		var rp sql.NullString
		var sn, del, guat mysql.NullTime
		if err := rows.Scan(&r.ID, &r.Cluster, &r.Service, &r.Db, &r.Table, &r.Input, &r.Output, &r.OutputFormat, &r.Version, &r.Gtid,
			&r.SeqNo, &guat, &r.SchemaGtid, &r.RawSchema, &sn, &del, &r.NeedSnapshot, &rp); err != nil {
			return nil, err
		}
		r.SnapshottedAt = time.Time{}
		if sn.Valid {
			r.SnapshottedAt = sn.Time
		}
		r.GtidUpdatedAt = time.Time{}
		if guat.Valid {
			r.GtidUpdatedAt = guat.Time
		}
		r.Deleted = false
		if del.Valid {
			r.Deleted = true
		}
		if err := readTableParams(rp, &r); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func parseRegRows(rows *sql.Rows) (Type, error) {
	defer func() { log.E(rows.Close()) }()
	res := make(Type, 0)
	var r Row
	var sa, ca, ua mysql.NullTime
	var rp sql.NullString
	var ns int
	for rows.Next() {
		if err := rows.Scan(&r.Cluster, &r.Service, &r.Db, &r.Table, &r.Input, &r.Output, &r.OutputFormat, &r.Version, &sa, &ns, &ca, &ua, &rp); err != nil {
			return nil, err
		}
		r.NeedSnapshot = false
		if ns > 0 {
			r.NeedSnapshot = true
		}
		r.SnapshottedAt = time.Time{}
		if sa.Valid {
			r.SnapshottedAt = sa.Time
		}
		if ca.Valid {
			r.CreatedAt = ca.Time
		}
		if ua.Valid {
			r.UpdatedAt = ua.Time
		}
		if err := readTableParams(rp, &r); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

//GetCondLow returns state rows with given condition in the state. Allows to
//select rows with deleted flag set
func GetCondLow(deleted bool, cond string, args ...interface{}) (Type, error) {
	if len(cond) != 0 {
		cond = " AND " + cond
	}
	sd := "IS"
	if deleted {
		sd = "IS NOT"
	}
	rows, err := util.QuerySQL(mgr.conn, stateSQL+" WHERE deleted_at "+sd+" NULL "+cond, args...)
	if err != nil {
		return nil, err
	}
	return parseRows(rows)
}

//GetCond returns state rows with given condition in the state, rows with deleted flags are ignored
func GetCond(cond string, args ...interface{}) (Type, error) {
	return GetCondLow(false, cond, args...)
}

//GetRegCond returns state rows with given condition in the state, rows with deleted flags are ignored
func GetRegCond(cond string, args ...interface{}) (Type, error) {
	if len(cond) != 0 && !strings.HasPrefix(cond, " LIMIT") {
		cond = " AND " + cond
	}
	rows, err := util.QuerySQL(mgr.conn, regSQL+cond, args...)
	if err != nil {
		return nil, err
	}
	return parseRegRows(rows)
}

//GetForCluster returns state rows for given cluster name
func GetForCluster(cluster string) (Type, error) {
	return GetCond("cluster=?", cluster)
}

//GetTable returns state rows for given service,cluster,db,table,input
func GetTable(service, cluster, db, table, input string, output string, version int) (*Row, error) {
	st, err := GetCond("service=? AND cluster=? AND db=? AND table_name=? AND input=? AND output=? AND version=?", service, cluster, db, table, input, output, version)
	if err != nil {
		return nil, err
	}
	if len(st) < 1 {
		return nil, nil
	}
	return &st[0], nil
}

//Get returns all the rows in the state corresponding to non-deleted tables
func Get() (Type, error) {
	return GetCond("")
}

//GetCount returns number of rows in the state
func GetCount(includeDeleted bool) (int, error) {
	query := "SELECT count(*) AS count FROM state"
	if !includeDeleted {
		query += " WHERE deleted_at IS NULL"
	}

	var cnt int
	err := util.QueryRowSQL(mgr.conn, query).Scan(&cnt)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return cnt, nil
}

//GetTableByID return state row for the given table id
func GetTableByID(id int64) (*Row, error) {
	rows, err := util.QuerySQL(mgr.conn, stateSQL+" WHERE state.id=? AND deleted_at IS NULL", id)
	if err != nil {
		return nil, err
	}

	tblRows, err := parseRows(rows)
	if err != nil {
		return nil, err
	}
	if len(tblRows) == 0 {
		return nil, nil
	}
	return &tblRows[0], nil
}

//GetGTID returns GTID saved in the state for given db locator
func GetGTID(cluster string) (gtid string, seqno int64, err error) {
	// Get first non empty gtid for the cluster
	err = util.QueryRowSQL(mgr.conn, "SELECT gtid, seqno FROM cluster_state WHERE cluster=?", cluster).Scan(&gtid, &seqno)
	return
}

//SetGTID saves given gtid for given db locator
func SetGTID(cluster, gtid string) error {
	return util.ExecSQL(mgr.conn, "UPDATE cluster_state SET gtid=? WHERE cluster=?", gtid, cluster)
}

//SaveBinlogState saves current state of the  binlog reader to the state DB.
//Binlog state is current GTID set and current seqNo
func SaveBinlogState(cluster, gtid string, seqNo uint64) error {
	return util.ExecSQL(mgr.conn, "UPDATE cluster_state SET gtid=?, seqno=? WHERE cluster=?", gtid, seqNo, cluster)
}

//GetNeedSnapshotFlag returns need_snapshot flag saved in the STATE
func GetNeedSnapshotFlag(id int64) (ns bool, err error) {
	err = util.QueryRowSQL(mgr.conn, "SELECT need_snapshot FROM state WHERE id=?", id).Scan(&ns)
	return ns, err
}

func getNeedSnapshotFlag(tx *sql.Tx, id int64) (regid int64, ns bool, err error) {
	err = util.QueryTxRowSQL(tx, "SELECT reg_id, need_snapshot FROM state WHERE id=? FOR UPDATE", id).Scan(&regid, &ns)
	return
}

func clearNeedSnapshotFlag(tx *sql.Tx, id int64, ts time.Time) error {
	regid, ns, err := getNeedSnapshotFlag(tx, id)
	if err != nil {
		return err
	}

	err = util.ExecTxSQL(tx, "UPDATE state SET need_snapshot=FALSE WHERE id=? AND snapshotted_at=?", id, ts)
	if err != nil {
		return err
	}
	sub := 0
	if ns { //Subtract from registrations only if we changed state value
		sub = 1
	}

	return util.ExecTxSQL(tx, "UPDATE registrations SET need_snapshot=IFNULL(need_snapshot,0)-? WHERE id=?", sub, regid)
}

func undeleteStateRow(tx *sql.Tx, id int64, outputFormat, params string) error {
	err := util.ExecTxSQL(tx, "UPDATE state SET snapshotted_at=NULL,need_snapshot=FALSE,deleted_at=NULL,output_format=?,params=? WHERE id=?", outputFormat, params, id)
	return err
}

func deleteStateRow(tx *sql.Tx, t *types.TableLoc) error {
	err := util.ExecTxSQL(tx, "UPDATE state SET deleted_at=CURRENT_TIMESTAMP WHERE service=? AND cluster=? AND db=? AND table_name=? AND input=? AND output=? AND version=?", t.Service, t.Cluster, t.Db, t.Table, t.Input, t.Output, t.Version)
	return err
}

func advanceSnapshottedAt(tx *sql.Tx, id int64, start time.Time) error {
	regid, ns, err := getNeedSnapshotFlag(tx, id)
	if err != nil {
		return err
	}
	err = util.ExecTxSQL(tx, "UPDATE state SET snapshotted_at=?, need_snapshot=TRUE WHERE id=? AND (snapshotted_at IS NULL OR snapshotted_at < ?)", start, id, start)
	if err != nil {
		return err
	}
	add := 0
	if !ns { //increase values in regs only if we increased it in state
		add = 1
	}

	return util.ExecTxSQL(tx, "UPDATE registrations SET snapshotted_at=?, need_snapshot=IFNULL(need_snapshot,0)+? WHERE id=? AND (snapshotted_at IS NULL OR snapshotted_at <= ?)", start, add, regid, start)
}

func updateSnapshottedAt(id int64, start time.Time) error {
	tx, err := mgr.conn.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	err = advanceSnapshottedAt(tx, id, start)
	if err != nil {
		return err
	}
	return tx.Commit()
}

//UpdateSnapshottedAt advances snapshotted_at to be no older then TableParams.Schedule.Interval from
//now
func UpdateSnapshottedAt(row *Row, tm time.Time) (*Row, error) {
	tm = tm.Round(time.Second)

	if row.TimeForSnapshot(tm) {
		err := updateSnapshottedAt(row.ID, tm)
		if log.E(err) {
			return nil, err
		}
		row, err = GetTableByID(row.ID)
		if log.E(err) {
			return nil, err
		}
		tm = time.Now().Round(time.Second)
	}

	return row, nil
}

//ClearNeedSnapshot clears flag indicating that streamer has taken a snapshot of table
func ClearNeedSnapshot(id int64, ts time.Time) error {
	tx, err := mgr.conn.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	err = clearNeedSnapshotFlag(tx, id, ts)
	if err != nil {
		return err
	}
	return tx.Commit()
}

//GetSchema returns structured schema saved in the state for give table
func GetSchema(svc, sdb, table, input string, output string, version int) (*types.TableSchema, error) {
	query := "SELECT column_name, ordinal_position, is_nullable, data_type, " +
		"character_maximum_length, numeric_precision, numeric_scale, column_type, " +
		"column_key from columns " +
		"INNER JOIN state ON state.id = columns.state_id " +
		"WHERE state.deleted_at is null AND state.db = ? AND state.table_name = ?  AND state.service = ? AND state.input = ? AND state.output = ? AND state.version = ? " +
		"GROUP BY service, db, table_name, input, output, version, column_name " +
		"ORDER BY ordinal_position"

	rows, err := mgr.conn.Query(query, sdb, table, svc, input, output, version)
	if err != nil {
		log.E(errors.Wrap(err, fmt.Sprintf("Error fetching column information for svc: %v, db: %v, table: %v",
			svc, sdb, table)))

		return nil, err
	}
	defer func() { log.E(rows.Close()) }()

	return schema.ParseColumnInfo(rows, sdb, table)
}

func getID(tx *sql.Tx, t *types.TableLoc) (id int64, schemaGTID string, needSnapshot bool, err error) {
	err = tx.QueryRow("SELECT id, need_snapshot, schema_gtid FROM state INNER JOIN raw_schema ON raw_schema.state_id = state.id "+
		"WHERE service=? AND cluster=? AND db=? AND table_name=? AND input=? AND output=? "+
		"AND version=? FOR UPDATE", t.Service, t.Cluster, t.Db, t.Table, t.Input, t.Output, t.Version).Scan(&id, &needSnapshot, &schemaGTID)
	if log.E(err) {
		return 0, "", false, err
	}
	return id, schemaGTID, needSnapshot, nil
}

func replaceStructuredSchema(tx *sql.Tx, id int64, s *types.TableSchema) error {
	if _, err := tx.Exec("DELETE FROM columns WHERE state_id=?", id); log.E(err) {
		return err
	}

	for _, c := range s.Columns {
		if _, err := tx.Exec("INSERT INTO columns(state_id,column_name,ordinal_position,is_nullable,data_type,character_maximum_length,numeric_precision,numeric_scale,column_type,column_key) VALUES (?,?,?,?,?,?,?,?,?,?)", id, c.Name,
			c.OrdinalPosition, c.IsNullable, c.DataType, c.CharacterMaximumLength, c.NumericPrecision, c.NumericScale,
			c.Type, c.Key); log.E(err) {
			return err
		}
	}

	return nil
}

func replaceRawSchema(tx *sql.Tx, oldGTID string, newGTID string, t *types.TableLoc, rawSchema string, s *types.TableSchema) (int64, error) {
	log.Debugf("Replacing raw schema for table %+v input=%v output=%v version=%v", t.Table, t.Input, t.Output, t.Version)
	var stateGTID string
	var id int64

	id, stateGTID, _, err := getID(tx, t)
	if err != nil {
		return 0, err
	}

	if oldGTID != "" && stateGTID != oldGTID {
		// This can happen because of the race condition between concurrent binlog
		// readers from the same master DB. This is just means that we are behind of him
		// and that's ok to skip persisting the schema, so as this version has been
		// persisted already by concurrent binlog reader earlier
		tableLocLog(t).WithFields(log.Fields{"current_gtid": oldGTID, "new_gtid": newGTID, "state_gtid": stateGTID}).Warnf("Newer schema version found in the state")
		return id, fmt.Errorf("newer schema found in state")
	}

	if _, err := tx.Exec("UPDATE raw_schema SET raw_schema=?, schema_gtid=? WHERE state_id=?", rawSchema, newGTID, id); log.E(err) {
		return 0, err
	}

	return id, nil
}

func clearClusterState(tx *sql.Tx, cluster string) error {
	if _, err := tx.Exec("UPDATE cluster_state SET gtid='' WHERE cluster=?", cluster); log.E(err) {
		return err
	}
	log.Infof("Old state cleared for cluster: %v", cluster)
	return nil
}

func insertNewSchema(tx *sql.Tx, newGTID string, t *types.TableLoc, regid int64, format, params, rawSchema string, s *types.TableSchema) (int64, error) {
	log.Debugf("Inserting schema for table %+v", t)

	if params == "" {
		params = "{}"
	}

	var cnt int64

	//Guarantee that row exists for given cluster
	if err := util.ExecSQL(mgr.conn, "INSERT IGNORE INTO cluster_state(cluster,gtid) VALUES (?,'')", t.Cluster); log.E(err) {
		return 0, err
	}

	//Lock the cluster row
	var unused string
	if err := tx.QueryRow("SELECT gtid FROM cluster_state WHERE cluster=? FOR UPDATE", t.Cluster).Scan(&unused); log.E(err) {
		return 0, err
	}

	if err := tx.QueryRow("SELECT COUNT(*) FROM state WHERE cluster=? AND deleted_at IS NULL", t.Cluster).Scan(&cnt); log.E(err) {
		return 0, err
	}

	//First table re-registered for the cluster clear old state
	if cnt == 0 {
		if err := clearClusterState(tx, t.Cluster); err != nil {
			return 0, err
		}
	}

	insRes, err := tx.Exec("INSERT INTO state (reg_id,service,cluster,db,table_name,input,output,version,output_format,params) VALUES (?,?,?,?,?,?,?,?,?,?)", regid, t.Service, t.Cluster, t.Db, t.Table, t.Input, t.Output, t.Version, format, params)
	if err != nil {
		if !isDuplicateKeyErr(err) {
			log.E(err)
			return 0, err
		}

		//This is only safe todo when changelog reader is catched up
		id, err := replaceRawSchema(tx, "", newGTID, t, rawSchema, s)
		if err != nil {
			return 0, err
		}

		if err = undeleteStateRow(tx, id, format, params); log.E(err) {
			return 0, err
		}

		return id, nil
	}

	id, err := insRes.LastInsertId()
	if log.E(err) {
		return 0, err
	}

	if _, err = tx.Exec("INSERT INTO raw_schema(state_id,schema_gtid,raw_schema) VALUES (?,?,?)",
		id, newGTID, rawSchema); log.E(err) {
		return 0, err
	}

	return id, nil
}

//replaceSchema replaces both structured and raw schema definitions saved in the
//state with new versions provided as parameters. If old GTID is empty adds it as new table to the state
func replaceSchemaLow(svc, cluster string, s *types.TableSchema, regid int64, rawSchema, oldGTID, newGTID, input, output string, version int, format string, params string) error {
	t := &types.TableLoc{Service: svc, Cluster: cluster, Db: s.DBName, Table: s.TableName, Input: input, Output: output, Version: version}

	tx, err := mgr.conn.Begin()
	if log.E(err) {
		return err
	}
	defer func() { _ = tx.Rollback() }() // noop if already committed

	var id int64

	if oldGTID != "" { //Changelog reader sees ALTER statement
		if id, err = replaceRawSchema(tx, oldGTID, newGTID, t, rawSchema, s); err != nil {
			return err
		}
	} else { //Table registered by API
		if id, err = insertNewSchema(tx, newGTID, t, regid, format, params, rawSchema, s); err != nil {
			return err
		}
	}

	if err = replaceStructuredSchema(tx, id, s); err != nil {
		return err
	}

	if err = tx.Commit(); log.E(err) {
		return err
	}

	tableLocLog(t).Debugf("Updated schema version from=%v, to=%v", oldGTID, newGTID)

	return nil
}

func replaceSchema(svc, cluster string, s *types.TableSchema, regid int64, rawSchema, oldGTID, newGTID, input, output string, version int, format string, params string) bool {

	for i := 0; i < trxRetryCount; i++ {
		err := replaceSchemaLow(svc, cluster, s, regid, rawSchema, oldGTID, newGTID, input, output, version, format, params)
		if err == nil {
			return true
		}
		if !isRetriableErr(err) {
			return false
		}
	}
	return false
}

//insertStateRow inserts new state row, updates and undelete if the row already
//exists in state
//used in new rable registration and updating existing through API
func insertStateRow(svc, cluster string, s *types.TableSchema, regid int64, rawSchema, oldGTID, newGTID, input, output string, version int, format string, params string) bool {
	return replaceSchema(svc, cluster, s, regid, rawSchema, oldGTID, newGTID, input, output, version, format, params)
}

//ReplaceSchema replaces both structured and raw schema definitions saved in the
//state with new versions provided as parameters
//Called from changelog reader on ALTER table statement
func ReplaceSchema(svc, cluster string, s *types.TableSchema, rawSchema, oldGTID, newGTID, input, output string, version int, format string, params string) bool {
	return replaceSchema(svc, cluster, s, 0, rawSchema, oldGTID, newGTID, input, output, version, format, params)
}

//TableRegistered checks if given table is listed in registrations table
func TableRegistered(svc, cluster, sdb, table, input, output string, version int, includeDeleted bool) (bool, error) {
	cluster, svc, sdb, table = SanitizeRegParams(cluster, svc, sdb, table, input)

	query := "SELECT COUNT(*) FROM registrations WHERE service=? AND cluster=? AND db=? AND table_name=? " +
		"AND input=? AND output=? AND version=?"
	if !includeDeleted {
		query += " AND deleted_at IS NULL"
	}

	var idRes int64
	if err := util.QueryRowSQL(mgr.conn, query, svc, cluster, sdb, table, input, output, version).Scan(&idRes); err != nil {
		return false, err
	}

	return idRes > 0, nil
}

//RegisterTable adds table for registration. The entry here is subsequently processed later on to update the state
//table
func RegisterTable(cluster, svc, sdb, table, input, output string, version int, outputFormat string, params string) bool {
	cluster, svc, sdb, table = SanitizeRegParams(cluster, svc, sdb, table, input)

	if params == "" {
		params = "{}"
	}

	if !ValidateRegistration(svc, sdb, table, input, output, version) {
		return false
	}

	err := util.ExecSQL(mgr.conn, "INSERT INTO registrations(cluster, service, db, table_name, input, output, version, output_format, params) "+
		"VALUES(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE deleted_at=NULL,snapshotted_at=NULL,need_snapshot=0,output_format=?,params=?,sync_state=?", cluster, svc, sdb, table, input, output, version, outputFormat, params, outputFormat, params, regStateUnsynced)
	if log.E(err) {
		return false
	}

	log.Infof("Registration added: cluster:%v service:%v db:%v tableName:%v, input:%v, output:%v, version:%v, format:%v, params:%v",
		cluster, svc, sdb, table, input, output, version, outputFormat, params)

	return true
}

//DeregisterTable removes given table from the state
func DeregisterTable(cluster, svc, sdb, table, input, output string, version int) bool {
	cluster, svc, sdb, table = SanitizeRegParams(cluster, svc, sdb, table, input)

	if !ValidateRegistration(svc, sdb, table, input, output, version) {
		return false
	}

	var args = make([]interface{}, 0)
	args = append(args, regStateUnsynced)

	var cond string
	cond, args = AddSQLCond(cond, args, "AND", "cluster", "=", cluster)
	cond, args = AddSQLCond(cond, args, "AND", "service", "=", svc)
	cond, args = AddSQLCond(cond, args, "AND", "db", "=", sdb)
	cond, args = AddSQLCond(cond, args, "AND", "table_name", "=", table)
	cond, args = AddSQLCond(cond, args, "AND", "input", "=", input)
	cond, args = AddSQLCond(cond, args, "AND", "output", "=", output)
	cond, args = AddSQLCond(cond, args, "AND", "version", "=", strconv.Itoa(version))

	if len(cond) > 0 {
		cond = " AND " + cond
	}

	if log.E(util.ExecSQL(mgr.conn, "UPDATE registrations SET deleted_at=CURRENT_TIMESTAMP, sync_state=? WHERE 1=1"+cond, args...)) {
		return false
	}

	log.Infof("Deregisteration successful, service: %v, cluster: %v, DB: %v, table: %v, input: %v, output: %v, version: %v",
		svc, cluster, sdb, table, input, output, version)
	return true
}

//ValidateRegistration validates the registration request
func ValidateRegistration(svc, sdb, table, input, output string, version int) bool {
	if input != types.InputMySQL {
		log.Errorf("Incorrect input type provided: %v", input)
		return false
	}

	if svc == "" {
		log.Errorf("Service name not provided or incorrect service: %v", svc)
		return false
	}

	if output == "" {
		log.Errorf("Incorrect output type provided: %v", output)
		return false
	}

	switch input {
	case types.InputMySQL:
		if sdb == "" {
			log.Errorf("DB name not provided or incorrect for input: %v, db: %v", types.InputMySQL, sdb)
			return false
		}
	}

	log.Debugf("Registration request valid for svc: %v, db: %v, table: %v, input: %v, version: %v", svc, sdb,
		table, input, version)

	return true
}

//SanitizeRegParams sanitizes the registration options
func SanitizeRegParams(cluster, svc, sdb, table, input string) (string, string, string, string) {
	cluster = strings.Trim(cluster, " *")
	svc = strings.Trim(svc, " *")
	sdb = strings.Trim(sdb, " *")
	table = strings.Trim(table, " *")

	return cluster, svc, sdb, table
}

//TableRegisteredInState checks if table with given id is still registered in the state
//ID is id field from row structure
func TableRegisteredInState(id int64) (bool, error) {
	var idRes sql.NullInt64
	err := util.QueryRowSQL(mgr.conn, "SELECT id FROM state WHERE id=? AND deleted_at IS NULL", id).Scan(&idRes)

	if err != nil || !idRes.Valid || idRes.Int64 != id {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

//PullCurrentSchema pulls current table schema from the source MySQL cluster
func PullCurrentSchema(dbl *db.Loc, table, input string) (*types.TableSchema, string) {
	ts, err := schema.Get(dbl, table, input)
	if log.E(err) {
		return nil, ""
	}

	rawSchema, err := schema.GetRaw(dbl, "`"+dbl.Name+"`.`"+table+"`", input)
	if log.E(err) {
		return nil, ""
	}

	return ts, rawSchema
}

//RegisterTableInState adds table to the state
func RegisterTableInState(dbl *db.Loc, table, input, output string, version int, format string, params string, regid int64) bool {
	sgtid, err := db.GetCurrentGTIDForDB(dbl, input)
	if log.E(err) {
		return false
	}

	ts, rawSchema := PullCurrentSchema(dbl, table, input)
	if ts == nil {
		return false
	}

	if !insertStateRow(dbl.Service, dbl.Cluster, ts, regid, rawSchema, "", sgtid, input, output, version, format, params) {
		return false
	}

	log.Infof("Registered table: %v, %v, %v, %v, %v, %v, %v", dbl.Service, dbl.Name, table, input, output,
		version, format)

	return true
}

//DeregisterTableFromState removes given table from the state
func DeregisterTableFromState(dbl *db.Loc, table, input, output string, version int, regid int64) bool {
	tx, err := mgr.conn.Begin()
	if log.E(err) {
		return false
	}
	defer func() { _ = tx.Rollback() }()
	err = deleteStateRow(tx, &types.TableLoc{Service: dbl.Service, Cluster: dbl.Cluster, Db: dbl.Name, Table: table, Input: input, Output: output, Version: version})
	if log.E(err) {
		return false
	}
	if err := tx.Commit(); log.E(err) {
		return false
	}
	return true
}

//Close uninitializes the state
func Close() error {
	log.Debugf("DB uninitialized")
	err := mgr.conn.Close()
	log.E(err)
	return err
}

// GetServerTimestamp fetches the current unix timestamp on the MySQL server
func GetServerTimestamp() (int64, error) {
	var res int64
	if err := util.QueryRowSQL(mgr.conn, "SELECT UNIX_TIMESTAMP()").Scan(&res); err != nil {
		return 0, err
	}

	return res, nil
}

// AddSQLCond is a helper function that helps with creating conditional queries over state tables
func AddSQLCond(cond string, args []interface{}, lop string, name string, op string, val string) (string, []interface{}) {
	if len(val) != 0 && val != "*" {
		if len(cond) != 0 && !strings.HasSuffix(cond, "(") {
			cond += lop + " "
		}
		cond += name + " " + op + " " + "? "
		args = append(args, val)
	}
	return cond, args
}

func isDuplicateKeyErr(err error) bool {
	return util.MySQLError(err, gmysql.ER_DUP_ENTRY)
}

func isRetriableErr(err error) bool {
	return util.MySQLError(err, gmysql.ER_LOCK_DEADLOCK) || util.MySQLError(err, gmysql.ER_LOCK_WAIT_TIMEOUT)
}

// TableMaxVersion returns maximum version for the table
func TableMaxVersion(svc, cluster, sdb, table, input, output string) (int, error) {
	cluster, svc, sdb, table = SanitizeRegParams(cluster, svc, sdb, table, input)

	query := "SELECT MAX(version) FROM registrations WHERE service=? AND cluster=? AND db=? AND table_name=? AND input=? AND output=?"

	var max int
	if err := util.QueryRowSQL(mgr.conn, query, svc, cluster, sdb, table, input, output).Scan(&max); err != nil {
		return 0, err
	}

	return max, nil
}

var cachedStateMySQLVersion atomic.Value

// CheckMySQLVersion check is state db cluster is of given version
func CheckMySQLVersion(expected string) bool {
	version, _ := cachedStateMySQLVersion.Load().(string)

	if version == "" {
		err := GetNoDB().QueryRow("SELECT @@global.version").Scan(&version)
		if log.E(err) {
			return false
		}
		cachedStateMySQLVersion.Store(version)
	}

	return strings.HasPrefix(version, expected)
}
