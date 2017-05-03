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
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
	"github.com/go-sql-driver/mysql"
)

type row struct {
	ID               int64
	Cluster          string
	Service          string
	Db               string
	Table            string
	IngestionVersion uint32
	Gtid             string
	SeqNo            uint64
	SchemaGtid       string
	RawSchema        string
	needBootstrap    bool
}

//Type is in-memory representation of state
type Type []row

var dbAddr *db.Addr
var conn *sql.DB
var nodbconn *sql.DB

//ConnectLow resolve address and connects to the state DB
//It can use state_connect_url config option, or using db.GetInfo resolver
//as any other database in the system
func ConnectLow(cfg *config.AppConfig, nodb bool) *sql.DB {
	if cfg.StateConnectURL != "" {
		cs := cfg.StateConnectURL
		/* url.Parse requires scheme in the beginning of the URL, just prepend
		* with random scheme if it wasn't in the config file URL */
		if !strings.Contains(cfg.StateConnectURL, "://") {
			cs = "dsn://" + cfg.StateConnectURL
		}
		u, err := url.Parse(cs)
		if log.E(err) {
			return nil
		}
		var host, port string = u.Host, ""
		if strings.Contains(u.Host, ":") {
			host, port, _ = net.SplitHostPort(u.Host)
		}
		if u.User.Username() == "" || host == "" {
			log.Errorf("Host and username required in DB db URL")
			return nil
		}
		if port == "" {
			port = "3306"
		}
		uport, err := strconv.ParseUint(port, 10, 16)
		if log.E(err) {
			return nil
		}
		pwd, _ := u.User.Password()
		dbAddr = &db.Addr{Host: host, Port: uint16(uport), User: u.User.Username(), Pwd: pwd, Db: types.MyDbName}
	} else {
		dbAddr = db.GetInfo(&db.Loc{Service: types.MySvcName, Name: types.MyDbName}, db.Master)
		if dbAddr == nil {
			return nil
		}
	}
	if nodb {
		dbAddr.Db = ""
	}
	conn, err := db.Open(dbAddr)
	if err == nil {
		return conn
	}
	return nil
}

//Connect connects to state DB. Returns false if connection failed
/* Exported for lock test */
func Connect(cfg *config.AppConfig) bool {
	conn = ConnectLow(cfg, false)
	if conn != nil {
		log.Debugf("Initialized and connected to state DB")
		return true
	}
	return false
}

//create database if necessary
func create(cfg *config.AppConfig) bool {
	nodbconn = ConnectLow(cfg, true)
	if nodbconn == nil {
		return false
	}
	log.Debugf("Creating state DB (if not exist)")
	err := util.ExecSQL(nodbconn, "CREATE DATABASE IF NOT EXISTS "+types.MyDbName)
	if err != nil {
		log.Errorf("State DB create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(nodbconn, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.state (
		id BIGINT NOT NULL AUTO_INCREMENT,
		cluster VARCHAR(128) NOT NULL,
		service VARCHAR(128) NOT NULL,
		db VARCHAR(128) NOT NULL,
		tableName VARCHAR(128) NOT NULL,
		ingestionVersion INT NOT NULL DEFAULT 1,
		gtid TEXT NOT NULL DEFAULT '',
		seqno BIGINT NOT NULL DEFAULT 0,
		schemaGTID TEXT NOT NULL DEFAULT '',
		rawSchema TEXT NOT NULL DEFAULT '',
		needBootstrap BOOLEAN NOT NULL DEFAULT TRUE,
		PRIMARY KEY(id),
		UNIQUE KEY(service, db, tableName),
		UNIQUE KEY(cluster, db, tableName)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("state table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(nodbconn, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.columns (
		service varchar(64) NOT NULL DEFAULT '',
		TABLE_SCHEMA varchar(64) NOT NULL DEFAULT '',
		TABLE_NAME varchar(64) NOT NULL DEFAULT '',
		COLUMN_NAME varchar(64) NOT NULL DEFAULT '',
		ORDINAL_POSITION bigint(21) unsigned NOT NULL DEFAULT '0',
		IS_NULLABLE varchar(3) NOT NULL DEFAULT '',
		DATA_TYPE varchar(64) NOT NULL DEFAULT '',
		CHARACTER_MAXIMUM_LENGTH bigint(21) unsigned DEFAULT NULL,
		NUMERIC_PRECISION bigint(21) unsigned DEFAULT NULL,
		NUMERIC_SCALE bigint(21) unsigned DEFAULT NULL,
		COLUMN_TYPE longtext NOT NULL,
		COLUMN_KEY varchar(3) NOT NULL DEFAULT '',
		PRIMARY KEY(service, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("schema table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(nodbconn, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.clusters (
		name varchar(128) NOT NULL,
		host varchar(128) NOT NULL,
		port int NOT NULL DEFAULT 3306,
		user varchar(64) NOT NULL,
		password varchar(64) NOT NULL,
		type varchar(32) NOT NULL DEFAULT 'slave',
		primary key(name)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("db info table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(nodbconn, `CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.outputSchema (
		name varchar(128) NOT NULL,
		schemaBody TEXT NOT NULL,
		primary key(name)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("schema table create failed: " + err.Error())
		return false
	}
	log.Debugf("State DB initialized")
	return true
}

//Init does complete state initialization.
//Create and connect to database
func Init(cfg *config.AppConfig) bool {
	return create(cfg) && Connect(cfg)
}

//GetDbAddr return low level address of the database: Host, Port, User, Password
func GetDbAddr() *db.Addr {
	return dbAddr
}

//GetDB returns active db connection to the state
func GetDB() *sql.DB {
	return conn
}

//GetNoDB returns active connection to the state database server
//Without connecting to any specific db
func GetNoDB() *sql.DB {
	return nodbconn
}

//GetVersion track the state changes
//Return value changes when something got changed in the state,
//new tables inserted or removed
//It doesn't guarantee that return value will increase monotonically
func GetVersion() int64 {
	var version sql.NullInt64
	var count int64
	err := util.QueryRowSQL(conn, "SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+types.MyDbName+"' AND TABLE_NAME = 'state'").Scan(&version)
	if err != nil {
		log.Errorf("GetVersion: %v", err.Error())
		return 0
	}
	if !version.Valid {
		log.Errorf("GetVersion:  schema is broken. NO AUTO_INCREMENT field in state table")
		return 0
	}
	err = util.QueryRowSQL(conn, "SELECT COUNT(*) FROM state").Scan(&count)
	if log.E(err) {
		return 0
	}
	/*FIXME: Next formula limits number of possible tables in the state to 16M */
	return (version.Int64 << 24) | count
}

func parseRows(rows *sql.Rows) (Type, error) {
	defer func() { log.E(rows.Close()) }()
	res := make(Type, 0)
	var r row
	for rows.Next() {
		if err := rows.Scan(&r.ID, &r.Cluster, &r.Service, &r.Db, &r.Table, &r.IngestionVersion, &r.Gtid, &r.SeqNo, &r.SchemaGtid, &r.RawSchema, &r.needBootstrap); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

//GetCond returns rows with given condition in the state
func GetCond(cond string, args ...interface{}) (Type, error) {
	if len(cond) != 0 {
		cond = " WHERE " + cond
	}
	rows, err := util.QuerySQL(conn, "SELECT * FROM state"+cond, args...)
	if err != nil {
		return nil, err
	}
	return parseRows(rows)
}

//GetForCluster returns state rows for given cluster name
func GetForCluster(cluster string) (Type, error) {
	return GetCond("cluster=?", cluster)
}

//Get returns all the rows in the state
func Get() (Type, error) {
	rows, err := util.QuerySQL(conn, "SELECT * FROM state")
	if err != nil {
		return nil, err
	}
	return parseRows(rows)
}

//GetCount returns number of rows in the state
func GetCount() (int, error) {
	var cnt int
	err := util.QueryRowSQL(conn, "SELECT count(*) AS count FROM state").Scan(&cnt)
	if err != nil {
		return 0, err
	}
	return cnt, nil
}

//TableRegistered checks if table with given id is still registered in the state
//Table id is id field from row structure
func TableRegistered(id int64) (bool, error) {
	var idRes sql.NullInt64
	err := util.QueryRowSQL(conn, "SELECT id FROM state WHERE id=?", id).Scan(&idRes)
	if err != nil || !idRes.Valid || idRes.Int64 != id {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

//GetTable return state row for the given table id
func GetTable(id int64) (Type, error) {
	rows, err := util.QuerySQL(conn, "SELECT * FROM state WHERE id=?", id)
	if err != nil {
		return nil, err
	}
	return parseRows(rows)
}

//GetGTID returns GTID saved in the state for given db locator
func GetGTID(d *db.Loc) (gtid string, err error) {
	/* Get first non empty gtid for the db */
	err = util.QueryRowSQL(conn, "SELECT gtid FROM state WHERE cluster=? ORDER BY gtid DESC LIMIT 1", d.Cluster).Scan(&gtid)
	return
}

//SetGTID saves given gtid for given db locator
func SetGTID(d *db.Loc, gtid string) error {
	return util.ExecSQL(conn, "UPDATE state SET gtid=? WHERE cluster=?", gtid, d.Cluster)
}

//GetTableNewFlag returns the flag indicating that table hasn't been snapshotted
//by streamer yet
//FIXME: May be replace service,db,table with db.Loc
func GetTableNewFlag(service string, db string, table string) (n bool, err error) {
	err = util.QueryRowSQL(conn, "SELECT needBootstrap FROM state WHERE service=? AND db=? AND tableName=?", service, db, table).Scan(&n)
	return
}

//SetTableNewFlag sets flag indicating that tables has been snapshotted by
//streamer
func SetTableNewFlag(service string, db string, table string, flag bool) error {
	return util.ExecSQL(conn, "UPDATE state SET needBootstrap=? WHERE service=? AND db=? AND tableName=?", flag, service, db, table)
}

//SaveBinlogState saves current state of the  binlog reader to the state DB
//binlog state is current GTID set and current seqNo
func SaveBinlogState(d *db.Loc, gtid string, seqNo uint64) error {
	return util.ExecSQL(conn, "UPDATE state SET gtid=?, seqno=? WHERE cluster=?", gtid, seqNo, d.Cluster)
}

//GetSchema return structured schema saved in the state for give table
func GetSchema(svc string, sdb string, table string) (*types.TableSchema, error) {
	return schema.GetColumns(conn, sdb, table, "columns", " AND service='"+svc+"'")
}

//ReplaceSchema replaces both dtructured and raw schema definitions saved in the
//state with new versions provided as parameters
//If oldGtid is empty adds it as new table to the state
//FIXME: To big function. Split it.
func ReplaceSchema(svc string, cluster string, s *types.TableSchema, rawSchema string, oldGtid string, gtid string) bool {
	tx, err := conn.Begin()
	if log.E(err) {
		return false
	}

	if oldGtid != "" {
		log.Debugf("Replacing schema for table %+v", s.TableName)
		var sgtid string
		err = tx.QueryRow("SELECT schemaGtid FROM state WHERE service=? AND db=? AND tableName=? FOR UPDATE", svc, s.DBName, s.TableName).Scan(&sgtid)
		if log.E(err) {
			return false
		}

		if sgtid != oldGtid {
			/* This can happened because of the race condition between concurrent binlog
			* readers from the same master DB. This is just means that we are behind of him
			* and that's ok to skip persisting the schema, so as this version has been
			* persisted already by concurrent binlog reader earlier */
			log.Warnf("Newer schema version found in the state, my: (current: %v, new: %v), state: %v. service=%v, db=%v, table=%v", oldGtid, gtid, sgtid, svc, s.DBName, s.TableName)
			log.E(tx.Rollback())
			return true
		}

		if _, err = tx.Exec("UPDATE state SET rawSchema=?, schemaGtid=? WHERE service=? AND db=? AND tableName=?", rawSchema, gtid, svc, s.DBName, s.TableName); log.E(err) {
			return false
		}
	} else {
		log.Debugf("Inserting schema for table %+v", s.TableName)
		if _, err := tx.Exec("INSERT INTO state (service, cluster, db, tableName, schemaGTID, rawSchema) VALUES (?, ?, ?, ?, ?, ?)", svc, cluster, s.DBName, s.TableName, gtid, rawSchema); err != nil {
			if err.(*mysql.MySQLError).Number == 1062 { //Duplicate key
				log.Warnf("Newer schema version found in the state, my: (current: %v, new: %v), state: ?. service=%v, db=%v, table=%v", "", gtid, svc, s.DBName, s.TableName)
				log.E(tx.Rollback())
				return true
			}
			log.E(err)
			return false
		}
	}

	if _, err = tx.Exec("DELETE FROM columns WHERE service=? AND table_schema=? AND table_name=?", svc, s.DBName, s.TableName); log.E(err) {
		return false
	}

	for _, c := range s.Columns {
		if _, err := tx.Exec("INSERT INTO columns VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", svc, s.DBName, s.TableName, c.Name, c.OrdinalPosition, c.IsNullable, c.DataType, c.CharacterMaximumLength, c.NumericPrecision, c.NumericScale, c.Type, c.Key); log.E(err) {
			return false
		}
	}

	if err = tx.Commit(); log.E(err) {
		return false
	}

	log.Debugf("Updated schema version from=%v, to=%v for service=%v, db=%v, table=%v", oldGtid, gtid, svc, s.DBName, s.TableName)

	return true
}

//RegisterTable adds table to the state
func RegisterTable(dbl *db.Loc, table string) bool {
	ts, err := schema.Get(dbl, table)
	if log.E(err) {
		return false
	}

	sgtid, err := GetCurrentGTIDForDB(dbl)
	if log.E(err) {
		return false
	}

	rawSchema, err := schema.GetRaw(dbl, "`"+dbl.Name+"`.`"+table+"`")
	if log.E(err) {
		return false
	}

	if !ReplaceSchema(dbl.Service, dbl.Cluster, ts, rawSchema, "", sgtid) {
		return false
	}

	log.Debugf("Registered table: %+v, %v", dbl, table)
	return true
}

//DeregisterTable removes given table from the state
func DeregisterTable(svc string, sdb string, table string) bool {
	if log.E(util.ExecSQL(conn, "DELETE FROM state WHERE service=? AND db=? AND tableName=?", svc, sdb, table)) {
		return false
	}

	if log.E(util.ExecSQL(conn, "DELETE FROM columns WHERE service=? AND table_schema=? AND table_name=?", svc, sdb, table)) {
		return false
	}

	log.Debugf("Deregistered table: %v, %v, %v", svc, sdb, table)
	return true
}

//InsertClusterInfo adds connection information for the cluster "name" to state
func InsertClusterInfo(name string, ci *db.Addr) error {
	err := util.ExecSQL(conn, "INSERT INTO clusters(name,host,port,user,password) VALUES(?, ?, ?, ?, ?)", name, ci.Host, ci.Port, ci.User, ci.Pwd)
	if log.E(err) {
		return err
	}
	log.Debugf("Cluster added: name:%v Host:%v Port:%v User:%v", name, ci.Host, ci.Port, ci.User)
	return nil
}

//DeleteClusterInfo delete cluster connection onfo from state database
func DeleteClusterInfo(name string) error {
	err := util.ExecSQL(conn, "DELETE FROM clusters WHERE name=?", name)
	if log.E(err) {
		return err
	}
	log.Debugf("Cluster deleted: %+v", name)
	return nil
}

//ConnectInfoGet resolves database address using state clusters table
func ConnectInfoGet(l *db.Loc, tp int) *db.Addr {
	var c string
	var a db.Addr

	//FIXME: Add connection type to info in state

	if conn == nil {
		log.Debugf("State hasn't been initialized yet")
		return nil
	}

	c = l.Cluster
	if l.Cluster == "" {
		err := util.QueryRowSQL(conn, "SELECT cluster FROM state WHERE service=? AND db=?", l.Service, l.Name).Scan(&c)
		if err != nil && err != sql.ErrNoRows {
			log.E(err)
			return nil
		}
		if c != "" {
			log.Debugf("Cluster name resolved from state: %v by service=%v and db=%v", c, l.Service, l.Name)
		}
	}

	//FIXME:Select by type
	err := util.QueryRowSQL(conn, "SELECT name,host,port,user,password FROM clusters WHERE name=?", c).Scan(&c, &a.Host, &a.Port, &a.User, &a.Pwd)
	if log.E(err) {
		return nil
	}

	if l.Cluster != "" && c != l.Cluster {
		log.Errorf("Cluster name mismatch, given: %v, in state %v", l.Cluster, c)
		return nil
	}

	a.Db = l.Name

	return &a
}

//InsertSchema inserts output schema into state
func InsertSchema(name string, schema string) error {
	err := util.ExecSQL(conn, "INSERT INTO outputSchema VALUES(?, ?)", name, schema)
	if log.E(err) {
		return err
	}
	log.Debugf("Schema added: name:%v schema:%v", name, schema)
	return nil
}

//UpdateSchema inserts output schema into state
func UpdateSchema(name string, schema string) error {
	err := util.ExecSQL(conn, "UPDATE outputSchema SET schemaBody=? WHERE name=?", schema, name)
	if log.E(err) {
		return err
	}
	log.Debugf("Schema updated: name:%v schema:%v", name, schema)
	return nil
}

//DeleteSchema deletes output schema from the state
func DeleteSchema(name string) error {
	err := util.ExecSQL(conn, "DELETE FROM outputSchema WHERE name=?", name)
	if log.E(err) {
		return err
	}
	log.Debugf("Schema deleted: %+v", name)
	return nil
}

//GetOutputSchema returns output schema from the state
func GetOutputSchema(name string) string {
	var body string

	err := util.QueryRowSQL(conn, "SELECT schemaBody FROM outputSchema WHERE name=?", name).Scan(&body)

	if err != nil {
		if err.Error() != "sql: no rows in result set" {
			log.E(err)
		}
		return ""
	}
	log.Debugf("Return output schema from state: %v", body)
	return body
}

//Close deinitializes the state
func Close() error {
	log.Debugf("DB deinitialized")
	err := conn.Close()
	log.E(err)
	return err
}

//GetCurrentGTID returns current gtid set for the specified db address
//(host,port,user,password)
//FIXME: move to db package
func GetCurrentGTID(a *db.Addr) (gtid string, err error) {
	var d *sql.DB
	if d, err = db.Open(a); err == nil {
		err = d.QueryRow("SELECT @@global.gtid_executed").Scan(&gtid)
		log.E(d.Close())
	}
	return
}

//GetCurrentGTIDForDB return current gtid set for the db specified by
//db locator (cluster,service,db)
//FIXME: move to db package
func GetCurrentGTIDForDB(l *db.Loc) (string, error) {
	if a := db.GetInfo(l, db.Slave); a != nil {
		return GetCurrentGTID(a)
	}
	return "", errors.New("Error resolving db info")
}
