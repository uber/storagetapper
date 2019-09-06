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
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/lock"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
	"golang.org/x/net/context" //"context"
	"golang.org/x/sync/semaphore"
)

const (
	// regSyncInterval is the time interval after which to fetch information about
	// registrations and update the state table
	regSyncInterval = 10 * time.Minute

	// regSyncConcurrency controls the concurrency of the registration and deregistration
	// sync requests
	regSyncConcurrency = 32

	regSyncSlack = 1 * time.Minute

	// lockName is used to identify the lock used by state when syncing state information
	lockName = "state_sync_lock"

	maxLockWait = 1 * time.Minute
)

// Registration sync state for the entries in the registrations table
const (
	regStateUnsynced = iota
	regStateSynced
	regStateNotFound
)

// Mgr is a state manager that controls all the state handling
type Mgr struct {
	conn        *sql.DB
	nodbconn    *sql.DB
	dbAddr      *db.Addr
	shutdownCtx context.Context
	metrics     *metrics.State
}

var mgr *Mgr

// InitManager creates a new state manager
func InitManager(ctx context.Context, cfg *config.AppConfig) error {
	log.Infof("Initializing state manager")

	mgr = &Mgr{shutdownCtx: ctx}
	var err error

	mgr.dbAddr, err = mgr.getDBAddr(cfg)
	if log.E(err) {
		return err
	}

	// First create a connection to be used to initialize the DB
	dbAddr2 := *mgr.dbAddr
	dbAddr2.Db = ""
	if mgr.nodbconn, err = db.Open(&dbAddr2); err != nil {
		return err
	}

	if !mgr.create() {
		return fmt.Errorf("failed to create state DB")
	}

	// Now let's create the main connection used to interface with the state DB
	if mgr.conn, err = db.Open(mgr.dbAddr); err != nil {
		return err
	}

	mgr.metrics = metrics.NewStateMetrics()

	mgr.startStateRegSync()

	return nil
}

// Reset resets the state tables and deletes all the existing data
func Reset() bool {
	return mgr.truncate()
}

// getConnection resolve address and connects to the state DB. It can use state_connect_url config option, or use
// db package's resolver as any other database in the system
func (m *Mgr) getDBAddr(cfg *config.AppConfig) (*db.Addr, error) {
	var err error
	var dbAddr *db.Addr

	if cfg.StateConnectURL != "" {
		cs := cfg.StateConnectURL

		// url.Parse requires scheme in the beginning of the URL, just prepend
		// with random scheme if it wasn't in the config file URL
		if !strings.Contains(cfg.StateConnectURL, "://") {
			cs = "dsn://" + cfg.StateConnectURL
		}
		u, err := url.Parse(cs)
		if err != nil {
			return nil, err
		}

		var host, port = u.Host, ""
		if strings.Contains(u.Host, ":") {
			host, port, _ = net.SplitHostPort(u.Host)
		}
		if u.User.Username() == "" || host == "" {
			return nil, fmt.Errorf("host and username required in DB db URL")
		}

		if port == "" {
			port = "3306"
		}
		uPort, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return nil, err
		}

		pwd, _ := u.User.Password()
		dbAddr = &db.Addr{Host: host, Port: uint16(uPort), User: u.User.Username(), Pwd: pwd, Db: types.MyDbName}
	} else {
		dbAddr, err = db.GetConnInfo(&db.Loc{Service: types.MySvcName, Name: types.MyDbName, Cluster: types.MyClusterName}, db.Master,
			types.InputMySQL)
		if err != nil {
			return nil, err
		}
	}

	return dbAddr, nil
}

//create database if necessary
func (m *Mgr) create() bool {
	log.Debugf("Creating state DB (if not exist)")
	err := util.ExecSQL(m.nodbconn, "CREATE DATABASE IF NOT EXISTS "+types.MyDbName+" DEFAULT CHARACTER SET latin1")
	if err != nil {
		log.Errorf("State DB create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(m.nodbconn, `
	CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.state (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,

		service    VARCHAR(128) NOT NULL,
		cluster    VARCHAR(128) NOT NULL,
		db         VARCHAR(128) NOT NULL,
		table_name VARCHAR(128) NOT NULL,
		input      VARCHAR(128) NOT NULL,
		output     VARCHAR(128) NOT NULL,
		version    INT NOT NULL DEFAULT 0,

		output_format VARCHAR(128) NOT NULL,

		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		deleted_at TIMESTAMP NULL,

		snapshotted_at TIMESTAMP NULL,
		need_snapshot  BOOLEAN DEFAULT FALSE,

		params JSON,

		reg_id BIGINT NOT NULL, /* references registrations.id */

		locked_at TIMESTAMP NULL,
		worker_id VARCHAR(128),

		UNIQUE KEY(service, db, table_name, input, output, version, cluster),
		KEY(cluster),
		KEY(locked_at),
		KEY(input),
		KEY(snapshotted_at)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("state table create failed %v. db: %v ", err, types.MyDbName)
		return false
	}
	err = util.ExecSQL(m.nodbconn, `
	CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.cluster_state (
		cluster VARCHAR(128) NOT NULL PRIMARY KEY,
		gtid    TEXT NOT NULL,
		seqno   BIGINT NOT NULL DEFAULT 0,

		locked_at TIMESTAMP NULL,
		worker_id VARCHAR(128),

		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

		KEY(locked_at)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("cluster_state table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(m.nodbconn, `
	CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.raw_schema (
		state_id    BIGINT NOT NULL PRIMARY KEY,
		schema_gtid TEXT NOT NULL,
		raw_schema  TEXT NOT NULL,

		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("raw_schema table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(m.nodbconn, `
	CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.columns (
		state_id BIGINT NOT NULL,

		column_name              VARCHAR(64) NOT NULL DEFAULT '',
		ordinal_position         BIGINT(21) unsigned NOT NULL DEFAULT '0',
		is_nullable              VARCHAR(3) NOT NULL DEFAULT '',
		data_type                VARCHAR(64) NOT NULL DEFAULT '',
		character_maximum_length BIGINT(21) unsigned DEFAULT NULL,
		numeric_precision        BIGINT(21) unsigned DEFAULT NULL,
		numeric_scale            BIGINT(21) unsigned DEFAULT NULL,
		column_type              LONGTEXt NOT NULL,
		column_key               VARCHAR(3) NOT NULL DEFAULT '',

		PRIMARY KEY(state_id, column_name)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("columns table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(m.nodbconn, `
	CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.clusters (
		name     VARCHAR(128) NOT NULL PRIMARY KEY,
		host     VARCHAR(128) NOT NULL,
		port     INT NOT NULL DEFAULT 3306,
		user     VARCHAR(64) NOT NULL,
		password VARCHAR(64) NOT NULL,
		type     VARCHAR(32) NOT NULL DEFAULT 'slave'
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("clusters table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(m.nodbconn, `
	CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.output_schema (
		name        VARCHAR(128) NOT NULL,
		type        VARCHAR(64) NOT NULL,
		schema_body TEXT NOT NULL,

		PRIMARY KEY(name, type)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("outputSchema table create failed: " + err.Error())
		return false
	}
	err = util.ExecSQL(m.nodbconn, `
	CREATE TABLE IF NOT EXISTS `+types.MyDbName+`.registrations (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,

		service    VARCHAR(128) NOT NULL,
		cluster    VARCHAR(128) NOT NULL,
		db         VARCHAR(128) NOT NULL,
		table_name VARCHAR(128) NOT NULL,
		input      VARCHAR(128) NOT NULL,
		output     VARCHAR(128) NOT NULL,
		version    INT NOT NULL DEFAULT 0,

		output_format VARCHAR(128) NOT NULL,

		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		deleted_at TIMESTAMP NULL,

		snapshotted_at TIMESTAMP NULL,
		need_snapshot  INT DEFAULT 0, /* or pending_snapshot? */

		params         TEXT,

		sync_state TINYINT NOT NULL DEFAULT 0,

		UNIQUE KEY(service, cluster, db, table_name, input, output, version)
	) ENGINE=INNODB`)
	if err != nil {
		log.Errorf("registrations table create failed: " + err.Error())
		return false
	}
	log.Debugf("State DB initialized")
	return true
}

//truncate drops and recreates empty state tables
func (m *Mgr) truncate() bool {
	addr := *mgr.dbAddr
	addr.Db = ""
	l := lock.Create(&addr)
	if !l.Lock(lockName, maxLockWait) {
		log.Warnf("Could not acquire lock on state tables")
		return false
	}
	defer l.Close()

	err := util.ExecSQL(m.nodbconn, `DROP DATABASE IF EXISTS `+types.MyDbName)
	if log.E(err) {
		return false
	}

	return m.create()
}

// startStateRegSync starts the state registrations tables sync loop
func (m *Mgr) startStateRegSync() {
	go func() {
		// Add a randomized slack before running the sync so that
		// different nodes start off the sync at different times
		ticker := time.NewTicker(regSyncInterval + getSyncIntervalSlack())
		defer ticker.Stop()

		addr := *mgr.dbAddr
		addr.Db = ""
		l := lock.Create(&addr)

		m.regSyncRunOnce(l)
		for {
			select {
			case <-ticker.C:
				m.regSyncRunOnce(l)
			case <-m.shutdownCtx.Done():
				log.Infof("Registration sync cancelled")
				return
			}
		}
	}()
}

// regSyncRunOnce performs a one time of sync of state and registrations tables
func (m *Mgr) regSyncRunOnce(l lock.Lock) {
	log.Debugf("Starting the state registration sync")

	if !l.Lock(lockName, maxLockWait) {
		log.Debugf("Could not acquire lock on state tables")
		return
	}
	defer l.Close()

	m.metrics.Sync.Inc()
	defer func(st time.Time) {
		m.metrics.SyncDuration.Record(time.Since(st))
		m.metrics.Sync.Dec()
	}(time.Now())

	log.Debugf("Lock acquired on state tables")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		SyncRegisteredTables()
	}()

	go func() {
		defer wg.Done()
		SyncDeregisteredTables()
	}()

	wg.Wait()

	EmitRegisteredTablesCount()

	log.Debugf("Completed the state registration sync")
}

// SyncRegisteredTables registers the tables based on the registration requests that were persisted in the
// registrations table
func SyncRegisteredTables() bool {
	log.Debugf("Syncing registered tables")

	rs1, err := util.QuerySQL(mgr.conn, "SELECT id, service, cluster, db, table_name, input, output, "+
		"version, output_format, params FROM registrations WHERE deleted_at IS NULL AND sync_state=?", regStateUnsynced)
	if log.E(err) {
		return false
	}
	defer func() { log.E(rs1.Close()) }()

	for rs1.Next() {
		var r Row
		var params sql.NullString
		if err := rs1.Scan(&r.ID, &r.Service, &r.Cluster, &r.Db, &r.Table, &r.Input, &r.Output, &r.Version, &r.OutputFormat, &params); err != nil {
			log.E(errors.Wrap(err, "Failed to scan rows to register while syncing state"))
			continue
		}
		r.ParamsRaw = params.String
		syncRegistered(&r)
	}

	if err := rs1.Err(); err != nil {
		log.E(errors.Wrap(err, "Error while scanning rows to register"))
		return false
	}

	log.Debugf("Finished syncing registered tables")

	return true
}

func syncRegistered(r *Row) {
	mgr.metrics.TableReg.Inc()
	defer func(st time.Time) {
		mgr.metrics.TableRegDuration.Record(time.Since(st))
		mgr.metrics.TableReg.Dec()
	}(time.Now())

	enumerator, err := db.GetEnumerator(r.Service, r.Cluster, r.Db, r.Table, r.Input)
	if log.E(err) {
		return
	}

	var tablesTotal, tablesSuccess uint64
	sem := semaphore.NewWeighted(regSyncConcurrency)

	for ; enumerator.Next(); tablesTotal++ {
		_ = sem.Acquire(context.Background(), 1)

		go func(dbl *db.Loc) {
			defer sem.Release(1)
			if RegisterTableInState(dbl, r.Table, r.Input, r.Output, r.Version, r.OutputFormat, r.ParamsRaw, r.ID) {
				atomic.AddUint64(&tablesSuccess, 1)
			} else {
				tableLocLog(&r.TableLoc).Errorf("Failed to register table in state")
				mgr.metrics.SyncErrors.Inc(1)
			}
		}(enumerator.Value())
	}

	_ = sem.Acquire(context.Background(), regSyncConcurrency)

	if tablesTotal != 0 && tablesSuccess == tablesTotal {
		if err = util.ExecSQL(mgr.conn, "UPDATE registrations SET sync_state=? WHERE id=?", regStateSynced, r.ID); err != nil {
			log.E(errors.Wrap(err, "Failed to update sync state in registrations"))
			mgr.metrics.SyncErrors.Inc(1)
		}
	}
}

// SyncDeregisteredTables deregisters the tables based on the deregistration requests that were persisted in the
// registrations table
//TODO: Deduplicate with SyncRegisteredTables
func SyncDeregisteredTables() bool {
	log.Debugf("Syncing deregistered tables")

	rs1, err := util.QuerySQL(mgr.conn, "SELECT id, cluster, service, db, table_name, input, output, version, output_format "+
		"FROM registrations WHERE deleted_at IS NOT NULL AND sync_state=?", regStateUnsynced)
	if log.E(err) {
		return false
	}
	defer func() { log.E(rs1.Close()) }()

	for rs1.Next() {
		var r Row
		if err := rs1.Scan(&r.ID, &r.Cluster, &r.Service, &r.Db, &r.Table, &r.Input, &r.Output, &r.Version, &r.OutputFormat); err != nil {
			log.E(errors.Wrap(err, "Failed to scan rows to deregister while syncing state"))
			continue
		}

		syncDeregistered(&r)
	}

	if err := rs1.Err(); err != nil {
		log.E(errors.Wrap(err, "Error while scanning rows to deregister"))
		return false
	}

	log.Debugf("Finished syncing deregistered tables")

	return true
}

func syncDeregistered(r *Row) {
	mgr.metrics.TableDereg.Inc()
	defer func(st time.Time) {
		mgr.metrics.TableDeregDuration.Record(time.Since(st))
		mgr.metrics.TableDereg.Dec()
	}(time.Now())

	var tablesTotal, tablesSuccess uint64
	syncedState := regStateSynced

	enumerator, err := db.GetEnumerator(r.Service, r.Cluster, r.Db, r.Table, r.Input)
	if err != nil {
		if err.Error() == "DB not found" && r.Cluster != "" && r.Db != "" {
			dbl := &db.Loc{Service: r.Service, Cluster: r.Cluster, Name: r.Db}
			if DeregisterTableFromState(dbl, r.Table, r.Input, r.Output, r.Version, r.ID) {
				syncedState = regStateNotFound
				tablesTotal = 1
				tablesSuccess = 1
			} else {
				tableLocLog(&r.TableLoc).Errorf("Failed to deregister")
				mgr.metrics.SyncErrors.Inc(1)
			}
		} else {
			log.E(err)
		}
	} else {
		sem := semaphore.NewWeighted(regSyncConcurrency)

		for ; enumerator.Next(); tablesTotal++ {
			_ = sem.Acquire(context.Background(), 1)

			go func(dbl *db.Loc) {
				defer sem.Release(1)
				if DeregisterTableFromState(dbl, r.Table, r.Input, r.Output, r.Version, r.ID) {
					atomic.AddUint64(&tablesSuccess, 1)
				} else {
					tableLocLog(&r.TableLoc).Errorf("Failed to deregister")
					mgr.metrics.SyncErrors.Inc(1)
				}
			}(enumerator.Value())
		}

		_ = sem.Acquire(context.Background(), regSyncConcurrency)
	}

	if tablesTotal != 0 && tablesSuccess == tablesTotal {
		if err = util.ExecSQL(mgr.conn, "UPDATE registrations SET sync_state=? WHERE id=?", syncedState, r.ID); err != nil {
			log.E(errors.Wrap(err, "Failed to update sync state in registrations"))
			mgr.metrics.SyncErrors.Inc(1)
		}
	}
}

// EmitRegisteredTablesCount emits a stat on the number of tables currently registered in state
func EmitRegisteredTablesCount() {
	cnt, err := GetCount(false)
	if err == nil {
		mgr.metrics.NumTablesRegistered.Set(int64(cnt))
	}
}

func getSyncIntervalSlack() time.Duration {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return time.Duration(r.Intn(int(regSyncSlack/time.Second))) * time.Second
}
