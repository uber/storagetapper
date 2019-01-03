package db

import (
	"database/sql"

	"github.com/pkg/errors"
	"github.com/uber/storagetapper/log"
)

// GetCurrentGTID returns current gtid set for the specified db address (host,port,user,password)
func GetCurrentGTID(addr *Addr) (string, error) {
	var d *sql.DB
	var gtid string
	var err error

	if d, err = Open(addr); err == nil {
		err = d.QueryRow("SELECT @@global.gtid_executed").Scan(&gtid)
		log.E(d.Close())
	}

	return gtid, err
}

// GetPurgedGTID returns purged gtid set for the specified db address (host,port,user,password)
func GetPurgedGTID(addr *Addr) (string, error) {
	var d *sql.DB
	var gtid string
	var err error

	if d, err = Open(addr); err == nil {
		err = d.QueryRow("SELECT @@global.gtid_purged").Scan(&gtid)
		log.E(d.Close())
	}

	return gtid, err
}

// GetCurrentGTIDForDB return current gtid set for the db specified by db locator (cluster,service,db)
func GetCurrentGTIDForDB(loc *Loc, inputType string) (string, error) {
	var err error
	var addr *Addr

	if addr, err = GetConnInfo(loc, Slave, inputType); err == nil {
		return GetCurrentGTID(addr)
	}

	return "", errors.Wrap(err, "error resolving db info")
}
