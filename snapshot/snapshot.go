package snapshot

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/uber/storagetapper/encoder"
)

//Reader is our contract for snapshot reader
type Reader interface {
	//Start connects to the db and starts snapshot for the table
	Start(cluster string, svc string, dbs string, table string, enc encoder.Encoder) (lastGtid string, err error)
	//End deinitializes snapshot reader
	End()

	//FIXME: FromTx methods used in validation code only, look for a way to
	//remove it from this generic
	//StartFromTx starts snapshot from given tx
	StartFromTx(svc string, dbs string, table string, enc encoder.Encoder, tx *sql.Tx) (lastGtid string, err error)
	//EndFromTx deinitializes reader started by PrepareFromTx
	EndFromTx()

	//GetNext pops record fetched by HasNext
	//returns: key and encoded message
	GetNext() (string, []byte, error)

	//HasNext fetches the record from the source and encodes using encoder provided when reader created
	//This is a blocking method
	HasNext() bool
}

//ReaderConstructor initializes logger plugin
type ReaderConstructor func() (Reader, error)

//Plugins contains registered snaphot reader plugins
var Plugins map[string]ReaderConstructor

func registerPlugin(name string, init ReaderConstructor) {
	if Plugins == nil {
		Plugins = make(map[string]ReaderConstructor)
	}
	Plugins[name] = init
}

//InitReader constructs encoder without updating schema
func InitReader(input string) (Reader, error) {
	init := Plugins[strings.ToLower(input)]

	if init == nil {
		return nil, fmt.Errorf("Unsupported snapshot input type: %s", strings.ToLower(input))
	}

	rd, err := init()
	if err != nil {
		return nil, err
	}

	return rd, nil
}
