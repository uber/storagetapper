package changelog

import (
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/pool"
	"golang.org/x/net/context" //"context"
)

//Reader is a contract for changelog reader
type Reader interface {
	//Worker is a main log reader routine
	//returns false if no more log readers of this type are needed
	Worker() bool
}

//ReaderConstructor initializes logger plugin
type ReaderConstructor func(ctx context.Context, cfg *config.AppConfig, bufPipe pipe.Pipe, outPipes *map[string]pipe.Pipe, t pool.Thread) (Reader, error)

//Plugins contains registered binlog reader plugins
var Plugins map[string]ReaderConstructor

func registerPlugin(name string, init ReaderConstructor) {
	if Plugins == nil {
		Plugins = make(map[string]ReaderConstructor)
	}
	Plugins[name] = init
}

//Worker iterates over available workers and try start them
func Worker(ctx context.Context, cfg *config.AppConfig, bufPipe pipe.Pipe, outPipes *map[string]pipe.Pipe, tp pool.Thread) bool {
	for n, init := range Plugins {
		reader, err := init(ctx, cfg, bufPipe, outPipes, tp)
		if err != nil {
			log.Errorf("Init failed for: %v", n)
		}
		if reader.Worker() {
			return true
		}
	}
	return false
}
