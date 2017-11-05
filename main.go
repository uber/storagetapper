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

package main

import (
	"encoding/json"
	"golang.org/x/net/context" //"context"
	"time"

	"github.com/uber/storagetapper/changelog"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/pool"
	"github.com/uber/storagetapper/server"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/streamer"
	"github.com/uber/storagetapper/types"

	_ "net/http/pprof"

	_ "github.com/go-sql-driver/mysql"
)

var version, revision string = "1.0", ""

func idle(v int64, timeout int, tpool pool.Thread) {
	metrics.GetGlobal().IdleWorkers.Inc()
	defer metrics.GetGlobal().IdleWorkers.Dec()
	//	for v == state.GetVersion() && !shutdown.Initiated() && !tpool.Terminate() {
	//	log.Debugf("No work for me. Going to be idle for the next %v secs", timeout)
	select {
	case <-shutdown.InitiatedCh():
	case <-time.After(time.Second * time.Duration(timeout)):
	}
	//	log.Debugf("Will check for work")
	//	}
}

func worker(ctx context.Context, cfg *config.AppConfig, inP pipe.Pipe, outP *map[string]pipe.Pipe, tpool pool.Thread) {
	log.Debugf("Started worker thread, Total: %+v", shutdown.NumProcs()+1)
	for !shutdown.Initiated() && !tpool.Terminate() {
		v := state.GetVersion()

		if !changelog.Worker(ctx, cfg, inP, outP, tpool) {
			streamer.Worker(cfg, inP, outP)
		}

		idle(v, cfg.StateUpdateTimeout, tpool)
	}
	log.Debugf("Finished worker thread. Threads remaining %v", shutdown.NumProcs()+1)
}

func connectInfoGet(dbl *db.Loc, tp int) *db.Addr {
	ci := state.ConnectInfoGet(dbl, tp)
	return ci
}

func schemaGet(namespace string, schemaName string, typ string) (*types.AvroSchema, error) {
	var err error
	var a *types.AvroSchema

	s := state.GetOutputSchema(schemaName, typ)
	if s != "" {
		a = &types.AvroSchema{}
		err = json.Unmarshal([]byte(s), a)
	} else {
		a, err = encoder.GetSchemaWebster(namespace, schemaName, typ)
	}

	return a, err
}

/*mainLow extracted for the tests to be able to run with different
* configurations */
func mainLow(cfg *config.AppConfig) {
	log.Configure(cfg.LogType, cfg.LogLevel, config.EnvProduction())

	log.Infof("%s Version: %s, Revision: %s", types.MySvcName, version, revision)

	log.Debugf("Config: %+v", cfg)

	err := metrics.Init()
	log.F(err)

	shutdown.Setup()

	//closer, err := xjaeger.InitGlobalTracer(cfg.Jaeger, cfg.ServiceName, metrics)
	//log.F(err)
	//defer closer.Close()

	db.GetInfo = connectInfoGet
	encoder.GetLatestSchema = schemaGet
	server.BufferTopicNameFormat = cfg.BufferTopicNameFormat

	if !state.Init(cfg) {
		log.Fatalf("StateInit failed")
		return
	}

	/*TODO: Add ability to gracefully shutdown the HTTP server */
	go server.StartHTTPServer(cfg.PortDyn)

	nprocs := uint(cfg.MaxNumProcs)

	if cfg.ReaderPipeType == "local" {
		nprocs = 1 /*Start changelog reader only, it'll control the size of the thread pool*/
	}

	//Increasing batch size is important to prevent Pipes from preserving
	//offsets before batch of the size batch_size has been committed to the
	//output pipe
	//There may be totail 2*batch_size+1 events in flight
	// * batch_size - in outP batch buffer
	// * batch_size - in streamer helper channel buffer
	// * +1 - waiting in streamer helper to be pushed when buffer is full
	inP, err := pipe.Create(shutdown.Context, cfg.ReaderPipeType, 2*cfg.PipeBatchSize+1, cfg, state.GetDB())
	log.F(err)

	outP := make(map[string]pipe.Pipe)
	for p := range pipe.Pipes {
		outP[p], err = pipe.Create(shutdown.Context, p, cfg.PipeBatchSize, cfg, state.GetDB())
		log.F(err)
	}

	tp := pool.Create()

	tp.Start(nprocs, func() {
		worker(shutdown.Context, cfg, inP, &outP, tp)
	})

	shutdown.Wait()

	log.Debugf("FINISHED")
}

func main() {
	cfg := config.Get()
	mainLow(cfg)
}
