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

	"golang.org/x/net/context"
)

var version, revision = "1.0", ""

func idle(interval time.Duration) {
	metrics.IdleWorkers.Inc()
	defer metrics.IdleWorkers.Dec()

	log.Debugf("going idle")

	select {
	case <-shutdown.InitiatedCh():
	case <-time.After(interval):
	}
}

func worker(ctx context.Context, cfg *config.AppConfig, inP pipe.Pipe, tpool pool.Thread) {
	log.Debugf("Started worker thread, Total: %+v", shutdown.NumProcs()+1)
	for !shutdown.Initiated() && !tpool.Terminate() {
		if !changelog.Worker(ctx, cfg, inP, tpool) && !streamer.Worker(cfg, inP) {
			idle(cfg.WorkerIdleInterval)
		}
	}

	log.Debugf("Finished worker thread. Threads remaining %v", shutdown.NumProcs()+1)
}

// mainLow extracted for the tests to be able to run with different configurations
func mainLow(cfg *config.AppConfig) {
	env := config.Environment()

	log.Configure(cfg.LogType, cfg.LogLevel, env == config.EnvProduction || env == config.EnvStaging)

	log.Infof("%s Version: %s, Revision: %s", types.MySvcName, version, revision)

	log.Debugf("Config: %+v", cfg)

	types.MyDbName = cfg.StateDBName
	types.MyClusterName = cfg.StateClusterName

	err := metrics.Init()
	log.F(err)

	shutdown.Setup()

	//This is to resolve package cyclic dependencies
	encoder.GetLatestSchema = state.SchemaGet
	db.BuiltinResolveCluster = state.ConnectInfoGet

	if err := state.InitManager(shutdown.Context, cfg); err != nil {
		log.F(err)
	}

	go server.StartHTTPServer(cfg.PortDyn)

	nprocs := uint(cfg.MaxNumProcs)

	if cfg.ChangelogPipeType == "local" {
		nprocs = 1 /*Start changelog reader only, it'll control the size of the thread pool*/
	}

	//Increasing batch size is important to prevent Pipes from preserving
	//offsets before batch of the size batch_size has been committed to the
	//output pipe
	//There may be total 2*batch_size+1 events in flight
	// * batch_size - in outP batch buffer
	// * batch_size - in streamer helper channel buffer
	// * +1 - waiting in streamer helper to be pushed when buffer is full
	p := cfg.Pipe
	p.MaxBatchSize = 2*p.MaxBatchSize + 1
	inP, err := pipe.Create(cfg.ChangelogPipeType, &p, state.GetDB())
	log.F(err)

	tp := pool.Create()

	tp.Start(nprocs, func() {
		worker(shutdown.Context, cfg, inP, tp)
	})

	shutdown.Wait()

	server.Shutdown()

	pipe.CacheDestroy()

	log.Debugf("FINISHED")
}

func main() {
	cfg := config.Get()
	mainLow(cfg)
}
