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

package server

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/state"

	"golang.org/x/net/context"
)

var server *http.Server
var mutex = sync.Mutex{}

func init() {
	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/schema", schemaCmd)
	http.HandleFunc("/cluster", clusterInfoCmd)
	http.HandleFunc("/table", tableCmd)
	http.HandleFunc("/config", configCmd)
	http.HandleFunc("/", indexCmd)
}

//StartHTTPServer starts listening and serving traffic on configured port and sets up http routes.
func StartHTTPServer(port int) {
	state.EmitRegisteredTablesCount()
	mutex.Lock()

	server = &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: nil}
	log.Debugf("HTTP server is listening on %v port", port)

	mutex.Unlock()

	err := server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.E(err)
	}
}

//healthCheck handles call to the health check endpoint
func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		log.Errorf("Health check failed: %s\n", err)
	}
}

//Shutdown gracefully stops the server
func Shutdown() {
	mutex.Lock()
	defer mutex.Unlock()
	if server == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.E(server.Shutdown(ctx))
	server = nil
}
