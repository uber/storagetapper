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

/*Package shutdown provides a mechanism for graceful shutdown of multithreaded programs.
It combines functionality of WaitGroups and context.WithCancel, to account
number running threads and notify threads waiting in the blocking calls.

Setup/Wait allowed to be called sequentially only
Register can be called only from the thread where Setup/Wait called or from the
thread for which register has been called already.
*/
package shutdown

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/net/context" //"context"
)

var group sync.WaitGroup
var flag int32

/*Context will be canceled when Initiate is called*/
var Context context.Context
var cancel context.CancelFunc
var numThreads int32

/*Initiate notifies threads of program's intent to terminate*/
//Should be called by one thread only
func Initiate() {
	if atomic.LoadInt32(&flag) == 0 {
		atomic.StoreInt32(&flag, 1)
		cancel()
	}
}

/*Initiated is used by threads to check if program is being terminated*/
func Initiated() bool {
	return atomic.LoadInt32(&flag) != 0
}

/*InitiatedCh is used by threads to receive terminate notification from channel*/
func InitiatedCh() <-chan struct{} {
	return Context.Done()
}

//Register thread so that Wait function will wait for it termination
//Register should be called before starting threads routine
func Register(i int32) {
	atomic.AddInt32(&numThreads, i)
	group.Add(int(i))
}

/*Done should be called on threads routine exit to notify Wait that thread has
* finished*/
func Done() {
	atomic.AddInt32(&numThreads, -1)
	group.Done()
}

/*Setup initializes shutdown framework. Setup signal listener for SIGINT,
* SIGTERM */
func Setup() {
	atomic.StoreInt32(&flag, 0)
	atomic.StoreInt32(&numThreads, 0)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	Context, cancel = context.WithCancel(context.Background())

	Register(1)
	go func() {
		defer Done()
		select {
		case sig := <-sigs:
			if sig == syscall.SIGINT || sig == syscall.SIGTERM {
				Initiate()
			}
		case <-InitiatedCh():
		}
	}()
}

/*Wait waits for all the registered threads to terminate*/
func Wait() {
	group.Wait()
}

/*NumProcs returns number of currently running threads*/
func NumProcs() int32 {
	return atomic.LoadInt32(&numThreads)
}

/*InitiateAndWait is a helper which is often used in tests, where we want to
Initiate shutdown and Wait program to shutdown on function exit*/
func InitiateAndWait() {
	Initiate()
	Wait()
}
