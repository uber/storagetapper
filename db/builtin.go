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

package db

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/types"
)

func init() {
	registerResolver(types.InputMySQL, NewBuiltinResolver)
}

//BuiltinResolveCluster to implement runtime polymorphism and resolve cyclic
//package dependency. state.SchemaGet will be assigned to in main.go and
//connectInfoGetFortest in db/open_test.go
var BuiltinResolveCluster func(*Loc, int) (*Addr, error)

// BuiltinResolver resolves MySQL connection info for SOA databases using Nemo
type BuiltinResolver struct {
}

// NewBuiltinResolver returns a new resolver for resolving connection info using Nemo
func NewBuiltinResolver() Resolver {
	return &BuiltinResolver{}
}

// GetInfo returns MySQL connection info
func (r *BuiltinResolver) GetInfo(ctx context.Context, dbl *Loc, connType int) (*Addr, error) {

	if BuiltinResolveCluster != nil {
		return BuiltinResolveCluster(dbl, connType)
	}

	return nil, fmt.Errorf("no builtin resolver set")
}

// IsValidConn checks the validity of the connection to make sure connection is to the correct MySQL DB
func (r *BuiltinResolver) IsValidConn(ctx context.Context, dbl *Loc, connType int, addr *Addr) bool {
	l := log.WithFields(log.Fields{"service": dbl.Service, "cluster": dbl.Cluster, "db": dbl.Name, "host": addr.Host, "port": addr.Port})

	conn, err := r.GetInfo(ctx, dbl, connType)
	if err != nil {
		l.Warnf(errors.Wrap(err, "Failed to fetch connection info to compare, assuming valid connection").Error())
		return true
	}

	if conn.Host != addr.Host || conn.Port != addr.Port {
		l.Warnf("Invalid connection due to mismatched host, port")
		return false
	}

	return true
}

// BuiltinEnumerator implements a db location enumerator for MySQL
type BuiltinEnumerator struct {
	current int
	data    []*Loc
}

// GetEnumerator returns an enumerator for db location info fetched from Nemo
func (r *BuiltinResolver) GetEnumerator(ctx context.Context, svc, cluster, sdb, table string) (Enumerator, error) {
	if svc == "" || sdb == "" || table == "" || cluster == "" {
		err := errors.New("service name, cluster name, DB name or table name not provided")
		return nil, err
	}

	loc := Loc{Cluster: cluster, Service: svc, Name: sdb}
	return &BuiltinEnumerator{
		current: -1, data: []*Loc{&loc},
	}, nil
}

// Next is used to check if anymore values can be returned by the enumerator
func (e *BuiltinEnumerator) Next() bool {
	e.current++

	return e.current < len(e.data)
}

// Value returns the current value from the enumerator
func (e *BuiltinEnumerator) Value() *Loc {
	return e.data[e.current]
}

// Reset resets the enumerator to the starting point
func (e *BuiltinEnumerator) Reset() {
	e.current = -1
}
