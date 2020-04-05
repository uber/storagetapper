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

	"github.com/pkg/errors"

	"github.com/uber/storagetapper/log"
)

// ConnectionType is used as enum to represent connection types
type ConnectionType int

const (
	// Master is the type corresponding to a master or a leader
	Master ConnectionType = iota

	// Slave is the type corresponding to a slave or a follower
	Slave
)

func (c ConnectionType) String() string {
	return [...]string{"master", "slave"}[c]
}

type resolverConstructor func() Resolver

// resolvers is a registry of resolver initializers
var resolvers map[string]resolverConstructor

// Addr contains information required to connect to DB
type Addr struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port uint16 `json:"port"`
	User string `json:"user"`
	Pwd  string `json:"pwd"`
	DB   string `json:"db"`
}

// Loc contains information related to database location
type Loc struct {
	Cluster string
	Service string
	Name    string
}

/*TableLoc - table location */
type TableLoc struct {
	Service string
	Cluster string
	DB      string
	Table   string
	Input   string
	Output  string
	Version int
}

// LogFields provides a logger that logs messages with Loc fields
func (d *Loc) LogFields() log.Logger {
	return log.WithFields(log.Fields{
		"cluster": d.Cluster,
		"service": d.Service,
		"db":      d.Name,
	})
}

// Resolver defines a MySQL connection info resolver
type Resolver interface {
	GetInfo(context.Context, *Loc, ConnectionType) (*Addr, error)
	GetEnumerator(context.Context, string, string, string, string) (Enumerator, error)
	IsValidConn(ctx context.Context, dbl *Loc, connType ConnectionType, addr *Addr) bool
}

// NewResolver returns a MySQL connection info resolver depending on the input type
func NewResolver(inputType string) (Resolver, error) {
	// If a resolver for the specified input type exists we create it
	if constructor, ok := resolvers[inputType]; ok {
		return constructor(), nil
	}

	return nil, errors.Errorf("failed to create resolver due to unknown input type %v", inputType)
}

// Enumerator defines an iterator that allows iterating over db locations
type Enumerator interface {
	Value() *Loc
	Next() bool
	Reset()
}

// registerResolver is called by a specific resolver type to register itself
func registerResolver(inputType string, c resolverConstructor) {
	if resolvers == nil {
		resolvers = make(map[string]resolverConstructor)
	}

	resolvers[inputType] = c
}
