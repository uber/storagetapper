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

package types

//MyDBName is a database name this service will use to save state to
var MyDBName = "storagetapper"

//MySvcName is a name of this service
const MySvcName = "storagetapper"

//TestMySQLUser is MySQL user used to connect to MySQL in test
var TestMySQLUser = "storagetapper"

//TestMySQLPassword is MySQL password used to connect to MySQL in test
var TestMySQLPassword = "storagetapper"

//MySQLBoolean represents MySQL equivalent of boolean type
const MySQLBoolean = "tinyint(1)"

//MyClusterName is a cluster name this service will use to save state to
var MyClusterName = "storagetapper"
