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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testsANSIQuotes = []struct {
	name  string
	src   string
	res   string
	quote byte
}{
	{"simple_single", "a", "a", '\''},
	{"simple_double", "a", "a", '"'},
	{"empty_single", "", "", '\''},
	{"empty_double", "", "", '"'},
	{"double_in_single", `"`, `"`, '\''},
	{"single_in_double", "'", "'", '"'},
	{"2double_in_single", `""`, `""`, '\''},
	{"2single_in_double", "''", "''", '"'},
	{"single_in_single", "'", "''", '\''},
	{"double_in_double", `"`, `""`, '"'},
	{"2single_in_single", "''", "''''", '\''},
	{"2double_in_double", `""`, `""""`, '"'},
	{"txt_single_in_single", "a'a'", "a''a''", '\''},
	{"txt_double_in_double", `a"a"`, `a""a""`, '"'},
	{"all_escapes_single", "\x00\b\n\r\x1a\"'\\", "\x00\b\n\r\x1a\"''\\", '\''},
	{"all_escapes_double", "\x00\b\n\r\x1a\"'\\", "\x00\b\n\r\x1a\"\"'\\", '"'},
}

var testsMySQLEscape = []struct {
	name string
	src  string
	res  string
}{
	{"simple", "a", "a"},
	{"all_escapes", "\x00\b\n\r\x1a\"'\\", `\0\b\n\r\Z\"\'\\`},
	{"all_escapes_doubled", "\x00\x00\b\b\n\n\r\r\x1a\x1a\"\"''\\\\", `\0\0\b\b\n\n\r\r\Z\Z\"\"\'\'\\\\`},
	{"txt_all_escapes", "a\x00a\ba\na\ra\x1aa\"a'a\\a", `a\0a\ba\na\ra\Za\"a\'a\\a`},
	{"txt_all_escapes_doubled", "a\x00\x00a\b\ba\n\na\r\ra\x1a\x1aa\"\"a''a\\\\a", `a\0\0a\b\ba\n\na\r\ra\Z\Za\"\"a\'\'a\\\\a`},
}

func TestANSIQuotes(t *testing.T) {
	for _, v := range testsANSIQuotes {
		t.Run(v.name, func(t *testing.T) {
			res := EscapeQuotes(v.src, v.quote)
			require.Equal(t, v.res, res)
		})
	}
}

func TestMySQLEscape(t *testing.T) {
	for _, v := range testsMySQLEscape {
		t.Run(v.name, func(t *testing.T) {
			res := MySQLEscape(true, v.src)
			require.Equal(t, v.res, res)
		})
	}
}

func TestMySQLEscapeNoBackslash(t *testing.T) {
	for i := 0; i < len(testsANSIQuotes); i += 2 {
		v := testsANSIQuotes[i]
		t.Run(v.name, func(t *testing.T) {
			res := MySQLEscape(false, v.src)
			require.Equal(t, v.res, res)
		})
	}
}
