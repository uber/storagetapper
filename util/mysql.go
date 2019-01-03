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

//https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
func escapeBackslash(s string) string {
	j := 0
	buf := make([]byte, len(s)*2)

	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\x00': //00
			buf[j] = '\\'
			buf[j+1] = '0'
			j += 2
		case '\b': //08
			buf[j] = '\\'
			buf[j+1] = 'b'
			j += 2
		case '\n': //10
			buf[j] = '\\'
			buf[j+1] = 'n'
			j += 2
		case '\r': //13
			buf[j] = '\\'
			buf[j+1] = 'r'
			j += 2
		case '\x1a': //26
			buf[j] = '\\'
			buf[j+1] = 'Z'
			j += 2
		case '"': //34
			buf[j] = '\\'
			buf[j+1] = '"'
			j += 2
		case '\'': //39
			buf[j] = '\\'
			buf[j+1] = '\''
			j += 2
		case '\\': //92
			buf[j] = '\\'
			buf[j+1] = '\\'
			j += 2
		default:
			buf[j] = s[i]
			j++
		}
	}

	return string(buf[:j])
}

// EscapeQuotes escapes given quote by doubling it
func EscapeQuotes(s string, quote byte) string {
	j := 0
	buf := make([]byte, len(s)*2)

	for i := 0; i < len(s); i++ {
		buf[j] = s[i]
		j++
		if s[i] == quote {
			buf[j] = quote
			j++
		}
	}

	return string(buf[:j])
}

// MySQLEscape returns escaped string
// escaping backslashes or duplicating single quotes, based on backslash
// parameter
func MySQLEscape(backslash bool, s string) string {
	if backslash {
		return escapeBackslash(s)
	}
	return EscapeQuotes(s, '\'')
}
