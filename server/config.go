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
	"encoding/json"
	"mime"
	"net/http"

	yaml "gopkg.in/yaml.v2"

	"errors"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
)

type configReq struct {
	Cmd  string
	Body string
}

func configCmd(w http.ResponseWriter, r *http.Request) {
	var err error
	s := configReq{}
	g := config.AppConfigODS{}
	ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if r.FormValue("cmd") == "get" {
		var b []byte
		b, err = yaml.Marshal(config.Get().AppConfigODS)
		if err == nil {
			w.Header().Set("Content-Type", "application/x-yaml")
			_, _ = w.Write(b)
		}
	} else { // set
		switch {
		case ct == "application/x-www-form-urlencoded", ct == "multipart/form-data", ct == "":
			if len(r.FormValue("body")) == 0 {
				err = errors.New("config body is empty")
			} else {
				err = yaml.Unmarshal([]byte(r.FormValue("body")), &g)
			}
		case ct == "application/x-yaml", ct == "text/vnd.yaml":
			if r.Body == nil {
				err = errors.New("config body is empty")
			} else {
				err = yaml.NewDecoder(r.Body).Decode(&g)
			}
		case ct == "application/json":
			if r.Body == nil {
				err = errors.New("config body is empty")
			} else {
				err = json.NewDecoder(r.Body).Decode(&g)
			}
		default:
			code := http.StatusUnsupportedMediaType
			http.Error(w, http.StatusText(code), code)
			return
		}
		if err == nil {
			if err = config.Set(&g); err == nil {
				err = config.Save()
			}
		}
	}
	if err != nil {
		log.Errorf("Config http: cmd=%v, body='%v', error=%v", s.Cmd, g, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
