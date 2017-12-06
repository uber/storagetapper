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

package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"

	yaml "gopkg.in/yaml.v2"

	"github.com/uber/storagetapper/types"
)

/*This is just tests that nothing crashes*/
//TODO: Functional tests
func TestBasic(t *testing.T) {
	def = &stdConfig{}
	loadFile = func(file string) ([]byte, error) {
		if file == "/etc/"+types.MySvcName+"/base.yaml" {
			return yaml.Marshal(getDefaultConfig())
		}
		return nil, nil
	}

	d := getDefaultConfig()
	c := Get()

	var e AppConfigODS
	err := LoadSection(&e)
	checkFail(t, err)

	if d.KafkaAddrs == nil {
		d.KafkaAddrs = make([]string, 0)
	}
	if d.Hadoop.Addresses == nil {
		d.Hadoop.Addresses = make([]string, 0)
	}
	if d.OutputTopicNameTemplate == nil {
		d.OutputTopicNameTemplate = make(map[string]map[string]string)
	}
	if d.ChangelogTopicNameTemplate == nil {
		d.ChangelogTopicNameTemplate = make(map[string]map[string]string)
	}

	if !reflect.DeepEqual(*d, c.AppConfigODS) {
		t.Fatalf("loaded should be equal to default")
	}

	if !reflect.DeepEqual(*d, e) {
		t.Fatalf("loaded should be equal to default")
	}
}

func TestConfigEnvironment(t *testing.T) {
	def = &stdConfig{}
	err := os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "production")
	checkFail(t, err)
	if !EnvProduction() {
		t.Fatalf("env should be set to production")
	}

	err = os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "development")
	checkFail(t, err)
	if !EnvProduction() {
		t.Fatalf("env should be same as previously set")
	}

	//stdConfig caches env, so need to create fresh instance
	def = &stdConfig{}
	err = os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "development")
	checkFail(t, err)
	if EnvProduction() {
		t.Fatalf("env should be set to development")
	}

	def = &stdConfig{}
	err = os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "something")
	checkFail(t, err)
	if !EnvProduction() {
		t.Fatalf("by default should be set to production")
	}
}

func TestConfigNeg(t *testing.T) {
	//fail on first file fail (base.yaml)
	def = &stdConfig{}
	loadFile = func(file string) ([]byte, error) {
		return nil, fmt.Errorf("simulate error")
	}
	err := Load()
	if err == nil {
		t.Fatalf("should return error")
	}

	//Return error on second file read. (development.yaml)
	def = &stdConfig{}
	loadFile = func(file string) ([]byte, error) {
		if !strings.HasSuffix(file, "base.yaml") {
			return nil, fmt.Errorf("simulate error")
		}
		return []byte{}, nil
	}
	err = Load()
	if err == nil {
		t.Fatalf("should return error")
	}

	def = &stdConfig{}
	loadFile = func(file string) ([]byte, error) {
		return []byte("ill formatted yaml"), nil
	}
	err = Load()
	if err == nil {
		t.Fatalf("should return error")
	}

	def = &stdConfig{}
	loadFile = func(file string) ([]byte, error) {
		return nil, syscall.ENOENT
	}
	err = Load()
	if err != nil {
		t.Fatalf("file not exists, shouldn't produce error")
	}

	//Get() should exit on error
	called := false
	exit = func(code int) {
		called = true
	}

	def = &stdConfig{}
	loadFile = func(file string) ([]byte, error) {
		return nil, fmt.Errorf("some error")
	}
	_ = Get()
	if !called {
		t.Fatalf("error should exit the program")
	}
}
