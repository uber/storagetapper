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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	yaml "gopkg.in/yaml.v2"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/types"
)

/*This is just tests that nothing crashes*/
//TODO: Functional tests
func TestBasic(t *testing.T) {
	loaders = []configLoader{&std{loadFn: func(_ interface{}, file string) ([]byte, error) {
		if file == "/etc/"+types.MySvcName+"/base.yaml" {
			return yaml.Marshal(getDefaultConfig())
		}
		return nil, nil
	}}}

	def := getDefaultConfig()
	loaded := Get()

	var section AppConfigODS
	err := LoadSection(&section)
	checkFail(t, err)

	if def.Pipe.Kafka.Addresses == nil {
		def.Pipe.Kafka.Addresses = make([]string, 0)
	}
	if def.Pipe.Hadoop.Addresses == nil {
		def.Pipe.Hadoop.Addresses = make([]string, 0)
	}
	if def.OutputTopicNameTemplate == nil {
		def.OutputTopicNameTemplate = make(map[string]map[string]string)
	}
	if def.ChangelogTopicNameTemplate == nil {
		def.ChangelogTopicNameTemplate = make(map[string]map[string]string)
	}
	if def.Filters == nil {
		def.Filters = make(map[string]map[string]RowFilter)
	}

	loaded.LockExpireTimeout = 0
	section.LockExpireTimeout = 0

	spew.Dump(*def)
	spew.Dump(loaded.AppConfigODS)

	require.Equal(t, *def, loaded.AppConfigODS)
	require.Equal(t, *def, section)
}

func resetStdLoader(fn func(interface{}, string) ([]byte, error)) {
	cfg = atomic.Value{}
	if fn == nil {
		fn = stdReadFile
	}
	loaders = []configLoader{&std{loadFn: fn}}
}

func TestConfigEnvironment(t *testing.T) {
	resetStdLoader(nil)
	err := os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "production")
	checkFail(t, err)
	require.True(t, Environment() == EnvProduction)

	err = os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "development")
	checkFail(t, err)
	require.True(t, Environment() == EnvProduction)

	//stdConfig caches env, so need to create fresh instance
	resetStdLoader(nil)
	err = os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "development")
	checkFail(t, err)
	require.True(t, Environment() != EnvProduction)

	resetStdLoader(nil)
	err = os.Setenv(strings.ToUpper(types.MySvcName)+"_ENVIRONMENT", "something")
	checkFail(t, err)
	require.True(t, Environment() == EnvProduction)
}

func TestConfigNeg(t *testing.T) {
	//fail on first file fail (base.yaml)
	resetStdLoader(func(_ interface{}, file string) ([]byte, error) {
		return nil, fmt.Errorf("simulate error")
	})
	err := Load()
	if err == nil {
		t.Fatalf("should return error")
	}

	//Return error on second file read. (development.yaml)
	resetStdLoader(func(_ interface{}, file string) ([]byte, error) {
		if !strings.HasSuffix(file, "base.yaml") {
			return nil, fmt.Errorf("simulate error")
		}
		return []byte{}, nil
	})
	err = Load()
	if err == nil {
		t.Fatalf("should return error")
	}

	resetStdLoader(func(_ interface{}, file string) ([]byte, error) {
		return []byte("ill formatted yaml"), nil
	})
	err = Load()
	if err == nil {
		t.Fatalf("should return error")
	}

	resetStdLoader(func(_ interface{}, file string) ([]byte, error) {
		return nil, syscall.ENOENT
	})
	err = Load()
	if err != nil {
		t.Fatalf("file not exists, shouldn't produce error")
	}

	//Get() should exit on error
	called := false
	exit = func(code int) {
		called = true
	}

	resetStdLoader(func(_ interface{}, file string) ([]byte, error) {
		return nil, fmt.Errorf("some error")
	})
	_ = Get()
	if !called {
		t.Fatalf("error should exit the program")
	}
}

func TestTableParamsMerge(t *testing.T) {
	var tests = []struct {
		original     TableParams
		originalCopy TableParams
		merge        string
		result       TableParams
	}{
		//Existing compound structures should be replaced
		{TableParams{Pipe: PipeConfig{Compression: true, Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}, TableParams{Pipe: PipeConfig{Compression: true, Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}, `{"Pipe" : {"Kafka": {"Addresses" : [ "kaddr3", "kaddr4"]}, "Hadoop": {"Addresses" : [ "haddr3"]}}}`, TableParams{Pipe: PipeConfig{Compression: true, Kafka: KafkaConfig{Addresses: []string{"kaddr3", "kaddr4"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr3"}}}}},
		//Existing should stay untouched
		{TableParams{Pipe: PipeConfig{Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}, TableParams{Pipe: PipeConfig{Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}, `{"Pipe" : {"Compression":true}}`, TableParams{Pipe: PipeConfig{Compression: true, Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}},
		//Simple key should be set to false
		{TableParams{Pipe: PipeConfig{Compression: true, Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}, TableParams{Pipe: PipeConfig{Compression: true, Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}, `{"Pipe" : {"Compression":false}}`, TableParams{Pipe: PipeConfig{Kafka: KafkaConfig{Addresses: []string{"kaddr1"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr1", "haddr2"}}}}},
		//Empty compound should be replaced
		{TableParams{Pipe: PipeConfig{Compression: true}}, TableParams{Pipe: PipeConfig{Compression: true}}, `{"Pipe" : {"Kafka": {"Addresses" : [ "kaddr3", "kaddr4"]}, "Hadoop": {"Addresses" : [ "haddr3"]}}}`, TableParams{Pipe: PipeConfig{Compression: true, Kafka: KafkaConfig{Addresses: []string{"kaddr3", "kaddr4"}}, Hadoop: HadoopConfig{Addresses: []string{"haddr3"}}}}},
		//Empty stays empty
		{TableParams{Pipe: PipeConfig{Compression: true}}, TableParams{Pipe: PipeConfig{Compression: true}}, `{}`, TableParams{Pipe: PipeConfig{Compression: true}}},
	}

	for _, v := range tests {
		i := v.original.CopyForMerge()
		if err := json.Unmarshal([]byte(v.merge), i); err != nil {
			require.NoError(t, err)
		}
		i.MergeCompound(&v.original)
		require.Equal(t, i, &v.result)
		//Unmarshal shouldn't modify original
		require.Equal(t, &v.original, &v.originalCopy)
	}
}
