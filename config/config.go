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
	"runtime"

	"github.com/uber/storagetapper/types"
)

// AppConfig is the config struct which the config gets loaded into
type AppConfig struct {
	LogType  string `yaml:"log_type"`
	LogLevel string `yaml:"log_level"`

	Verbose bool

	ServiceName string `yaml:"serviceName"`
	Port        int    `yaml:"port"`
	PortDyn     int

	MaxNumProcs        int          `yaml:"max_num_procs"`
	StateUpdateTimeout int          `yaml:"state_update_timeout"`
	StateConnectURL    string       `yaml:"state_connect_url"`
	KafkaAddrs         []string     `yaml:"kafka_addresses"`
	Hadoop             HadoopConfig `yaml:"hadoop"`

	ChangelogPipeType        string `yaml:"changelog_pipe_type"`
	ChangelogOutputFormat    string `yaml:"changelog_output_format"`
	ChangelogTopicNameFormat string `yaml:"changelog_topic_name_format"`
	ChangelogBuffer          bool   `yaml:"changelog_buffer"`

	OutputPipeType        string `yaml:"output_pipe_type"`
	OutputFormat          string `yaml:"output_format"`
	ConcurrentBootstrap   bool   `yaml:"concurrent_bootstrap"`
	OutputTopicNameFormat string `yaml:"output_topic_name_format"`
	ClusterConcurrency    int    `yaml:"cluster_concurrency"`

	PipeBatchSize int `yaml:"pipe_batch_size"`

	OutputPipeConcurrency int  `yaml:"output_pipe_concurrency"`
	ForceMasterConnection bool `yaml:"force_master_connection"`

	ThrottleTargetMB   int64 `yaml:"throttle_target_mb"`
	ThrottleTargetIOPS int64 `yaml:"throttle_target_iops"`

	DefaultInputType string `yaml:"default_input_type"`

	DataDir     string `yaml:"data_dir"`
	MaxFileSize int64  `yaml:"max_file_size"`

	InternalEncoding string `yaml:"internal_encoding"`
}

// HadoopConfig holds hadoop output pipe configuration
type HadoopConfig struct {
	User      string   `yaml:"user"`
	Addresses []string `yaml:"addresses"`
	BaseDir   string   `yaml:"base_dir"`
}

// GetDefaultConfig returns default configuration
func GetDefaultConfig() *AppConfig {
	return &AppConfig{
		Port:    7836,
		PortDyn: 7836,

		MaxNumProcs:        runtime.NumCPU(),
		StateUpdateTimeout: 300,

		ChangelogPipeType:        "kafka",
		ChangelogOutputFormat:    "json",
		ChangelogTopicNameFormat: types.MySvcName + ".service.%s.db.%s.table.%s",
		ChangelogBuffer:          true,

		OutputPipeType:        "kafka",
		OutputFormat:          "avro",
		OutputTopicNameFormat: "hp-tap-%s-%s-%s",

		PipeBatchSize:         256,
		OutputPipeConcurrency: 1,
		ClusterConcurrency:    0,
		ForceMasterConnection: false,

		LogType:  "std",
		LogLevel: "info",

		ThrottleTargetMB:   0,
		ThrottleTargetIOPS: 0,

		DefaultInputType: "mysql",

		DataDir:     fmt.Sprintf("/var/lib/%s", types.MySvcName),
		MaxFileSize: 1024 * 1024 * 1024,

		InternalEncoding: "json",
	}
}

type configLoader interface {
	Get() *AppConfig
	EnvProduction() bool
	Load() error
	LoadSection(interface{}) error
}

var def configLoader = &stdConfig{}

//Get returns
func Get() *AppConfig {
	return def.Get()
}

//EnvProduction returns true in production environment
func EnvProduction() bool {
	return def.EnvProduction()
}

//Load creates the config
func Load() error {
	return def.Load()
}

//LoadSection can be used to load subsections of config files at runtime
func LoadSection(cfg interface{}) error {
	return def.LoadSection(cfg)
}
