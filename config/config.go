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
	"runtime"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/uber/storagetapper/types"
)

// AppConfigODS is the config struct which the config gets loaded into
type AppConfigODS struct {
	LogType  string `yaml:"log_type"`
	LogLevel string `yaml:"log_level"`

	ServiceName string `yaml:"serviceName"`
	Port        int    `yaml:"port"`
	MaxNumProcs int    `yaml:"max_num_procs"`

	StateUpdateInterval time.Duration `yaml:"state_update_interval"`
	WorkerIdleInterval  time.Duration `yaml:"worker_idle_interval"`
	LockExpireTimeout   time.Duration `yaml:"lock_expire_timeout"`
	StateConnectURL     string        `yaml:"state_connect_url"`
	StateDBName         string        `yaml:"state_db_name"`
	StateClusterName    string        `yaml:"state_cluster_name"`

	ChangelogPipeType                 string                       `yaml:"changelog_pipe_type"`
	ChangelogTopicNameTemplateDefault string                       `yaml:"changelog_topic_name_template_default"`
	ChangelogTopicNameTemplate        map[string]map[string]string `yaml:"changelog_topic_name_template"`
	ChangelogWatchdogInterval         time.Duration

	OutputTopicNameTemplateDefault string                       `yaml:"output_topic_name_template_default"`
	OutputTopicNameTemplate        map[string]map[string]string `yaml:"output_topic_name_template"`

	Verbose bool

	ChangelogBuffer       bool `yaml:"changelog_buffer"`
	ForceMasterConnection bool `yaml:"force_master_connection"`

	InternalEncoding string `yaml:"internal_encoding"`

	TableParams `yaml:",inline"` //Merged with table specific config if any

	Filters map[string]map[string]RowFilter `yaml:"filters"`

	ConfigRefreshInterval time.Duration `yaml:"config_refresh_interval"`
}

// EncryptionConfig holds encryption configuration options
type EncryptionConfig struct {
	Enabled    bool
	PublicKey  string `yaml:"public_key"`  // used to encrypt in producer
	PrivateKey string `yaml:"private_key"` // used to decrypt in consumer
	SigningKey string `yaml:"signing_key"` // used to sign in producer and verify in consumer
}

// PipeConfig holds pipe configuration options
type PipeConfig struct {
	MaxBatchSize      int `yaml:"max_batch_size"`
	MaxBatchSizeBytes int `yaml:"max_batch_size_bytes"`

	BaseDir string `yaml:"base_dir"`

	MaxFileSize     int64 `yaml:"max_file_size"`      // file size on disk
	MaxFileDataSize int64 `yaml:"max_file_data_size"` //uncompressed data size

	Compression bool
	//Delimited enables producing delimited message to text files and length
	//prepended messages to binary files
	FileDelimited bool `yaml:"file_delimited"`

	NonBlocking bool `yaml:"non_blocking"`

	EndOfStreamMark bool

	Encryption EncryptionConfig

	S3     S3Config
	Hadoop HadoopConfig
	Kafka  KafkaConfig

	SQL SQLConfig `yaml:"sql"`
}

// KafkaConfig holds Kafka pipe configuration
type KafkaConfig struct {
	Addresses       []string
	MaxMessageBytes int `yaml:"max_message_bytes"`
}

// S3Config holds S3 output pipe configuration
type S3Config struct {
	Region   string
	Endpoint string
	Bucket   string
	BaseDir  string `yaml:"base_dir"`

	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	SessionToken    string `yaml:"session_token"`

	Timeout time.Duration
}

// HadoopConfig holds hadoop output pipe configuration
type HadoopConfig struct {
	User      string
	Addresses []string
	BaseDir   string `yaml:"base_dir"`
}

// SQLConfig holds SQL output pipe configuration
type SQLConfig struct {
	Type    string
	DSN     string `yaml:"dsn"`
	Service string
	Cluster string
	Db      string
}

// ScheduleConfig holds snapshot schedule parameters
type ScheduleConfig struct {
	Interval time.Duration //seconds. TODO: Implement proper duration unmarshalling
	//	Retention time.Duration
}

// TableParams holds per table configuration options
type TableParams struct {
	ClusterConcurrency int   `yaml:"cluster_concurrency"`
	ThrottleTargetMB   int64 `yaml:"throttle_target_mb"`
	ThrottleTargetIOPS int64 `yaml:"throttle_target_iops"`

	Pipe       PipeConfig
	Schedule   ScheduleConfig
	NoSnapshot bool `yaml:"no_snapshot"`

	RowFilter `yaml:"row_filter"`
}

// RowFilter has the condition, column name & values on which filter will be applied
type RowFilter struct {
	Column    string   `yaml:"column"`
	Values    []string `yaml:"values"`
	Condition string   `yaml:"condition"`
	Operator  string   `yaml:"operator"`
}

// AppConfig is the config struct which the config gets loaded into
type AppConfig struct {
	AppConfigODS
	PortDyn                                 int
	ChangelogTopicNameTemplateParsed        map[string]map[string]*template.Template
	OutputTopicNameTemplateParsed           map[string]map[string]*template.Template
	ChangelogTopicNameTemplateDefaultParsed *template.Template
	OutputTopicNameTemplateDefaultParsed    *template.Template
}

func log(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format, args...)
}

func getDefaultConfig() *AppConfigODS {
	return &AppConfigODS{
		Port: 7836,

		MaxNumProcs:         runtime.NumCPU(),
		StateDBName:         types.MyDbName,
		StateClusterName:    types.MyClusterName,
		StateUpdateInterval: 300 * time.Second,
		WorkerIdleInterval:  30 * time.Second,

		ChangelogPipeType:                 "kafka",
		ChangelogTopicNameTemplateDefault: types.MySvcName + ".service.{{.Service}}.db.{{.Db}}.table.{{.Table}}{{if .Version}}.v{{.Version}}{{end}}",

		OutputTopicNameTemplateDefault: "hp-tap-{{.Service}}-{{.Db}}-{{.Table}}{{if .Version}}-v{{.Version}}{{end}}",
		ChangelogBuffer:                true,
		ChangelogWatchdogInterval:      300 * time.Second,

		LogType:  "std",
		LogLevel: "info",

		InternalEncoding: "json",

		TableParams: TableParams{
			Pipe: PipeConfig{
				BaseDir:           fmt.Sprintf("/var/lib/%s", types.MySvcName),
				MaxFileSize:       1024 * 1024 * 1024,
				MaxBatchSize:      4096,
				MaxBatchSizeBytes: 32 * 1024 * 1024,
				FileDelimited:     true,

				S3: S3Config{
					Timeout: 7 * 24 * time.Hour,
				},
				Kafka: KafkaConfig{
					MaxMessageBytes: 30 * 1024 * 1024,
				},
			},
		},

		ConfigRefreshInterval: 300 * time.Second,
	}
}

//Define various environments
const (
	EnvProduction  = "production"
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvTest        = "test"
)

type configLoader interface {
	environment() string
	load(*AppConfig) error
	loadSection(interface{}) error
	zone() string
	save(*AppConfig) error
	parseConfig(cfg *AppConfig) error
}

//TODO: implement cmdline and env loaders
var loaders = []configLoader{&std{loadFn: stdReadFile}, &std{loadFn: mysqlRead, saveFn: mysqlWrite}}
var cfg atomic.Value
var updatedAt atomic.Value

//Get returns current config. Reloads it if refersh interval is expired
func Get() *AppConfig {
	c := cfg.Load()

	if c != nil {
		ac := c.(*AppConfig)
		updAt := updatedAt.Load()
		if updAt != nil && updAt.(time.Time).After(time.Now().Add(-ac.ConfigRefreshInterval)) {
			return ac
		}
	}

	err := Load()
	c = cfg.Load()

	if err != nil {
		log("error (re)loading config: %v\n", err.Error())
		if c == nil {
			exit(1)
			return nil // exit can be remapped in tests, so it doesn't stop program execution
		}
	} else {
		updatedAt.Store(time.Now())
	}

	return c.(*AppConfig)
}

//Set replaces current config with the contents of parameter
func Set(a *AppConfigODS) error {
	c := &AppConfig{AppConfigODS: *a}
	for _, l := range loaders {
		err := l.parseConfig(c)
		if err != nil {
			return err
		}
	}
	err := parseConfig(c)
	if err != nil {
		return err
	}
	cfg.Store(c)
	return nil
}

//Load creates the config
func Load() error {
	c := &AppConfig{AppConfigODS: *getDefaultConfig()}
	for _, l := range loaders {
		err := l.load(c)
		if err != nil {
			return err
		}
		if err = l.parseConfig(c); err != nil {
			return err
		}
	}
	if err := parseConfig(c); err != nil {
		return err
	}
	cfg.Store(c)
	return nil
}

//LoadSection can be used to load subsections of config files at runtime
func LoadSection(cfg interface{}) error {
	for _, l := range loaders {
		err := l.loadSection(cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

//Environment returns current environment
func Environment() string {
	return loaders[0].environment()
}

//Save saves config to backends
func Save() error {
	c := cfg.Load()
	if c == nil {
		return nil
	}
	for i := 0; i < len(loaders); i++ {
		if err := loaders[i].save(c.(*AppConfig)); err != nil {
			return err
		}
		updatedAt.Store(time.Now())
	}
	return nil
}

// Zone returns the current zone that the application is running in
func Zone() string {
	for _, l := range loaders {
		z := l.zone()
		if z != "" {
			return z
		}
	}
	return ""
}

func parseTemplates(tmplMap map[string]map[string]string) (map[string]map[string]*template.Template, error) {
	parsedMap := make(map[string]map[string]*template.Template)

	for i, inp := range tmplMap {
		for o, out := range inp {
			t, err := template.New(i + "." + o).Parse(out)
			if err != nil {
				return nil, err
			}
			if parsedMap[i] == nil {
				parsedMap[i] = make(map[string]*template.Template)
			}
			parsedMap[i][o] = t
		}
	}

	return parsedMap, nil
}

func parseConfig(c *AppConfig) error {
	t, err := parseTemplates(c.OutputTopicNameTemplate)
	if err != nil {
		return err
	}

	c.OutputTopicNameTemplateParsed = t

	t, err = parseTemplates(c.ChangelogTopicNameTemplate)
	if err != nil {
		return err
	}

	c.ChangelogTopicNameTemplateParsed = t

	td, err := template.New("otntd").Parse(c.OutputTopicNameTemplateDefault)
	if err != nil {
		return err
	}
	c.OutputTopicNameTemplateDefaultParsed = td

	td, err = template.New("ctntd").Parse(c.ChangelogTopicNameTemplateDefault)
	if err != nil {
		return err
	}
	c.ChangelogTopicNameTemplateDefaultParsed = td

	c.TableParams.Schedule.Interval *= time.Second

	//By default allow to pass 2 state update intervals to consider lock as stale
	if c.LockExpireTimeout == 0 {
		c.LockExpireTimeout = 2 * c.StateUpdateInterval
	}

	return nil
}

func sanitizeForLog(s string) string {
	if s != "" {
		return "<hidden>"
	}
	return "<empty>"
}

// String sanitizes config for log output
func (e EncryptionConfig) String() string {
	return fmt.Sprintf("{Enabled:%v, PublicKey:%v, PrivateKey:%v, SigningKey:%v}", e.Enabled, sanitizeForLog(e.PublicKey), sanitizeForLog(e.PrivateKey), sanitizeForLog(e.SigningKey))
}

//CopyForMerge clear all compound fields in preparation for merge by json.Unmarshal
func (t *TableParams) CopyForMerge() *TableParams {
	tp := *t
	tp.Pipe.Hadoop.Addresses = nil
	tp.Pipe.Kafka.Addresses = nil
	return &tp
}

//MergeCompound restores compound fields from original structure if they were
//empty in merged config
func (t *TableParams) MergeCompound(r *TableParams) {
	if len(t.Pipe.Hadoop.Addresses) == 0 && len(r.Pipe.Hadoop.Addresses) != 0 {
		t.Pipe.Hadoop.Addresses = r.Pipe.Hadoop.Addresses
	}
	if len(t.Pipe.Kafka.Addresses) == 0 && len(r.Pipe.Kafka.Addresses) != 0 {
		t.Pipe.Kafka.Addresses = r.Pipe.Kafka.Addresses
	}
}
