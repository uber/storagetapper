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
	"io/ioutil"
	"os"
	"strings"

	yaml "gopkg.in/yaml.v2"

	"github.com/uber/storagetapper/types"
)

const defaultEnvironment = "production"

var paths = []string{"/etc/" + types.MySvcName, os.Getenv("HOME") + "/." + types.MySvcName, os.Getenv(strings.ToUpper(types.MySvcName) + "_CONFIG_DIR")}

type stdConfig struct {
	environment string
	cfg         *AppConfig
}

func (c *stdConfig) getEnvironment() string {
	if len(c.environment) != 0 {
		return c.environment
	}
	c.environment = strings.ToLower(os.Getenv(strings.ToUpper(types.MySvcName) + "_ENVIRONMENT"))
	if c.environment != "production" && c.environment != "development" {
		c.environment = defaultEnvironment
	}
	return c.environment
}

//EnvProduction returns true in production environment
func (c *stdConfig) EnvProduction() bool {
	c.getEnvironment()
	return c.environment == "production"
}

func (c *stdConfig) loadFile(file string, cfg interface{}) error {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("Error loading: %v: %v", file, err.Error())
	}
	fmt.Fprintf(os.Stderr, "config file: %+v\n", file)
	if err = yaml.Unmarshal(b, cfg); err != nil {
		return fmt.Errorf("Error parsing: %v: %v", file, err.Error())
	}
	return nil
}

func (c *stdConfig) loadPath(path string, cfg interface{}) error {
	if err := c.loadFile(path+"/base.yaml", cfg); err != nil {
		return err
	}
	if err := c.loadFile(path+"/"+c.environment+".yaml", cfg); err != nil {
		return err
	}
	return nil
}

//LoadSection returns
func (c *stdConfig) LoadSection(cfg interface{}) error {
	c.getEnvironment()

	for _, v := range paths {
		if err := c.loadPath(v, cfg); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "config: %+v\n", cfg)

	return nil
}

//Load config from files
func (c *stdConfig) Load() error {
	cfg := GetDefaultConfig()

	if err := c.LoadSection(cfg); err != nil {
		return err
	}

	cfg.PortDyn = cfg.Port
	c.cfg = cfg

	return nil
}

//Get returns currently loaded config
func (c *stdConfig) Get() *AppConfig {
	if c.cfg == nil {
		err := Load()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err.Error())
			os.Exit(1)
		}
	}
	return c.cfg
}
