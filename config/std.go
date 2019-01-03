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

	"gopkg.in/yaml.v2"

	"github.com/uber/storagetapper/types"
)

const defaultEnvironment = EnvProduction
const defaultZone = ""

var paths = []string{"/etc/" + types.MySvcName, os.Getenv("HOME") + "/." + types.MySvcName, os.Getenv(strings.ToUpper(types.MySvcName) + "_CONFIG_DIR")}

var exit = os.Exit

type std struct {
	loadFn func(interface{}, string) ([]byte, error)
	saveFn func(interface{}, string, []byte) error
	env    string
}

func (c *std) getEnvironment() string {
	if len(c.env) != 0 {
		return c.env
	}
	c.env = strings.ToLower(os.Getenv(strings.ToUpper(types.MySvcName) + "_ENVIRONMENT"))
	if c.env != EnvProduction && c.env != EnvDevelopment && c.env != EnvStaging && c.env != EnvTest {
		c.env = defaultEnvironment
	}
	return c.env
}

//environment returns current environment
func (c *std) environment() string {
	return c.getEnvironment()
}

// Zone returns the current zone that the application is running in
func (c *std) zone() string {
	return defaultZone
}

func stdReadFile(_ interface{}, name string) ([]byte, error) {
	return ioutil.ReadFile(name)
}

func (c *std) loadFile(file string, cfg interface{}) error {
	b, err := c.loadFn(cfg, file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("error loading: %v: %v", file, err.Error())
	}
	log("config file: %+v\n", file)
	if err = yaml.Unmarshal(b, cfg); err != nil {
		return fmt.Errorf("error parsing: %v: %v", file, err.Error())
	}
	return nil
}

func (c *std) saveFile(file string, cfg interface{}) error {
	var b []byte
	var err error
	if c.saveFn == nil {
		return nil
	}
	if b, err = yaml.Marshal(cfg); err != nil {
		return fmt.Errorf("error serializing: %v: %v", file, err.Error())
	}
	err = c.saveFn(cfg, file, b)
	if err != nil {
		return fmt.Errorf("error saving: %v: %v", file, err.Error())
	}
	log("saved config file: %+v\n", file)
	return nil
}

func (c *std) loadPath(path string, cfg interface{}) error {
	if err := c.loadFile(path+"/base.yaml", cfg); err != nil {
		return err
	}
	return c.loadFile(path+"/"+c.env+".yaml", cfg)
}

func (c *std) loadSection(cfg interface{}) error {
	c.getEnvironment()
	for _, v := range paths {
		if v == "" {
			continue
		}
		if err := c.loadPath(v, cfg); err != nil {
			return err
		}
	}
	log("config: %+v\n", cfg)
	return nil
}

func (c *std) parseConfig(cfg *AppConfig) error {
	cfg.PortDyn = cfg.Port
	return nil
}

func (c *std) load(cfg *AppConfig) error {
	return c.loadSection(&cfg.AppConfigODS)
}

func (c *std) save(cfg *AppConfig) error {
	return c.saveFile(paths[len(paths)-1]+"/"+c.env+".yaml", &cfg.AppConfigODS)
}
