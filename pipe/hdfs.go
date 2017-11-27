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

package pipe

import (
	"database/sql"
	"io"
	"os"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"golang.org/x/net/context" //"context"
)

type hdfsClient struct {
	*hdfs.Client
}

func (p *hdfsClient) OpenRead(name string, offset int64) (io.ReadCloser, error) {
	f, err := p.Client.Open(name)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (p *hdfsClient) OpenWrite(name string) (io.WriteCloser, io.Seeker, error) {
	f, err := p.Client.Append(name)
	if err != nil {
		f, err = p.Client.Create(name)
	}
	return f, nil, err
}

type hdfsPipe struct {
	filePipe
	hdfs *hdfs.Client
}

// hdfsProducer synchronously pushes messages to Hdfs using topic specified during producer creation
type hdfsProducer struct {
	fileProducer
}

// hdfsConsumer consumes messages from Hdfs using topic and partition specified during consumer creation
type hdfsConsumer struct {
	fileConsumer
}

func init() {
	registerPlugin("hdfs", initHdfsPipe)
}

func initHdfsPipe(pctx context.Context, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error) {
	cp := hdfs.ClientOptions{User: cfg.Hadoop.User, Addresses: cfg.Hadoop.Addresses}
	client, err := hdfs.NewClient(cp)
	if log.E(err) {
		return nil, err
	}

	log.Infof("Connected to HDFS cluster at: %v", cfg.Hadoop.Addresses)

	return &hdfsPipe{filePipe{cfg.Hadoop.BaseDir, cfg.MaxFileSize}, client}, nil
}

// Type returns Pipe type as Hdfs
func (p *hdfsPipe) Type() string {
	return "hdfs"
}

//NewProducer registers a new sync producer
func (p *hdfsPipe) NewProducer(topic string) (Producer, error) {
	return &hdfsProducer{fileProducer{Header{}, p.datadir, topic, "", make(map[string]*file), 0, p.maxFileSize, &hdfsClient{p.hdfs}, true}}, nil
}

//NewConsumer registers a new hdfs consumer with context
func (p *hdfsPipe) NewConsumer(topic string) (Consumer, error) {
	c := &hdfsConsumer{fileConsumer{nil, nil, p.datadir, topic, "", nil, "", nil, nil, &hdfsClient{p.hdfs}, true, nil, nil}}
	_, err := p.initConsumer(&c.fileConsumer)
	return c, err
}

func (p *hdfsConsumer) waitAndOpenNextFile() bool {
	for {
		nextFn, err := p.nextFile(p.topic, p.name)
		if log.E(err) {
			p.err = err
			return true
		}

		if nextFn != "" && !strings.HasSuffix(nextFn, ".open") {
			p.openFile(nextFn, 0)
			return true
		}

		//TODO: Implement proper watching for new files. Instead of polling.
		//For now use consumer in tests only
		time.Sleep(200 * time.Millisecond)
	}
}

//FetchNext fetches next message from Hdfs and commits offset read
func (p *hdfsConsumer) FetchNext() bool {
	for {
		if p.fetchNextLow() {
			return true
		}
		if !p.waitAndOpenNextFile() {
			return false //context canceled, no message
		}
		if p.err != nil {
			return true //has message with error set
		}
	}
}
