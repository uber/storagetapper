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

	"github.com/efirs/hdfs/v2"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	//"context"
)

type hdfsClient struct {
	*hdfs.Client
}

type hdfsWriter struct {
	*hdfs.FileWriter
}

func (p *hdfsClient) OpenRead(name string, offset int64) (io.ReadCloser, error) {
	f, err := p.Client.Open(name)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (p *hdfsClient) openWriteLow(name string) (flushWriteCloser, io.Seeker, error) {
	f, err := p.Client.Append(name)
	if err != nil {
		f, err = p.Client.Create(name)
	}
	return &hdfsWriter{f}, nil, err
}

func (p *hdfsClient) OpenWrite(name string) (fc flushWriteCloser, sc io.Seeker, err error) {
	return fc, sc, withRetry(func() error { fc, sc, err = p.openWriteLow(name); return err })
}

var retryTimeout = 10 //seconds

func retriable(err error) bool {
	return strings.Contains(err.Error(), "org.apache.hadoop.ipc.StandbyException") ||
		strings.Contains(err.Error(), "org.apache.hadoop.ipc.RetriableException")
}

func withRetry(fn func() error) error {
	err := fn()
	for i := 0; err != nil && retriable(err) && i < retryTimeout*10; i++ {
		time.Sleep(100 * time.Millisecond)
		err = fn()
	}
	return err
}

func (p *hdfsClient) MkdirAll(path string, perm os.FileMode) error {
	return withRetry(func() error { return p.Client.MkdirAll(path, perm) })
}

func (p *hdfsClient) Rename(oldpath, newpath string) error {
	return withRetry(func() error { return p.Client.Rename(oldpath, newpath) })
}

func (p *hdfsClient) Remove(path string) error {
	return withRetry(func() error { return p.Client.Remove(path) })
}

func (p *hdfsClient) Cancel(f io.Closer) error {
	return nil
}

func (p *hdfsClient) ReadDir(dir string, _ string) ([]os.FileInfo, error) {
	return p.Client.ReadDir(dir)
}

func (p *hdfsWriter) Write(b []byte) (int, error) {
	var err error
	var n int
	off := 0
	for off < len(b) && err == nil {
		n, err = p.FileWriter.Write(b[off:])
		off += n
		for i := 0; err != nil && retriable(err) && i < retryTimeout*10 && off < len(b); i++ {
			time.Sleep(100 * time.Millisecond)
			n, err = p.FileWriter.Write(b[off:])
			off += n
		}
	}
	return off, err
}

func (p *hdfsWriter) Flush() error {
	return nil
	//	return withRetry(func() error { return p.FileWriter.Flush() })
}

func (p *hdfsWriter) Close() error {
	return withRetry(func() error { return p.FileWriter.Close() })
}

type hdfsPipe struct {
	filePipe
	hdfs *hdfs.Client
}

// hdfsConsumer consumes messages from Hdfs using topic and partition specified during consumer creation
type hdfsConsumer struct {
	fileConsumer
}

func init() {
	registerPlugin("hdfs", initHdfsPipe)
}

func initHdfsPipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	cp := hdfs.ClientOptions{User: cfg.Hadoop.User, Addresses: cfg.Hadoop.Addresses}
	client, err := hdfs.NewClient(cp)
	if log.E(err) {
		return nil, err
	}

	log.Infof("Connected to HDFS cluster at: %v", cfg.Hadoop.Addresses)

	return &hdfsPipe{filePipe{cfg.Hadoop.BaseDir, *cfg}, client}, nil
}

// Type returns Pipe type as Hdfs
func (p *hdfsPipe) Type() string {
	return "hdfs"
}

// Close releases resources associated with the pipe
func (p *hdfsPipe) Close() error {
	return p.hdfs.Close()
}

//NewProducer registers a new sync producer
func (p *hdfsPipe) NewProducer(topic string) (Producer, error) {
	m := metrics.NewFilePipeMetrics("pipe_producer", map[string]string{"topic": topic, "pipeType": "hdfs"})
	return &fileProducer{filePipe: &p.filePipe, topic: topic, files: make(map[string]*file), fs: &hdfsClient{p.hdfs}, metrics: m, stats: make(map[string]*stat)}, nil
}

//NewConsumer registers a new hdfs consumer with context
func (p *hdfsPipe) NewConsumer(topic string) (Consumer, error) {
	m := metrics.NewFilePipeMetrics("pipe_consumer", map[string]string{"topic": topic, "pipeType": "hdfs"})
	c := &hdfsConsumer{fileConsumer{filePipe: &p.filePipe, topic: topic, fs: &hdfsClient{p.hdfs}, metrics: m}}
	_, err := p.initConsumer(&c.fileConsumer, c.fetchNextPoll)
	return c, err
}
