// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation hdfss (the "Software"), to deal
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
	"bufio"
	"database/sql"
	"fmt"
	"golang.org/x/net/context" //"context"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
)

//TODO: Support reading hdfs currently open by producer
//TODO: Support offset persistence

var hdfsDelimiter byte = '\n'

//hdfsPipe hdfs pipe structure
type hdfsPipe struct {
	hdfs        *hdfs.Client
	datadir     string
	maxFileSize int64
}

type hdfsFile struct {
	file   *hdfs.FileWriter
	name   string
	offset int64
	writer *bufio.Writer
}

// hdfsProducer synchronously pushes messages to Hdfs using topic specified during producer creation
type hdfsProducer struct {
	hdfs    *hdfs.Client
	datadir string
	topic   string
	gen     string
	files   map[string]*hdfsFile
	seqno   int

	maxFileSize int64
}

// hdfsConsumer consumes messages from Hdfs using topic and partition specified during consumer creation
type hdfsConsumer struct {
	hdfs    *hdfs.Client
	ctx     context.Context
	cancel  context.CancelFunc
	datadir string
	topic   string
	gen     string
	file    *hdfs.FileReader
	reader  *bufio.Reader

	msg []byte
	err error
}

func init() {
	registerPlugin("hdfs", initHdfsPipe)
}

func initHdfsPipe(pctx context.Context, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error) {
	client, err := hdfs.New(cfg.HadoopAddress)
	if log.E(err) {
		return nil, err
	}
	log.Debugf("Connected to HDFS cluster at: %v", cfg.HadoopAddress)
	return &hdfsPipe{client, cfg.DataDir, cfg.MaxFileSize}, nil
}

// Type returns Pipe type as Hdfs
func (p *hdfsPipe) Type() string {
	return "hdfs"
}

//NewProducer registers a new sync producer
func (p *hdfsPipe) NewProducer(topic string) (Producer, error) {
	return &hdfsProducer{p.hdfs, p.datadir, topic, "", make(map[string]*hdfsFile), 0, p.maxFileSize}, nil
}

//NewConsumer registers a new hdfs consumer with context
func (p *hdfsPipe) NewConsumer(topic string) (Consumer, error) {
	c := &hdfsConsumer{p.hdfs, nil, nil, p.datadir, topic, "", nil, nil, nil, nil}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	fn, offset, err := c.seek(topic, InitialOffset)
	if log.E(err) {
		return nil, err
	}

	if fn == "" {
		return c, nil
	}

	log.Debugf("Open %v", fn)

	file, err := p.hdfs.Open(c.topicPath(topic) + fn)
	if log.E(err) {
		return nil, err
	}

	_, err = file.Seek(offset, os.SEEK_SET)
	if log.E(err) {
		return nil, err
	}

	c.file = file
	c.reader = bufio.NewReader(file)

	return c, nil
}

//SetGen sets generation of the topic stream, separate directory with the string
//representation of "gen" will be created inside the topic directory
func (p *hdfsProducer) SetGen(gen int64) {
	p.gen = strconv.FormatInt(gen, 10)
}

//SetGen sets generation of the topic stream, separate directory with the string
//representation of "gen" will be consumed
func (p *hdfsConsumer) SetGen(gen int64) {
	p.gen = strconv.FormatInt(gen, 10)
}

func hdfsTopicPath(datadir string, topic string, gen string) string {
	var r string

	if datadir != "" {
		r += datadir + "/"
	}

	if topic != "" {
		r += topic + "/"
	}

	if gen != "" {
		r += gen + "/"
	}

	log.Debugf("TopicPath: %v", r)

	return r
}

func (p *hdfsProducer) topicPath(topic string) string {
	return hdfsTopicPath(p.datadir, topic, p.gen)
}

func (p *hdfsConsumer) topicPath(topic string) string {
	return hdfsTopicPath(p.datadir, topic, p.gen)
}

func (p *hdfsConsumer) nextFile(topic string, curFile string) (string, error) {
	files, err := p.hdfs.ReadDir(p.topicPath(topic))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}

		return "", err
	}

	if len(files) == 0 {
		return "", nil
	}

	for _, v := range files {
		log.Debugf("cmp: %v %v", p.topicPath(topic)+v.Name(), curFile)
		if v.Name() > curFile {
			return v.Name(), nil
		}
	}

	return "", nil
}

func (p *hdfsConsumer) seek(topic string, offset int64) (string, int64, error) {
	files, err := p.hdfs.ReadDir(p.topicPath(topic))
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, nil
		}

		return "", 0, err
	}

	if len(files) == 0 {
		return "", 0, nil
	}

	if offset == OffsetOldest {
		return files[0].Name(), 0, nil
	}

	if offset == OffsetNewest {
		return files[len(files)-1].Name(), files[len(files)-1].Size(), nil
	}

	return "", 0, fmt.Errorf("Arbitrary offsets not supported, only OffsetOldest and OffsetNewest offsets supported")
}

func (p *hdfsProducer) newFileName(key string) string {
	p.seqno++ //Precaution to not generate files with the same name if timestamps are equal
	return fmt.Sprintf("%s%010d.%03d.%s.open", p.topicPath(p.topic), time.Now().Unix(), p.seqno, key)
}

func (p *hdfsProducer) newFile(key string) error {
	if err := p.hdfs.MkdirAll(p.topicPath(p.topic), 0770); err != nil {
		return err
	}

	name := p.newFileName(key)
	f, err := p.hdfs.Append(name)
	if err != nil {
		f, err = p.hdfs.Create(name)
		if err != nil {
			return err
		}
	}

	_ = p.closeFile(p.files[key])

	log.Debugf("Opened: %v, %v", key, name)

	p.files[key] = &hdfsFile{f, name, 0, bufio.NewWriter(f)}

	return nil
}

func (p *hdfsProducer) getFile(key string) (*hdfsFile, error) {
	f := p.files[key]
	if f == nil {
		if err := p.newFile(key); err != nil {
			return nil, err
		}
		f = p.files[key]
	}
	return f, nil
}

func (p *hdfsProducer) closeFile(f *hdfsFile) error {
	var rerr error
	if f != nil {
		if err := f.writer.Flush(); log.E(err) {
			rerr = err
		}
		if err := f.file.Close(); log.E(err) {
			rerr = err
		}
		if err := p.hdfs.Rename(f.name, strings.TrimSuffix(f.name, ".open")); log.E(err) {
			rerr = err
		}
		log.Debugf("Closed: %v", f.name)
	}
	return rerr
}

//Push produces message to Hdfs topic
func (p *hdfsProducer) push(key string, in interface{}, batch bool) error {
	var bytes []byte
	switch in.(type) {
	case []byte:
		bytes = in.([]byte)
	default:
		return fmt.Errorf("HDFS pipe can handle binary arrays only")
	}

	f, err := p.getFile(key)
	if err != nil {
		return err
	}

	if _, err := f.writer.Write(bytes); err != nil {
		return err
	}
	if err := f.writer.WriteByte(hdfsDelimiter); err != nil {
		return err
	}

	log.Debugf("Push: %v, len=%v", key, len(bytes))

	if !batch {
		log.E(f.writer.Flush())
	}

	f.offset += int64(len(bytes)) + 1
	if f.offset >= p.maxFileSize {
		if batch {
			if err := f.writer.Flush(); err != nil {
				return err
			}
		}
		_ = p.closeFile(p.files[key])
		p.files[key] = nil
	}

	return nil
}

//PushK sends a keyed message to Hdfs
func (p *hdfsProducer) PushK(key string, in interface{}) error {
	return p.push(key, in, false)
}

//Push produces message to Hdfs topic
func (p *hdfsProducer) Push(in interface{}) error {
	return p.push("default", in, false)
}

//PushBatch stashes a keyed message into batch which will be send to Hdfs by
//PushBatchCommit
func (p *hdfsProducer) PushBatch(key string, in interface{}) error {
	return p.push(key, in, true)
}

//PushBatchCommit commits currently queued messages in the producer
func (p *hdfsProducer) PushBatchCommit() error {
	for _, v := range p.files {
		log.E(v.writer.Flush())
	}
	return nil
}

func (p *hdfsProducer) PushSchema(key string, data []byte) error {
	if err := p.PushBatchCommit(); err != nil {
		return err
	}
	if key == "" {
		key = "default"
	}
	_ = p.closeFile(p.files[key])
	p.files[key] = nil

	return p.PushK(key, data)
}

// Close HDFS Producer
func (p *hdfsProducer) Close() error {
	var err error
	var keys []string

	//Consumers expect to see files in order, so we need to close them in order here
	for k := range p.files {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for i := 0; i < len(keys); i++ {
		v := p.files[keys[i]]
		if e := p.closeFile(v); log.E(e) {
			err = e
		}
	}

	return err
}

//FetchNext fetches next message from Hdfs and commits offset read
func (p *hdfsConsumer) FetchNext() bool {
	for {
		//reader and file can be nil when directory is empty during
		//NewConsumer
		if p.reader != nil {
			p.msg, p.err = p.reader.ReadBytes(hdfsDelimiter)
			log.Debugf("consumer msg=%v err=%v", p.msg, p.err)
			if p.err == nil {
				p.msg = p.msg[:len(p.msg)-1]
				log.Debugf("Consumed message: %v", string(p.msg))
				return true
			}

			if p.err != io.EOF {
				log.E(p.err)
				return true
			}

			if len(p.msg) != 0 {
				p.err = fmt.Errorf("Corrupted file. Not ending with delimiter: %v %v", p.file.Name(), string(p.msg))
				return true
			}
		}

		for {
			curFn := ""
			if p.file != nil {
				curFn = p.file.Name()
			}

			log.Debugf("nexthdfs loop %v %v", curFn, p.topic)

			nextFn, err := p.nextFile(p.topic, curFn)
			if log.E(err) {
				p.err = err
				return true
			}

			log.Debugf("NextFn: %v %v", nextFn, p.topic)

			if nextFn != "" && !strings.HasSuffix(nextFn, ".open") {
				file, err := p.hdfs.Open(p.topicPath(p.topic) + nextFn)
				if log.E(err) {
					p.err = err
					return true
				}

				p.reader = bufio.NewReader(file)
				p.file = file

				log.Debugf("Consumer opened: %v", file.Name())

				break
			}

			//TODO: Implement proper watching for new files. Instead of polling.
			//For now use consumer in tests only
			time.Sleep(200 * time.Millisecond)
		}
	}
}

//Pop pops pipe message
func (p *hdfsConsumer) Pop() (interface{}, error) {
	return p.msg, p.err
}

//Close closes consumer
func (p *hdfsConsumer) close(graceful bool) error {
	log.Debugf("Close consumer: %v", p.topic)
	p.cancel()
	if p.file != nil {
		err := p.file.Close()
		log.E(err)
	}
	return nil
}

//Close closes consumer
func (p *hdfsConsumer) Close() error {
	return p.close(true)
}

//Close closes consumer
func (p *hdfsConsumer) CloseOnFailure() error {
	return p.close(false)
}

func (p *hdfsConsumer) SaveOffset() error {
	return nil
}
