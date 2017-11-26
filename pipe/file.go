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
	"bufio"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"golang.org/x/net/context" //"context"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
)

//TODO: Support reading file currently open by producer
//TODO: Support offset persistence

var delimiter byte = '\n'

//FilePipe file pipe structure
type FilePipe struct {
	datadir     string
	maxFileSize int64
}

type file struct {
	file   *os.File
	offset int64
	hash   hash.Hash
	writer *bufio.Writer
}

// fileProducer synchronously pushes messages to File using topic specified during producer creation
type fileProducer struct {
	header  Header
	datadir string
	topic   string
	gen     string
	files   map[string]*file
	seqno   int

	maxFileSize int64
}

// fileConsumer consumes messages from File using topic and partition specified during consumer creation
type fileConsumer struct {
	ctx     context.Context
	cancel  context.CancelFunc
	datadir string
	topic   string
	gen     string
	file    *os.File
	reader  *bufio.Reader
	header  *Header

	msg []byte
	err error
}

func init() {
	registerPlugin("file", initFilePipe)
}

func initFilePipe(pctx context.Context, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error) {
	return &FilePipe{cfg.DataDir, cfg.MaxFileSize}, nil
}

// Type returns Pipe type as File
func (p *FilePipe) Type() string {
	return "file"
}

//NewProducer registers a new sync producer
func (p *FilePipe) NewProducer(topic string) (Producer, error) {
	return &fileProducer{Header{}, p.datadir, topic, "", make(map[string]*file), 0, p.maxFileSize}, nil
}

//NewConsumer registers a new file consumer with context
func (p *FilePipe) NewConsumer(topic string) (Consumer, error) {
	c := &fileConsumer{nil, nil, p.datadir, topic, "", nil, nil, nil, nil, nil}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	fn, offset, err := c.seek(topic, InitialOffset)
	if log.E(err) {
		return nil, err
	}

	if fn == "" {
		return c, nil
	}

	log.Debugf("Open %v", fn)

	file, err := os.OpenFile(c.topicPath(topic)+fn, os.O_RDONLY, 0)
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
func (p *fileProducer) SetGen(gen int64) {
	p.gen = strconv.FormatInt(gen, 10)
}

//SetGen sets generation of the topic stream, separate directory with the string
//representation of "gen" will be consumed
func (p *fileConsumer) SetGen(gen int64) {
	p.gen = strconv.FormatInt(gen, 10)
}

func topicPath(datadir string, topic string, gen string) string {
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

func (p *fileProducer) topicPath(topic string) string {
	return topicPath(p.datadir, topic, p.gen)
}

func (p *fileConsumer) topicPath(topic string) string {
	return topicPath(p.datadir, topic, p.gen)
}

/*
func (p *fileConsumer) parseFileName(name string) (string, int64, error) {
	ehint := "Expected file name format 'unixtimestamp.seqno.partitionkey'"

	s := strings.Split(name, ".")
	if len(s) != 3 {
		return "", 0, fmt.Errorf("Incorrect file name format. %v", ehint)
	}

	offs, err := strconv.ParseInt(s[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("Error: %v, %v", err.Error(), ehint)
	}

	return s[2], offs, nil
}
*/

func (p *fileConsumer) nextFile(topic string, curFile string) (string, error) {
	files, err := ioutil.ReadDir(p.topicPath(topic))
	if err != nil {
		switch e := err.(type) {
		case *os.PathError:
			if e.Err == syscall.ENOENT {
				return "", nil
			}
		}

		return "", err
	}

	if len(files) == 0 {
		return "", nil
	}

	for _, v := range files {
		log.Debugf("cmp: %v %v", v.Name(), curFile)
		if p.topicPath(topic)+v.Name() > curFile {
			return v.Name(), nil
		}
	}

	return "", nil
}

func (p *fileConsumer) seek(topic string, offset int64) (string, int64, error) {
	files, err := ioutil.ReadDir(p.topicPath(topic))
	if err != nil {
		switch e := err.(type) {
		case *os.PathError:
			if e.Err == syscall.ENOENT {
				return "", 0, nil
			}
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

func (p *fileProducer) newFileName(key string) string {
	p.seqno++ //Precaution to not generate file with the same name if timestamps are equal
	return fmt.Sprintf("%s%010d.%03d.%s.open", p.topicPath(p.topic), time.Now().Unix(), p.seqno, key)
}

func (p *fileProducer) newFile(key string) error {
	if err := os.MkdirAll(p.topicPath(p.topic), 0770); err != nil {
		return err
	}

	f, err := os.OpenFile(p.newFileName(key), os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return err
	}

	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if offset == 0 {
		var hash []byte
		hash = make([]byte, sha256.Size)
		if err := writeHeader(&p.header, hash, f); err != nil {
			return err
		}
	}

	_ = p.closeFile(p.files[key])

	log.Debugf("Opened: %v, %v", key, f.Name())

	p.files[key] = &file{f, offset, sha256.New(), bufio.NewWriter(f)}

	return nil
}

func (p *fileProducer) getFile(key string) (*file, error) {
	f := p.files[key]
	if f == nil {
		if err := p.newFile(key); err != nil {
			return nil, err
		}
		f = p.files[key]
	}
	return f, nil
}

func (p *fileProducer) closeFile(f *file) error {
	var rerr error
	if f != nil {
		if err := f.writer.Flush(); log.E(err) {
			rerr = err
		}
		if _, err := f.file.Seek(0, io.SeekStart); log.E(err) {
			rerr = err
		}
		if err := writeHeader(&p.header, f.hash.Sum(nil), f.file); log.E(err) {
			rerr = err
		}
		if err := f.file.Close(); log.E(err) {
			rerr = err
		}
		if err := os.Rename(f.file.Name(), strings.TrimSuffix(f.file.Name(), ".open")); log.E(err) {
			rerr = err
		}
		log.Debugf("Closed: %v", f.file.Name())
	}
	return rerr
}

//Push produces message to File topic
func (p *fileProducer) push(key string, in interface{}, batch bool) error {
	var bytes []byte
	switch in.(type) {
	case []byte:
		bytes = in.([]byte)
	default:
		return fmt.Errorf("File pipe can handle binary arrays only")
	}

	f, err := p.getFile(key)
	if err != nil {
		return err
	}

	if _, err := f.writer.Write(bytes); err != nil {
		return err
	}
	_, _ = f.hash.Write(bytes)
	if err := f.writer.WriteByte(delimiter); err != nil {
		return err
	}
	_, _ = f.hash.Write([]byte{delimiter})

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

//PushK sends a keyed message to File
func (p *fileProducer) PushK(key string, in interface{}) error {
	return p.push(key, in, false)
}

//Push produces message to File topic
func (p *fileProducer) Push(in interface{}) error {
	return p.push("default", in, false)
}

//PushBatch stashes a keyed message into batch which will be send to File by
//PushBatchCommit
func (p *fileProducer) PushBatch(key string, in interface{}) error {
	return p.push(key, in, true)
}

//PushBatchCommit commits currently queued messages in the producer
func (p *fileProducer) PushBatchCommit() error {
	for _, v := range p.files {
		log.E(v.writer.Flush())
	}
	return nil
}

func (p *fileProducer) PushSchema(key string, data []byte) error {
	if err := p.PushBatchCommit(); err != nil {
		return err
	}
	if key == "" {
		key = "default"
	}
	_ = p.closeFile(p.files[key])
	p.files[key] = nil

	p.header.Schema = data

	return p.push(key, data, false)
}

// Close File Producer
func (p *fileProducer) Close() error {
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

func (p *fileConsumer) waitForNextFilePrepare() (*fsnotify.Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	err = watcher.Add(p.topicPath(p.topic))
	if err != nil {
		switch v := err.(type) {
		case syscall.Errno:
			if v == syscall.ENOENT {
				err = watcher.Add(p.datadir)
			}
		}
	}

	return watcher, err
}

func (p *fileConsumer) waitForNextFile(watcher *fsnotify.Watcher) (bool, error) {
	log.Debugf("Waiting for directory events %v", p.topic)

	for {
		select {
		case event := <-watcher.Events:
			log.Debugf("event: %v", event)
			if event.Op&fsnotify.Create == fsnotify.Create {
				log.Debugf("modified file: %v", event.Name)
				return false, nil
			}
		case err := <-watcher.Errors:
			return false, err
		case <-p.ctx.Done():
			return true, nil
		}
	}
}

func (p *fileConsumer) waitAndOpenNextFile() bool {
	for {
		curFn := ""
		if p.file != nil {
			curFn = p.file.Name()
		}

		log.Debugf("nextfile loop %v %v", curFn, p.topic)

		//Need to start watching before p.nextFile() to avoid race condition
		watcher, err := p.waitForNextFilePrepare()
		if log.E(err) {
			p.err = err
			return true
		}
		defer func() { log.E(watcher.Close()) }()

		nextFn, err := p.nextFile(p.topic, curFn)
		if log.E(err) {
			p.err = err
			return true
		}

		log.Debugf("NextFn: %v %v", nextFn, p.topic)

		if nextFn != "" && !strings.HasSuffix(nextFn, ".open") {
			file, err := os.OpenFile(p.topicPath(p.topic)+nextFn, os.O_RDONLY, 0)
			if log.E(err) {
				p.err = err
				return true
			}

			p.reader = bufio.NewReader(file)
			p.file = file

			h, err := readHeader(p.reader)
			if log.E(err) {
				p.err = err
				return true
			}
			p.header = h

			log.Debugf("Consumer opened: %v", file.Name())

			return true
		}

		ctxDone, err := p.waitForNextFile(watcher)
		log.Debugf("nextfile loop %v %v %v %v", curFn, ctxDone, err, p.topic)
		if log.E(err) {
			p.err = err
			return true
		}
		if ctxDone {
			log.Debugf("ctxDone")
			return false
		}
	}
}

//FetchNext fetches next message from File and commits offset read
func (p *fileConsumer) FetchNext() bool {
	for {
		//reader and file can be nil when directory is empty during
		//NewConsumer
		if p.reader != nil {
			p.msg, p.err = p.reader.ReadBytes(delimiter)
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

			p.err = nil
		}

		if !p.waitAndOpenNextFile() {
			return false //context canceled, no message
		}
		if p.err != nil {
			return true //has message with error set
		}
	}
}

//Pop pops pipe message
func (p *fileConsumer) Pop() (interface{}, error) {
	return p.msg, p.err
}

//Close closes consumer
func (p *fileConsumer) close(graceful bool) error {
	log.Debugf("Close consumer: %v", p.topic)
	p.cancel()
	if p.file != nil {
		err := p.file.Close()
		log.E(err)
	}
	return nil
}

//Close closes consumer
func (p *fileConsumer) Close() error {
	return p.close(true)
}

//Close closes consumer
func (p *fileConsumer) CloseOnFailure() error {
	return p.close(false)
}

func (p *fileConsumer) SaveOffset() error {
	return nil
}

func (p *fileProducer) SetFormat(format string) {
	p.header.Format = format
}
