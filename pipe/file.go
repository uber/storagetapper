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
	"bytes"
	"compress/gzip"
	"crypto"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/packet" //"context"
	"golang.org/x/sys/unix"

	"github.com/fsnotify/fsnotify"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
)

//TODO: Support reading file currently open by producer
//TODO: Support offset persistence

const (
	dirPerm        = 0775
	delimiter byte = '\n'
)

var signKeyPw = ""
var privateKeyPw = ""

type flushWriteCloser interface {
	Write([]byte) (int, error)
	Flush() error
	Close() error
}

//fs calls abstraction to reuse most of the code in HDFS pipe
type fs interface {
	MkdirAll(path string, perm os.FileMode) error
	Rename(oldpath, newpath string) error
	ReadDir(dirname string, listFrom string) ([]os.FileInfo, error)
	OpenRead(name string, offset int64) (io.ReadCloser, error)
	OpenWrite(name string) (flushWriteCloser, io.Seeker, error)
	Remove(name string) error
	Cancel(io.Closer) error
}

type filePipe struct {
	datadir string
	cfg     config.PipeConfig
}

type file struct {
	name   string
	key    string
	file   io.WriteCloser //this is only used to cancel s3 streams, see fs.Cancel()
	seek   io.Seeker
	hash   hash.Hash
	offset int64
	nRecs  int64
	writer flushWriteCloser
	//Open order dlist
	prev *file
	next *file

	compressedSize int64
}

type stat struct {
	NumRecs  int64
	Hash     string
	FileName string
}

// fileProducer synchronously pushes messages to File using topic specified during producer creation
type fileProducer struct {
	*filePipe

	header Header
	topic  string
	files  map[string]*file
	//Linked list of files in open order, to be able to close in the same order
	ffirst *file
	flast  *file
	seqno  int

	fs   fs
	text int64 //Can be changed by SetFormat

	metrics *metrics.FilePipeMetrics

	stats map[string]*stat
}

// fileConsumer consumes messages from File using topic and partition specified during consumer creation
type fileConsumer struct {
	*filePipe
	baseConsumer
	topic  string
	file   io.ReadCloser
	name   string
	reader *bufio.Reader
	header Header
	fs     fs
	text   int64 //Determined by the Format field of the file header. See openFile

	msg []byte
	err error

	metrics *metrics.FilePipeMetrics
	pgpMD   *openpgp.MessageDetails

	watcher *fsnotify.Watcher

	offset int64
}

type noopFlusher struct {
	io.WriteCloser
}

func (f *noopFlusher) Flush() error {
	return nil
}

type flushClose struct {
	*bufio.Writer
}

func (f *flushClose) Close() error {
	return f.Writer.Flush()
}

//hashWriter currently is used only to report number of bytes really written
//to file and count compressed file size for rotation
type hashWriter struct {
	flushWriteCloser
	hash    hash.Hash
	metrics *metrics.FilePipeMetrics
	f       *file
}

func (w *hashWriter) Write(b []byte) (int, error) {
	n, err := w.flushWriteCloser.Write(b)
	if w.hash != nil {
		_, _ = w.hash.Write(b)
	}
	w.metrics.BytesWritten.Inc(int64(n))
	if w.f != nil {
		w.f.compressedSize += int64(n)
	}
	return n, err
}

type chainer struct {
	w1 flushWriteCloser
	w2 flushWriteCloser
}

func (c *chainer) Write(b []byte) (int, error) {
	return c.w1.Write(b) // writes to c.w2
}

func (c *chainer) Close() error {
	err1 := c.w1.Close()
	err2 := c.w2.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *chainer) Flush() error {
	if err := c.w1.Flush(); err != nil {
		return err
	}
	return c.w2.Flush()
}

func init() {
	registerPlugin("file", initFilePipe)
}

func initFilePipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	return &filePipe{cfg.BaseDir, *cfg}, nil
}

// Type returns Pipe type as File
func (p *filePipe) Type() string {
	return "file"
}

// Config returns pipe configuration
func (p *filePipe) Config() *config.PipeConfig {
	return &p.cfg
}

// Close releases resources associated with the pipe
func (p *filePipe) Close() error {
	return nil
}

//NewProducer registers a new sync producer
func (p *filePipe) NewProducer(topic string) (Producer, error) {
	m := metrics.NewFilePipeMetrics("pipe_producer", map[string]string{"topic": topic, "pipeType": "file"})
	return &fileProducer{filePipe: p, topic: topic, files: make(map[string]*file), fs: &fileFS{}, metrics: m, stats: make(map[string]*stat)}, nil
}

func (p *filePipe) initConsumer(c *fileConsumer, fn fetchFunc) (Consumer, error) {
	fname, offset, err := c.seek(c.topic, InitialOffset)
	if log.E(err) {
		return nil, err
	}

	if fname != "" {
		if strings.HasSuffix(fname, ".open") {
			c.offset = offset
		} else {
			c.openFile(fname, offset)
		}
	}

	c.initBaseConsumer(fn)

	return c, nil
}

//NewConsumer registers a new file consumer with context
func (p *filePipe) NewConsumer(topic string) (Consumer, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	m := metrics.NewFilePipeMetrics("pipe_consumer", map[string]string{"topic": topic, "pipeType": "file"})
	c := &fileConsumer{filePipe: p, topic: topic, fs: &fileFS{}, metrics: m, watcher: w}
	return p.initConsumer(c, c.fetchNext)
}

func topicPath(datadir string, topic string) string {
	var r string

	if datadir != "" {
		r += datadir + "/"
	}

	if topic != "" {
		r += topic
	}

	return r
}

func (p *fileProducer) topicPath(topic string) string {
	return topicPath(p.datadir, topic)
}

func (p *fileConsumer) topicPath(topic string) string {
	return topicPath(p.datadir, topic)
}

func (p *fileConsumer) nextFile(topic string, curFile string) (string, error) {
	tp := p.topicPath(topic)
	dir := filepath.Dir(tp)

	if curFile == "" {
		curFile = tp
	}

	files, err := p.fs.ReadDir(dir, curFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}

		return "", err
	}

	if len(files) == 0 {
		return "", nil
	}

	i := sort.Search(len(files), func(i int) bool {
		fn := dir + "/" + files[i].Name()
		return fn > curFile
	})

	if i < len(files) {
		fn := dir + "/" + files[i].Name()
		if strings.HasPrefix(fn, tp) && !files[i].IsDir() {
			log.Debugf("%v NextFile: %v,  CurFile: %v", topic, files[i].Name(), curFile)
			return files[i].Name(), nil
		}
	}

	return "", nil
}

func (p *fileConsumer) seek(topic string, offset int64) (string, int64, error) {
	tp := p.topicPath(topic)
	dir := filepath.Dir(tp)
	files, err := p.fs.ReadDir(dir, tp)
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
		for _, f := range files {
			if strings.HasPrefix(dir+"/"+f.Name(), tp) && !f.IsDir() {
				if strings.HasSuffix(f.Name(), ".open") {
					return "", 0, nil
				}
				return f.Name(), 0, nil
			}
		}
		return "", 0, nil
	}

	if offset == OffsetNewest {
		for i := len(files) - 1; i >= 0; i-- {
			if strings.HasPrefix(dir+"/"+files[i].Name(), tp) && !files[i].IsDir() {
				return files[i].Name(), files[i].Size(), nil
			}
		}
		return "", 0, nil
	}

	return "", 0, fmt.Errorf("arbitrary offsets not supported, only OffsetOldest and OffsetNewest offsets supported")
}

func (p *fileProducer) newFileName(key string) string {
	p.seqno++ //Precaution to not generate file with the same name if timestamps are equal
	format := "%s%010d.%03d.%s"
	if p.cfg.Compression {
		format += ".gz"
	}
	if p.cfg.Encryption.Enabled {
		format += ".gpg"
	}
	return fmt.Sprintf(format+".open", p.topicPath(p.topic), time.Now().Unix(), p.seqno, key)
}

func (p *fileProducer) initCrypterWriter(filename string, writer io.WriteCloser) (io.WriteCloser, error) {
	if len(p.cfg.Encryption.PublicKey) == 0 {
		return nil, fmt.Errorf("public key is empty. required for producing encrypted stream")
	}
	block, err := armor.Decode(bytes.NewReader([]byte(p.cfg.Encryption.PublicKey)))
	if log.E(err) {
		return nil, err
	}

	if block.Type != openpgp.PublicKeyType {
		return nil, fmt.Errorf("expected public key. got:%s", block.Type)
	}

	encEntity, err := openpgp.ReadEntity(packet.NewReader(block.Body))
	if log.E(err) {
		return nil, err
	}

	block, err = armor.Decode(bytes.NewReader([]byte(p.cfg.Encryption.SigningKey)))
	if log.E(err) {
		return nil, err
	}

	if block.Type != openpgp.PrivateKeyType {
		return nil, fmt.Errorf("expected private key. got:%s", block.Type)
	}

	signEntity, err := openpgp.ReadEntity(packet.NewReader(block.Body))
	if log.E(err) {
		return nil, err
	}

	err = signEntity.PrivateKey.Decrypt([]byte(signKeyPw))
	log.Debugf("signKeyPw %v", err)
	if log.E(err) {
		return nil, err
	}

	log.Debugf("pubKey:  %x", encEntity.PrimaryKey.Fingerprint)
	log.Debugf("signKey: %x", signEntity.PrimaryKey.Fingerprint)

	hints := &openpgp.FileHints{
		IsBinary: true,
		FileName: filename,
		ModTime:  time.Now(),
	}

	writer, err = openpgp.Encrypt(writer, []*openpgp.Entity{encEntity}, signEntity, hints, nil)
	if log.E(err) {
		return nil, err
	}

	return writer, nil
}

func listInsert(p *fileProducer, n *file) {
	if p.flast != nil {
		p.flast.next = n
	}
	if p.ffirst == nil {
		p.ffirst = n
	}
	p.flast = n
}

func listRemove(p *fileProducer, d *file) {
	if d.prev != nil {
		d.prev.next = d.next
	} else {
		p.ffirst = d.next
	}
	if d.next != nil {
		d.next.prev = d.prev
	} else {
		p.flast = d.prev
	}
}

func (p *fileProducer) newFile(key string) error {
	if err := p.fs.MkdirAll(filepath.Dir(p.topicPath(p.topic)), dirPerm); err != nil {
		return err
	}

	n := p.newFileName(key)
	w, seeker, err := p.fs.OpenWrite(n)
	if err != nil {
		return err
	}

	// continue writing works for uncompressed unencrypted files only
	var offset int64
	if seeker != nil {
		offset, err = seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
	}

	h := sha256.New()
	hw := &hashWriter{w, h, p.metrics, nil}
	var writer flushWriteCloser = hw

	if p.cfg.Encryption.Enabled {
		w, err := p.initCrypterWriter(n, writer)
		if err != nil {
			return err
		}
		writer = &chainer{&noopFlusher{w}, writer}
	}

	writer = &chainer{&flushClose{bufio.NewWriter(writer)}, writer}
	if p.cfg.Compression {
		writer = &chainer{gzip.NewWriter(writer), writer}
	}

	_ = p.closeFile(p.files[key], true)

	log.Debugf("Opened: %v, %v compression: %v", key, n, p.cfg.Compression)

	f := &file{n, key, w, seeker, h, offset, 0, writer, p.flast, nil, offset}
	hw.f = f

	listInsert(p, f)
	p.files[key] = f

	p.metrics.FilesOpened.Inc(1)

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

func (p *fileProducer) cancel(f *file) {
	err := p.fs.Remove(strings.TrimSuffix(f.name, ".open"))
	if err != nil && !os.IsNotExist(err) {
		log.E(err)
	}
	err = p.fs.Remove(f.name)
	if err != nil && !os.IsNotExist(err) {
		log.E(err)
	}
	log.E(p.fs.Cancel(f.file))
}

func syncFsMetadata() error {
	_, _, errno := unix.Syscall(unix.SYS_SYNC, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (p *fileProducer) closeFile(f *file, graceful bool) error {
	log.Debugf("Closed: %v", f)
	if f == nil {
		return nil
	}
	var rerr error
	defer func() {
		listRemove(p, f)
		delete(p.files, f.key)
		if rerr != nil || !graceful {
			p.cancel(f)
		}
	}()
	if err := f.writer.Close(); log.E(err) {
		rerr = err
	}
	fn := strings.TrimSuffix(f.name, ".open")
	if graceful && rerr == nil {
		if err := p.fs.Rename(f.name, fn); log.E(err) {
			rerr = err
		}
	}
	p.stats[fn] = &stat{NumRecs: f.nRecs, Hash: fmt.Sprintf("%x", f.hash.Sum(nil)), FileName: fn}
	p.metrics.FilesClosed.Inc(1)
	log.E(syncFsMetadata())
	log.Debugf("Closed: %v", f.name)
	return rerr
}

func (p *fileProducer) writeBinaryMsgLength(f *file, len int) error {
	if atomic.LoadInt64(&p.text) == 1 || !p.cfg.FileDelimited {
		return nil
	}

	sz := make([]byte, 4)
	binary.LittleEndian.PutUint32(sz, uint32(len))
	_, err := f.writer.Write(sz)

	return err
}

func (p *fileProducer) writeTextMsgDelimiter(f *file) error {
	if atomic.LoadInt64(&p.text) == 0 || !p.cfg.FileDelimited {
		return nil
	}

	o := make([]byte, 0)
	o = append(o, delimiter)

	_, err := f.writer.Write(o)
	return err
}

func (p *fileProducer) rotateOnSizeLimit(key string, f *file) {
	if (p.cfg.MaxFileDataSize != 0 && f.offset >= p.cfg.MaxFileDataSize) || (p.cfg.MaxFileSize != 0 && f.compressedSize > p.cfg.MaxFileSize) {
		_ = p.closeFile(p.files[key], true)
	}
}

//Push produces message to File topic
func (p *fileProducer) push(key string, in interface{}, batch bool) error {
	var bytes []byte
	switch b := in.(type) {
	case []byte:
		bytes = b
	default:
		return fmt.Errorf("file pipe can handle binary arrays only")
	}

	f, err := p.getFile(key)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			p.cancel(f)
		}
	}()

	//Prepend message with size in the case of binary delimited format
	if err = p.writeBinaryMsgLength(f, len(bytes)); err != nil {
		return err
	}

	if _, err = f.writer.Write(bytes); err != nil {
		return err
	}

	//In the case of text format apppend delimiter after the message
	if err = p.writeTextMsgDelimiter(f); err != nil {
		return err
	}

	f.offset += int64(len(bytes)) + 1
	f.nRecs++

	if !batch {
		if err = f.writer.Flush(); err != nil {
			return err
		}
		p.rotateOnSizeLimit(key, f)
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
	//Flush and may be close in open order
	f := p.ffirst
	for f != nil {
		if err := f.writer.Flush(); err != nil {
			p.cancel(f)
			return err
		}
		p.rotateOnSizeLimit(f.key, f)
		f = f.next
	}
	return nil
}

func (p *fileProducer) PushSchema(key string, data []byte) error {
	if err := p.PushBatchCommit(); err != nil {
		return err
	}
	if key == "" {
		key = "schema"
	}
	_ = p.closeFile(p.files[key], true)

	if len(data) == 0 {
		return nil
	}

	p.header.Schema = data

	return p.push(key, data, false)
}

func (p *fileProducer) close(graceful bool) (err error) {
	if len(p.files) == 0 {
		return nil
	}

	for p.ffirst != nil {
		if e := p.closeFile(p.ffirst, graceful); e != nil {
			err = e
		}
	}

	p.files = nil

	return err
}

func (p *fileProducer) dumpStat(f io.Writer) error {
	s := make([]*stat, 0)
	for _, n := range p.stats {
		s = append(s, n)
	}

	sort.Slice(s, func(i, j int) bool { return s[i].FileName < s[j].FileName })

	l, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if _, err := f.Write(l); err != nil {
		return err
	}

	return nil
}

// Close removes unfinished files
func (p *fileProducer) Close() error {
	err := p.close(true)
	if err != nil {
		return err
	}

	if p.cfg.EndOfStreamMark {
		if err := p.fs.MkdirAll(filepath.Dir(p.topicPath(p.topic)), dirPerm); err != nil {
			return err
		}
		name := filepath.Dir(p.topicPath(p.topic)) + "/_DONE"
		f, _, err := p.fs.OpenWrite(name)
		if err != nil {
			return err
		}
		if err := p.dumpStat(f); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// CloseOnFailure removes unfinished files
func (p *fileProducer) CloseOnFailure() error {
	return p.close(false)
}

//PartitionKey transforms input row key into partition key
func (p *fileProducer) PartitionKey(source string, key string) string {
	if source == "snapshot" {
		return "snapshot"
	}
	return "log"
}

func (p *fileConsumer) waitForNextFilePrepare() error {
	err := p.watcher.Add(filepath.Dir(p.topicPath(p.topic)))
	if err != nil {
		if os.IsNotExist(err) {
			err = p.watcher.Add(p.datadir)
		}
	}

	return err
}

func (p *fileConsumer) waitForNextFileFinish(watcher *fsnotify.Watcher) error {
	_ = watcher.Remove(filepath.Dir(p.topicPath(p.topic)))
	_ = watcher.Remove(p.datadir)
	return nil
}

func (p *fileConsumer) checkForNextFile(watcher *fsnotify.Watcher) bool {
	var e bool
	for {
		select {
		case event := <-watcher.Events:
			if event.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
				log.Debugf("modified file during readdir: %v %v", event.Name, event.Op)
				e = true
			}
		default:
			return e
		}
	}
}

func (p *fileConsumer) waitForNextFile(watcher *fsnotify.Watcher) (bool, error) {
	log.Debugf("Waiting for directory events %v", p.topic)

	for {
		select {
		case event := <-watcher.Events:
			if event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
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
	defer func() { log.E(p.waitForNextFileFinish(p.watcher)) }()
	for {
		//Need to start watching before p.nextFile() to avoid race condition
		p.err = p.waitForNextFilePrepare()
		if log.E(p.err) {
			return true
		}

		nextFn, err := p.nextFile(p.topic, p.name)
		if log.E(p.err) {
			return true
		}

		// There was create file events while we were reading the directory in
		// nextFile, so we need to start over to guarantee file ordering
		if p.checkForNextFile(p.watcher) {
			log.Debugf("directory was modified, restarting check for next file")
			log.E(p.waitForNextFileFinish(p.watcher))
			continue
		}

		if nextFn != "" && !strings.HasSuffix(nextFn, ".open") {
			p.openFile(nextFn, p.offset)
			p.offset = 0
			return true
		}

		if p.cfg.NonBlocking {
			return false
		}

		var ctxDone bool
		ctxDone, p.err = p.waitForNextFile(p.watcher)
		if log.E(err) {
			return true
		}

		if ctxDone {
			return false
		}

		log.E(p.waitForNextFileFinish(p.watcher))
	}
}

func (p *fileConsumer) waitAndOpenNextFilePoll() bool {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
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

		if p.cfg.NonBlocking {
			return false
		}

		select {
		case <-ticker.C:
		case <-p.ctx.Done():
			return false
		}
	}
}

func (p *fileConsumer) initCrypterReader(reader io.Reader) (io.Reader, *openpgp.MessageDetails, error) {
	if len(p.cfg.Encryption.PrivateKey) == 0 {
		return nil, nil, fmt.Errorf("private key is empty. required for consuming encrypted stream")
	}

	block, err := armor.Decode(bytes.NewReader([]byte(p.cfg.Encryption.PrivateKey)))
	if log.E(err) {
		return nil, nil, err
	}

	if block.Type != openpgp.PrivateKeyType {
		err = fmt.Errorf("expected private key. got:%s", block.Type)
		log.E(err)
		return nil, nil, err
	}

	privEntity, err := openpgp.ReadEntity(packet.NewReader(block.Body))
	if log.E(err) {
		return nil, nil, err
	}

	err = privEntity.PrivateKey.Decrypt([]byte(privateKeyPw))
	if log.E(err) {
		return nil, nil, err
	}

	md, err := openpgp.ReadMessage(reader, openpgp.EntityList{privEntity}, nil, &packet.Config{DefaultHash: crypto.SHA256})
	if log.E(err) {
		return nil, nil, err
	}
	return md.UnverifiedBody, md, nil
}

func (p *fileConsumer) openFileInitFilter() (err error) {
	//TODO: Get encryption and compression type from Filters field of the header
	if p.cfg.Encryption.Enabled || p.cfg.Compression {
		//Header reader cached more then just a header, so need to reopen
		log.E(p.file.Close())
		p.file, err = p.fs.OpenRead(p.name, 0)
		if log.E(err) {
			return
		}

		var reader io.Reader = p.file
		if p.cfg.Encryption.Enabled {
			reader, p.pgpMD, err = p.initCrypterReader(reader)
			if err != nil {
				return
			}
		}

		if p.cfg.Compression {
			reader, err = gzip.NewReader(reader)
			if log.E(err) {
				return
			}
		}

		p.reader = bufio.NewReader(reader)
	}

	return
}

func (p *fileConsumer) openFile(nextFn string, offset int64) {
	dir := filepath.Dir(p.topicPath(p.topic)) + "/"
	p.file, p.err = p.fs.OpenRead(dir+nextFn, 0)
	if log.E(p.err) {
		return
	}

	defer func() {
		if p.err != nil {
			p.reader = nil
			if p.file != nil {
				log.E(p.file.Close())
			}
			p.file = nil
		}
	}()

	p.reader = bufio.NewReader(p.file)

	p.header.Delimited = p.cfg.FileDelimited

	if !p.header.Delimited {
		p.err = fmt.Errorf("cannot consume non delimited file")
		log.E(p.err)
		return
	}

	if p.header.Format == "json" || p.header.Format == "text" {
		atomic.StoreInt64(&p.text, 1)
	}

	if offset != 0 {
		log.E(p.file.Close())
		p.file, p.err = p.fs.OpenRead(dir+nextFn, offset)
		if log.E(p.err) {
			return
		}
		p.reader = bufio.NewReader(p.file)
	}

	p.name = dir + nextFn

	p.err = p.openFileInitFilter()
	if p.err != nil {
		return
	}

	p.metrics.FilesOpened.Inc(1)
	log.Debugf("Consumer opened: %v, header: %+v", p.name, p.header)
}

func (p *fileConsumer) writeMessage() {
	if atomic.LoadInt64(&p.text) == 0 {
		msg := make([]byte, 4)
		_, p.err = p.reader.Read(msg)
		if p.err == nil {
			p.msg = make([]byte, binary.LittleEndian.Uint32(msg))
			_, p.err = io.ReadFull(p.reader, p.msg)
			/*
				if p.err == nil {
					log.Debugf("Consumed message: %x %p", p.msg, &p.baseConsumer)
				}
			*/
		}
	} else {
		p.msg, p.err = p.reader.ReadBytes(delimiter)
		if p.err == nil {
			p.msg = p.msg[:len(p.msg)-1]
			//log.Debugf("Consumed message: %x %p", p.msg, &p.baseConsumer)
		}
	}
}

func (p *fileConsumer) fetchNextLow() bool {
	//reader and file can be nil when directory is empty during
	//NewConsumer
	if p.reader != nil {
		p.writeMessage()
		if p.err == nil {
			return true
		}

		if p.err != io.EOF && (!p.cfg.Compression || p.err != io.ErrUnexpectedEOF) {
			log.E(p.err)
			return true
		}

		log.E(p.file.Close())
		p.reader = nil
		p.file = nil

		if p.pgpMD != nil && p.pgpMD.IsSigned && p.pgpMD.SignedBy != nil {
			if p.pgpMD.SignatureError != nil {
				log.Errorf("signature error:  %v", p.pgpMD.SignatureError)
			} else {
				if p.pgpMD.SignedBy.PublicKey != nil {
					log.Debugf("valid signature, signed by: %x", p.pgpMD.SignedBy.PublicKey.Fingerprint)
				}
			}
		}

		log.Debugf("Consumer closed: %v", p.name)

		if atomic.LoadInt64(&p.text) == 1 && p.cfg.FileDelimited && len(p.msg) != 0 {
			p.err = fmt.Errorf("corrupted file. Not ending with delimiter: %v %v", p.name, string(p.msg))
			return true
		}

		p.err = nil
	}
	return false
}

//fetchNext fetches next message from File and commits offset read
func (p *fileConsumer) fetchNext() (interface{}, error) {
	for {
		if p.fetchNextLow() {
			return p.msg, p.err
		}
		if !p.waitAndOpenNextFile() {
			return nil, p.err
		}
		if p.err != nil {
			return p.msg, p.err
		}
	}
}

//fetchNext fetches next message from File and commits offset read
func (p *fileConsumer) fetchNextPoll() (interface{}, error) {
	for {
		if p.fetchNextLow() {
			return p.msg, p.err
		}
		if !p.waitAndOpenNextFilePoll() {
			return nil, p.err
		}
		if p.err != nil {
			return p.msg, p.err
		}
	}
}

//Close closes consumer
func (p *fileConsumer) close(graceful bool) (err error) {
	log.Debugf("Close consumer: %v", p.topic)
	p.cancel()
	p.wg.Wait()
	if p.watcher != nil {
		err = p.watcher.Close()
		log.E(err)
	}
	if p.file != nil {
		err = p.file.Close()
		log.E(err)
	}
	return err
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

func (p *fileConsumer) SetFormat(format string) {
	p.header.Format = format
	if format == "json" || format == "text" {
		atomic.StoreInt64(&p.text, 1)
	}
}

func (p *fileProducer) SetFormat(format string) {
	p.header.Format = format
	if format == "json" || format == "text" {
		atomic.StoreInt64(&p.text, 1)
	}
}

type fileFS struct {
}

func (p *fileFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (p *fileFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (p *fileFS) ReadDir(dirname string, _ string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

func (p *fileFS) OpenRead(name string, offset int64) (io.ReadCloser, error) {
	f, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return f, nil
}

type fileWriter struct {
	*os.File
}

func (f *fileWriter) Flush() error {
	return f.File.Sync()
}

func (p *fileFS) OpenWrite(name string) (flushWriteCloser, io.Seeker, error) {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0644)
	return &fileWriter{f}, f, err
}

func (p *fileFS) Remove(path string) error {
	return os.Remove(path)
}

func (p *fileFS) Cancel(f io.Closer) error {
	return nil
}
