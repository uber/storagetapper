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
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"golang.org/x/net/context" //"context"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"sort"
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

//Delimited enables producing delimited message to text files and length
//prepended messages to binary files
var Delimited = false

//fs calls abstraction to reuse most of the code in HDFS pipe
type fs interface {
	MkdirAll(path string, perm os.FileMode) error
	Rename(oldpath, newpath string) error
	ReadDir(dirname string) ([]os.FileInfo, error)
	OpenRead(name string, offset int64) (io.ReadCloser, error)
	OpenWrite(name string) (io.WriteCloser, io.Seeker, error)
}

type filePipe struct {
	datadir     string
	maxFileSize int64
	AESKey      string
	HMACKey     string
	verifyHMAC  bool
	compression bool
}

type writerFlusher interface {
	Write([]byte) (int, error)
	Flush() error
}

type file struct {
	name    string
	file    io.WriteCloser
	seek    io.Seeker
	offset  int64
	crypter cipher.Stream
	hash    *hashWriter
	writer  writerFlusher
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
	fs          fs
	text        bool //Can be changed by SetFormat

	AESKey  string
	HMACKey string

	compression bool
}

// fileConsumer consumes messages from File using topic and partition specified during consumer creation
type fileConsumer struct {
	ctx     context.Context
	cancel  context.CancelFunc
	datadir string
	topic   string
	gen     string
	file    io.ReadCloser
	name    string
	reader  *bufio.Reader
	header  *Header
	fs      fs
	text    bool //Determined by the Format field of the file header. See openFile

	msg []byte
	err error

	AESKey     string
	HMACKey    string
	verifyHMAC bool

	compression bool
}

type hashWriter struct {
	writer io.Writer
	hash   hash.Hash
}

func (w *hashWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	_, _ = w.hash.Write(b)
	return n, err
}

func init() {
	registerPlugin("file", initFilePipe)
}

func initFilePipe(pctx context.Context, batchSize int, cfg *config.AppConfig, db *sql.DB) (Pipe, error) {
	return &filePipe{cfg.DataDir, cfg.MaxFileSize, cfg.PipeAES256Key, cfg.PipeHMACKey, cfg.PipeVerifyHMAC, cfg.PipeCompression}, nil
}

// Type returns Pipe type as File
func (p *filePipe) Type() string {
	return "file"
}

//NewProducer registers a new sync producer
func (p *filePipe) NewProducer(topic string) (Producer, error) {
	return &fileProducer{datadir: p.datadir, topic: topic, files: make(map[string]*file), maxFileSize: p.maxFileSize, fs: p, AESKey: p.AESKey, HMACKey: p.HMACKey, compression: p.compression}, nil
}

func (p *filePipe) initConsumer(c *fileConsumer) (Consumer, error) {
	c.ctx, c.cancel = context.WithCancel(context.Background())

	fn, offset, err := c.seek(c.topic, InitialOffset)
	if log.E(err) {
		return nil, err
	}

	if fn == "" {
		return c, nil
	}

	c.openFile(fn, offset)

	return c, nil
}

//NewConsumer registers a new file consumer with context
func (p *filePipe) NewConsumer(topic string) (Consumer, error) {
	c := &fileConsumer{datadir: p.datadir, topic: topic, fs: p, AESKey: p.AESKey, HMACKey: p.HMACKey, verifyHMAC: p.verifyHMAC, compression: p.compression}
	return p.initConsumer(c)
}

/*
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
*/

func topicPath(datadir string, topic string, gen string) string {
	var r string

	if datadir != "" {
		r += datadir + "/"
	}

	if topic != "" {
		r += topic + "/"
	}
	/*
		if gen != "" {
			r += gen + "/"
		}
	*/

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
	files, err := p.fs.ReadDir(p.topicPath(topic))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}

		return "", err
	}

	if len(files) == 0 {
		return "", nil
	}

	tp := p.topicPath(topic)
	i := sort.Search(len(files), func(i int) bool { return tp+files[i].Name() > curFile })

	if i < len(files) {
		log.Debugf("NextFile: %v,  CurFile: %v", files[i].Name(), curFile)
		return files[i].Name(), nil
	}

	return "", nil
}

func (p *fileConsumer) seek(topic string, offset int64) (string, int64, error) {
	files, err := p.fs.ReadDir(p.topicPath(topic))
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

func (p *fileProducer) newFileName(key string) string {
	p.seqno++ //Precaution to not generate file with the same name if timestamps are equal
	return fmt.Sprintf("%s%010d.%03d.%s.open", p.topicPath(p.topic), time.Now().Unix(), p.seqno, key)
}

func (p *fileProducer) newFile(key string) error {
	if err := p.fs.MkdirAll(p.topicPath(p.topic), 0770); err != nil {
		return err
	}

	n := p.newFileName(key)
	f, seeker, err := p.fs.OpenWrite(n)
	if err != nil {
		return err
	}

	var writer io.Writer = f

	var offset int64
	if seeker != nil {
		offset, err = seeker.Seek(0, os.SEEK_END)
		if err != nil {
			return err
		}
	}

	iv := make([]byte, aes.BlockSize)
	if p.AESKey != "" {
		if _, err := io.ReadFull(rand.Reader, iv); err != nil {
			return err
		}
		p.header.IV = fmt.Sprintf("%0x", iv)
	}

	if offset == 0 {
		p.header.Delimited = Delimited
		p.header.Filters = nil
		if p.AESKey != "" {
			p.header.Filters = append(p.header.Filters, "aes256-cfb")
		}
		if p.compression {
			p.header.Filters = append(p.header.Filters, "zlib")
		}
		var hash []byte
		if seeker != nil {
			hash = make([]byte, sha256.Size)
		}
		if err := writeHeader(&p.header, hash, writer); err != nil {
			return err
		}
	}

	hw := &hashWriter{writer, hmac.New(sha256.New, []byte(p.HMACKey))}
	writer = hw

	var crypter cipher.Stream
	if p.AESKey != "" {
		block, err := aes.NewCipher([]byte(p.AESKey))
		if err != nil {
			return err
		}

		crypter = cipher.NewCFBEncrypter(block, iv)
		writer = cipher.StreamWriter{S: crypter, W: writer}
	}

	var bufWriter writerFlusher = bufio.NewWriter(writer)
	if p.compression {
		bufWriter = zlib.NewWriter(writer)
	}

	_ = p.closeFile(p.files[key])

	log.Debugf("Opened: %v, %v compression: %v", key, n, p.compression)

	p.files[key] = &file{n, f, seeker, offset, crypter, hw, bufWriter}

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
		if f.seek != nil {
			if _, err := f.seek.Seek(0, os.SEEK_SET); log.E(err) {
				rerr = err
			}
			if err := writeHeader(&p.header, f.hash.hash.Sum(nil), f.file); log.E(err) {
				rerr = err
			}
		}
		if err := f.file.Close(); log.E(err) {
			rerr = err
		}
		if err := p.fs.Rename(f.name, strings.TrimSuffix(f.name, ".open")); log.E(err) {
			rerr = err
		}
		log.Debugf("Closed: %v", f.name)
	}
	return rerr
}

func (p *fileProducer) writeBinaryMsgLength(f *file, len int) error {
	if p.text || !Delimited {
		return nil
	}

	sz := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(sz, uint64(len))
	_, err := f.writer.Write(sz[:n])
	return err
}

func (p *fileProducer) writeTextMsgDelimiter(f *file) error {
	if !p.text || !Delimited {
		return nil
	}

	o := make([]byte, 0)
	o = append(o, delimiter)

	_, err := f.writer.Write(o)
	return err
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

	//Prepend message with size in the case of binary delimited format
	if err = p.writeBinaryMsgLength(f, len(bytes)); err != nil {
		return err
	}

	if _, err := f.writer.Write(bytes); err != nil {
		return err
	}

	//In the case of text format apppend delimiter after the message
	if err = p.writeTextMsgDelimiter(f); err != nil {
		return nil
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
		delete(p.files, key)
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
	delete(p.files, key)

	if len(data) == 0 {
		return nil
	}

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
		//Need to start watching before p.nextFile() to avoid race condition
		var watcher *fsnotify.Watcher
		watcher, p.err = p.waitForNextFilePrepare()
		if log.E(p.err) {
			return true
		}
		defer func() { log.E(watcher.Close()) }()

		nextFn, err := p.nextFile(p.topic, p.name)
		if log.E(p.err) {
			return true
		}

		if nextFn != "" && !strings.HasSuffix(nextFn, ".open") {
			p.openFile(nextFn, 0)
			return true
		}

		var ctxDone bool
		ctxDone, p.err = p.waitForNextFile(watcher)
		if log.E(err) {
			return true
		}

		if ctxDone {
			return false
		}
	}
}

func skipHeader(file io.Reader) error {
	b := make([]byte, 1)
	for b[0] != delimiter {
		if _, err := file.Read(b); log.E(err) {
			return err
		}
	}
	return nil
}

func (p *fileConsumer) checkHMAC(fn string, HMACRef string) error {
	file, err := p.fs.OpenRead(fn, 0)
	if log.E(err) {
		return err
	}

	if err = skipHeader(file); err != nil {
		return err
	}

	buf := make([]byte, 32768)
	h := hmac.New(sha256.New, []byte(p.HMACKey))

	for {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.E(err)
			return err
		}
		if n != 0 {
			_, _ = h.Write(buf[:n])
		}
	}

	err = file.Close()
	if log.E(err) {
		return err
	}

	if HMACRef != fmt.Sprintf("%0x", h.Sum(nil)) {
		return fmt.Errorf("File authentication failed")
	}

	log.Debugf("HMAC successfully verified for: %v", fn)
	return nil
}

func (p *fileConsumer) openFileInitFilter() (err error) {
	iv := make([]byte, hex.DecodedLen(len(p.header.IV)))
	_, p.err = hex.Decode(iv, []byte(p.header.IV))
	if p.err != nil {
		return
	}

	//TODO: Get encryption and compression type from Filters field of the header
	if p.AESKey != "" || p.compression {
		//Header reader cached more then just a header, so need to reopen
		log.E(p.file.Close())
		p.file, err = p.fs.OpenRead(p.name, 0)
		if log.E(err) {
			return
		}

		if err = skipHeader(p.file); err != nil {
			return
		}

		var reader io.Reader = p.file
		if p.AESKey != "" {
			var block cipher.Block
			block, err = aes.NewCipher([]byte(p.AESKey))
			if log.E(err) {
				return
			}

			crypter := cipher.NewCFBDecrypter(block, iv)
			reader = cipher.StreamReader{S: crypter, R: p.file}
		}

		if p.compression {
			reader, err = zlib.NewReader(reader)
			if log.E(err) {
				return
			}
		}

		p.reader = bufio.NewReader(reader)
	}

	return
}

func (p *fileConsumer) openFile(nextFn string, offset int64) {
	p.file, p.err = p.fs.OpenRead(p.topicPath(p.topic)+nextFn, 0)
	if log.E(p.err) {
		return
	}

	defer func() {
		if p.err != nil {
			p.reader = nil
			log.E(p.file.Close())
			p.file = nil
		}
	}()

	p.reader = bufio.NewReader(p.file)

	p.header, p.err = readHeader(p.reader)
	if log.E(p.err) {
		return
	}

	if !p.header.Delimited {
		p.err = fmt.Errorf("cannot consume non delimited file")
		log.E(p.err)
		return
	}

	p.text = p.header.Format == "json" || p.header.Format == "text"

	if p.verifyHMAC {
		p.err = p.checkHMAC(p.topicPath(p.topic)+nextFn, p.header.HMAC)
		if p.err != nil {
			return
		}
	}

	if offset != 0 {
		log.E(p.file.Close())
		p.file, p.err = p.fs.OpenRead(p.topicPath(p.topic)+nextFn, offset)
		if log.E(p.err) {
			return
		}
		p.reader = bufio.NewReader(p.file)
	}

	p.name = p.topicPath(p.topic) + nextFn

	p.err = p.openFileInitFilter()
	if log.E(p.err) {
		return
	}

	log.Debugf("Consumer opened: %v, header: %+v", p.name, p.header)
}

func (p *fileConsumer) fetchNextLow() bool {
	//reader and file can be nil when directory is empty during
	//NewConsumer
	if p.reader != nil {
		if !p.text {
			var sz uint64
			sz, p.err = binary.ReadUvarint(p.reader)
			if p.err == nil {
				p.msg = make([]byte, sz)
				_, p.err = io.ReadFull(p.reader, p.msg)
				if p.err == nil {
					log.Debugf("Consumed message: %x", p.msg)
				}
			}
		} else {
			p.msg, p.err = p.reader.ReadBytes(delimiter)
			if p.err == nil {
				p.msg = p.msg[:len(p.msg)-1]
				log.Debugf("Consumed message: %v", string(p.msg))
			}
		}
		if p.err == nil {
			return true
		}

		if p.err != io.EOF && (!p.compression || p.err != io.ErrUnexpectedEOF) {
			log.E(p.err)
			return true
		}

		log.E(p.file.Close())
		p.reader = nil
		p.file = nil
		log.Debugf("Consumer closed: %v", p.name)

		if p.text && Delimited && len(p.msg) != 0 {
			p.err = fmt.Errorf("Corrupted file. Not ending with delimiter: %v %v", p.name, string(p.msg))
			return true
		}

		p.err = nil
	}
	return false
}

//FetchNext fetches next message from File and commits offset read
func (p *fileConsumer) FetchNext() bool {
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
	p.text = format == "json" || format == "text"
}

func (p *filePipe) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (p *filePipe) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (p *filePipe) ReadDir(dirname string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

func (p *filePipe) OpenRead(name string, offset int64) (io.ReadCloser, error) {
	f, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (p *filePipe) OpenWrite(name string) (io.WriteCloser, io.Seeker, error) {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0640)
	return f, f, err
}
