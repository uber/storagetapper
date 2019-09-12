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
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"golang.org/x/net/context" //"context"
)

type s3Client struct {
	client     *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
	opTimeout  time.Duration
}

//Reader and writer interfaces
type s3Writer struct {
	w      *io.PipeWriter
	ch     chan error
	cancel context.CancelFunc
}

type s3Reader struct {
	r      *io.PipeReader
	ch     chan error
	cancel context.CancelFunc
}

func (p *s3Writer) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p *s3Writer) Close() error {
	_ = p.w.Close()
	return <-p.ch
}

func (p *s3Writer) Flush() error {
	return nil
}

func (p *s3Reader) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

func (p *s3Reader) Close() error {
	return p.r.Close()
}

//s3WriterAt is an adapter to use PipeWriter as a WriterAt
//this is possible because we use streaming download
type s3WriterAt struct {
	w io.Writer
}

func (p *s3WriterAt) WriteAt(b []byte, off int64) (n int, err error) {
	return p.w.Write(b)
}

func (p *s3Client) OpenRead(name string, offset int64) (io.ReadCloser, error) {
	log.Debugf("OpenRead: %v offset=%v", name, offset)
	if offset != 0 {
		return nil, fmt.Errorf("s3 read is non seekable")
	}
	r, w := io.Pipe()
	ch := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), p.opTimeout)
	go func() {
		defer cancel()
		_, err := p.downloader.DownloadWithContext(ctx, &s3WriterAt{w}, &s3.GetObjectInput{Bucket: &p.bucket, Key: &name})
		if cerr := w.Close(); err == nil {
			err = cerr
		}
		ch <- err
	}()
	return &s3Reader{r: r, ch: ch, cancel: cancel}, nil
}

func (p *s3Client) OpenWrite(name string) (flushWriteCloser, io.Seeker, error) {
	if strings.HasSuffix(name, ".open") {
		name = strings.TrimSuffix(name, ".open")
	}
	log.Debugf("OpenWrite: %v", name)
	r, w := io.Pipe()
	ch := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), p.opTimeout)
	go func() {
		defer cancel()
		_, err := p.uploader.UploadWithContext(ctx, &s3manager.UploadInput{Bucket: &p.bucket, Key: &name, Body: r})
		ch <- err
	}()
	return &s3Writer{w: w, ch: ch, cancel: cancel}, nil, nil
}

func (p *s3Client) MkdirAll(path string, _ os.FileMode) error {
	return nil
}

//implements os.FileInfo interface for s3.ListItem
type s3Stat struct {
	*s3.Object
}

func (fs *s3Stat) Size() int64 {
	if fs.Object.Size == nil {
		return 0
	}
	return *fs.Object.Size
}
func (fs *s3Stat) ModTime() time.Time {
	if fs.LastModified == nil {
		return time.Time{}
	}
	return *fs.LastModified
}
func (fs *s3Stat) Mode() os.FileMode { return 0777 }
func (fs *s3Stat) Sys() interface{}  { return *fs.Object }
func (fs *s3Stat) Name() string      { return *fs.Object.Key }
func (fs *s3Stat) IsDir() bool       { return false }

func (p *s3Client) ReadDir(dirname string, _ string) (res []os.FileInfo, err error) {
	log.Debugf("ReadDir: %v", dirname)

	if len(dirname) != 0 && dirname[len(dirname)-1] != '/' {
		dirname = dirname + "/"
	}

	if len(dirname) != 0 && dirname[0] == '/' {
		dirname = dirname[1:]
	}

	resp := &s3.ListObjectsOutput{IsTruncated: aws.Bool(true)}

	for aws.BoolValue(resp.IsTruncated) {
		resp, err = p.client.ListObjects(&s3.ListObjectsInput{Bucket: &p.bucket, Prefix: &dirname, Marker: resp.Marker})
		if err != nil {
			return nil, err
		}

		for _, v := range resp.Contents {
			*v.Key = strings.TrimPrefix(*v.Key, dirname)
			res = append(res, &s3Stat{v})
		}
	}

	return res, nil
}

func (p *s3Client) Rename(oldpath, newpath string) error {
	return nil
}

func (p *s3Client) Remove(path string) error {
	return nil
}

func (p *s3Client) Cancel(f io.Closer) error {
	w, ok := f.(*s3Writer)
	if ok {
		w.cancel()
		return <-w.ch
	}
	r, ok := f.(*s3Reader)
	if ok {
		r.cancel()
		return <-r.ch
	}
	return fmt.Errorf("wrong type")
}

type s3Pipe struct {
	filePipe
	client *s3Client
}

// s3Consumer consumes messages from Terrablob using topic specified during consumer creation
type s3Consumer struct {
	fileConsumer
}

func init() {
	registerPlugin("s3", initS3Pipe)
}

func initS3Pipe(cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	w := &aws.Config{Region: &cfg.S3.Region, Endpoint: &cfg.S3.Endpoint, S3ForcePathStyle: aws.Bool(true)}
	if cfg.S3.AccessKeyID != "" {
		w.Credentials = credentials.NewStaticCredentials(cfg.S3.AccessKeyID, cfg.S3.SecretAccessKey, cfg.S3.SessionToken)
	}
	s, err := session.NewSession(w)
	if log.E(err) {
		return nil, err
	}
	client := s3.New(s)
	result, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: &cfg.S3.Bucket,
	})
	if log.E(err) {
		aerr, ok := err.(awserr.Error)
		if !ok || (aerr.Code() != s3.ErrCodeBucketAlreadyExists && aerr.Code() != s3.ErrCodeBucketAlreadyOwnedByYou) {
			return nil, err
		}
	} else {
		log.Infof("S3 bucket created in: %s", result)
	}

	uploader := s3manager.NewUploaderWithClient(client, func(d *s3manager.Uploader) {
		d.Concurrency = 1
	})
	downloader := s3manager.NewDownloaderWithClient(client, func(d *s3manager.Downloader) {
		d.Concurrency = 1
	})

	return &s3Pipe{filePipe{cfg.S3.BaseDir, *cfg}, &s3Client{client, uploader, downloader, cfg.S3.Bucket, cfg.S3.Timeout}}, nil
}

// Type returns Pipe type as Terrablob
func (p *s3Pipe) Type() string {
	return "s3"
}

//NewProducer registers a new Terrablob producer
func (p *s3Pipe) NewProducer(topic string) (Producer, error) {
	m := metrics.NewFilePipeMetrics("pipe_producer", map[string]string{"topic": topic, "pipeType": "s3"})
	return &fileProducer{filePipe: &p.filePipe, topic: topic, files: make(map[string]*file), fs: p.client, metrics: m, stats: make(map[string]*stat)}, nil
}

//NewConsumer registers a new Terrablob consumer
func (p *s3Pipe) NewConsumer(topic string) (Consumer, error) {
	m := metrics.NewFilePipeMetrics("pipe_consumer", map[string]string{"topic": topic, "pipeType": "s3"})
	c := &s3Consumer{fileConsumer{filePipe: &p.filePipe, topic: topic, fs: p.client, metrics: m}}
	_, err := p.initConsumer(&c.fileConsumer)
	return c, err
}

//FetchNext fetches next message from Terrablob
func (p *s3Consumer) FetchNext() bool {
	for {
		if p.fetchNextLow() {
			return true
		}
		if !p.waitAndOpenNextFilePoll() {
			return false //context canceled, no message
		}
		if p.err != nil {
			return true //has message with error set
		}
	}
}
