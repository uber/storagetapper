package pipe

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
)

func prepareTestS3Topics(t *testing.T) {
	u, err := uuid.NewV4()
	test.CheckFail(err, t)
	cfg.Pipe.S3.BaseDir = "/tmp/s3_pipe_test/" + u.String()
	log.Debugf("Test directory %v", cfg.Pipe.S3.BaseDir)
}

func testS3Basic(size int64, pt int, t *testing.T) {
	prepareTestS3Topics(t)

	pcfg := cfg.Pipe
	pcfg.FileDelimited = true
	p, err := initS3Pipe(&pcfg, nil)
	test.CheckFail(err, t)

	startCh = make(chan bool)

	testLoop(p, t, pt)
}

func TestS3Basic(t *testing.T) {
	testS3Basic(1024, NOKEY, t)
	testS3Basic(1024, KEY, t)
}

func TestS3Small(t *testing.T) {
	testS3Basic(1, NOKEY, t)
	testS3Basic(1, KEY, t)
}

func TestS3Type(t *testing.T) {
	pt := "s3"
	p, _ := initS3Pipe(&cfg.Pipe, nil)
	test.Assert(t, p.Type() == pt, "type should be "+pt)
}
