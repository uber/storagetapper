package pipe

import (
	"os"
	"testing"

	"github.com/colinmarc/hdfs"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/test"
)

func deleteTestHdfsTopics(t *testing.T) {
	cp := hdfs.ClientOptions{User: cfg.Hadoop.User, Addresses: cfg.Hadoop.Addresses}
	client, err := hdfs.NewClient(cp)
	test.CheckFail(err, t)

	err = client.Remove(cfg.Hadoop.BaseDir)
	if !os.IsNotExist(err) {
		test.CheckFail(err, t)
	}

	err = client.MkdirAll(cfg.Hadoop.BaseDir, 0770) //Pipe calls mkdirall so this may not be needed
	test.CheckFail(err, t)
}

func testHdfsBasic(size int64, t *testing.T) {
	s := Delimited
	Delimited = true
	p, err := initHdfsPipe(shutdown.Context, 128, cfg, nil)
	Delimited = s
	test.CheckFail(err, t)

	startCh = make(chan bool)

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	deleteTestHdfsTopics(t)
	testLoop(p, t, NOKEY)

	deleteTestHdfsTopics(t)
	testLoop(p, t, KEY)
}

func TestHdfsBasic(t *testing.T) {
	t.Skip("Flaky in VM env. Skip for now")
	testHdfsBasic(1024, t)
}

func TestHdfsSmall(t *testing.T) {
	t.Skip("Flaky in VM env. Skip for now")
	testHdfsBasic(1, t)
}

func TestHdfsType(t *testing.T) {
	pt := "hdfs"
	p, _ := initHdfsPipe(nil, 0, cfg, nil)
	test.Assert(t, p.Type() == pt, "type should be "+pt)
}
