package pipe

import (
	"os"
	"testing"

	"github.com/colinmarc/hdfs"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/test"
)

var path = "/user/" + os.Getenv("USER") + "/tmp/hdfs_pipe_test"

func deleteTestHdfsTopics(t *testing.T) {
	client, err := hdfs.New(cfg.HadoopAddress)
	test.CheckFail(err, t)

	err = client.Remove(path)
	if !os.IsNotExist(err) {
		test.CheckFail(err, t)
	}

	err = client.MkdirAll(path, 0770) //Pipe calls mkdirall so this may not be needed
	test.CheckFail(err, t)
}

func testHdfsBasic(size int64, t *testing.T) {
	cfg.DataDir = path

	p, err := initHdfsPipe(shutdown.Context, 128, cfg, nil)
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
	testHdfsBasic(1024, t)
}

func TestHdfsSmall(t *testing.T) {
	testHdfsBasic(1, t)
}
