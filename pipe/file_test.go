package pipe

import (
	"os"
	"testing"

	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/test"
)

func deleteTestTopics(t *testing.T) {
	err := os.RemoveAll("/tmp/file_pipe_test")
	test.CheckFail(err, t)

	err = os.Mkdir("/tmp/file_pipe_test", 0770)
	test.CheckFail(err, t)
}

func testFileBasic(size int64, t *testing.T) {
	p := &FilePipe{"/tmp/file_pipe_test", size}

	startCh = make(chan bool)

	shutdown.Setup()
	defer func() {
		shutdown.Initiate()
		shutdown.Wait()
	}()

	deleteTestTopics(t)
	testLoop(p, t, NOKEY)

	deleteTestTopics(t)
	testLoop(p, t, KEY)
}

func TestFileBasic(t *testing.T) {
	testFileBasic(1024, t)
}

func TestSmallFile(t *testing.T) {
	testFileBasic(1, t)
}
