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

func TestHeader(t *testing.T) {
	deleteTestTopics(t)

	fp := &FilePipe{"/tmp/file_pipe_test", 1024}

	p, err := fp.NewProducer("header-test-topic")
	test.CheckFail(err, t)

	c, err := fp.NewConsumer("header-test-topic")
	test.CheckFail(err, t)

	p.SetFormat("json")

	err = p.PushSchema("key", []byte("schema-to-test-header"))
	test.CheckFail(err, t)

	msg := `{"Test" : "file data"}`
	err = p.Push([]byte(msg))
	test.CheckFail(err, t)

	err = p.Close()
	test.CheckFail(err, t)

	test.Assert(t, c.FetchNext(), "there should be schema message")

	m, err := c.Pop()
	test.CheckFail(err, t)

	test.Assert(t, string(m.([]byte)) == "schema-to-test-header", "first message should be schema")

	test.Assert(t, c.FetchNext(), "there should be exactly one data message")

	m, err = c.Pop()
	test.CheckFail(err, t)

	test.Assert(t, string(m.([]byte)) == msg, "read back incorrect message")

	h := c.(*fileConsumer).header

	test.Assert(t, h.Format == "json", "unexpected")
	test.Assert(t, string(h.Schema) == "schema-to-test-header", "unexpected")
	test.Assert(t, h.HashSum == "d814ab34da9e76c671066fa47d865c7afa7487f18225bf97ca849c080065536d", "unexpected")

	err = c.Close()
	test.CheckFail(err, t)
}
