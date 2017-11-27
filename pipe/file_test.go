package pipe

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/test"
)

var baseDir = "/tmp/storagetapper/file_pipe_test"

func deleteTestTopics(t *testing.T) {
	err := os.RemoveAll(baseDir)
	test.CheckFail(err, t)

	err = os.MkdirAll(baseDir, 0770)
	test.CheckFail(err, t)
}

func testFileBasic(size int64, t *testing.T) {
	p := &filePipe{baseDir, size}

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

	fp := &filePipe{baseDir, 1024}

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

func TestBinary(t *testing.T) {
	deleteTestTopics(t)

	fp := &filePipe{baseDir, 1024}

	p, err := fp.NewProducer("binary-test-topic")
	test.CheckFail(err, t)
	p.SetFormat("binary") // anything !json && !text are binary

	c, err := fp.NewConsumer("binary-test-topic")
	test.CheckFail(err, t)

	msg1 := `first`
	err = p.Push([]byte(msg1))
	test.CheckFail(err, t)

	msg2 := `second`
	err = p.Push([]byte(msg2))
	test.CheckFail(err, t)

	err = p.Close()
	test.CheckFail(err, t)

	test.Assert(t, c.FetchNext(), "there should be first message")

	m, err := c.Pop()
	test.CheckFail(err, t)

	test.Assert(t, string(m.([]byte)) == msg1, "read back incorrect first message")

	test.Assert(t, c.FetchNext(), "there should be second message")

	m, err = c.Pop()
	test.CheckFail(err, t)

	test.Assert(t, string(m.([]byte)) == msg2, "read back incorrect first message")

	err = c.Close()
	test.CheckFail(err, t)
}

func TestNoDelimiter(t *testing.T) {
	deleteTestTopics(t)

	topic := "no-delimiter-test-topic"
	delimited = false

	fp := &filePipe{baseDir, 1024}

	p, err := fp.NewProducer(topic)
	test.CheckFail(err, t)
	p.SetFormat("json")

	c, err := fp.NewConsumer(topic)
	test.CheckFail(err, t)

	msg1 := `first`
	err = p.Push([]byte(msg1))
	test.CheckFail(err, t)

	msg2 := `second`
	err = p.Push([]byte(msg2))
	test.CheckFail(err, t)

	err = p.Close()
	test.CheckFail(err, t)

	test.Assert(t, c.FetchNext(), "there should be message with error set")

	_, err = c.Pop()
	test.Assert(t, err.Error() == "cannot consume non delimited file", err.Error())

	err = c.Close()
	test.CheckFail(err, t)

	dc, err := ioutil.ReadDir(baseDir + "/" + topic)
	test.CheckFail(err, t)
	test.Assert(t, len(dc) == 1, "expect exactly one file in the directory")

	r, err := ioutil.ReadFile(baseDir + "/" + topic + "/" + dc[0].Name())
	test.Assert(t, string(r) == `{"Format":"json","SHA256":"da83f63e1a473003712c18f5afc5a79044221943d1083c7c5a7ac7236d85e8d2"}
firstsecond`, "file content mismatch")

	delimited = true
}
