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

func testFileBasic(size int64, AESKey string, HMACKey string, t *testing.T) {
	verifyHMAC := HMACKey != ""
	p := &filePipe{datadir: baseDir, maxFileSize: size, AESKey: AESKey, HMACKey: HMACKey, verifyHMAC: verifyHMAC, compression: cfg.PipeCompression}

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

	deleteTestTopics(t)
	testLoop(p, t, BATCH)
}

func TestFileBasic(t *testing.T) {
	testFileBasic(1024, "", "", t)
}

func TestFileSmall(t *testing.T) {
	testFileBasic(1, "", "", t)
}

func TestFileHeader(t *testing.T) {
	deleteTestTopics(t)

	fp := &filePipe{datadir: baseDir, maxFileSize: 1024}

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
	test.Assert(t, h.HMAC == "79272b31d679f9ff72457002da87e985dad203a281ac84de6b5420631f9cb17c", "unexpected")

	err = c.Close()
	test.CheckFail(err, t)
}

func TestFileBinary(t *testing.T) {
	deleteTestTopics(t)

	fp := &filePipe{datadir: baseDir, maxFileSize: 1024}

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

func TestFileNoDelimiter(t *testing.T) {
	deleteTestTopics(t)

	topic := "no-delimiter-test-topic"
	Delimited = false

	fp := &filePipe{datadir: baseDir, maxFileSize: 1024}

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
	test.Assert(t, string(r) == `{"Format":"json","HMAC-SHA256":"e8bf2c23a49dda570ac39e0a90683fe305620263f9d50ade99f835d3bc0bb05e"}
firstsecond`, "file content mismatch")

	Delimited = true
}

func consumeAndCheck(t *testing.T, c Consumer, msg string) {
	test.Assert(t, c.FetchNext(), "there should be a message: %v", msg)

	m, err := c.Pop()
	test.CheckFail(err, t)

	got := string(m.([]byte))
	test.Assert(t, got == msg, "read back incorrect message: %v", got)
}

func TestFileOffsets(t *testing.T) {
	topic := "file-offsets-test-topic"
	deleteTestTopics(t)

	fp := &filePipe{datadir: baseDir, maxFileSize: 1024}

	p, err := fp.NewProducer(topic)
	test.CheckFail(err, t)

	p.SetFormat("json")

	//By default consumers see only messages produced after creation
	//InitialOffset = OffsetNewest
	c1, err := fp.NewConsumer(topic)
	test.CheckFail(err, t)

	msg1 := `{"Test" : "filedata1"}`
	err = p.Push([]byte(msg1))
	test.CheckFail(err, t)

	//This consumer will not see msg1
	c2, err := fp.NewConsumer(topic)
	test.CheckFail(err, t)

	//Change default InitialOffset to OffsetOldest
	saveOffset := InitialOffset
	InitialOffset = OffsetOldest
	defer func() { InitialOffset = saveOffset }()

	//This consumers will see both bmesages
	c3, err := fp.NewConsumer(topic)
	test.CheckFail(err, t)

	msg2 := `{"Test" : "filedata2"}`

	err = p.Push([]byte(msg2))
	test.CheckFail(err, t)

	err = p.Close()
	test.CheckFail(err, t)

	consumeAndCheck(t, c1, msg1)
	consumeAndCheck(t, c1, msg2)

	consumeAndCheck(t, c2, msg2)

	consumeAndCheck(t, c3, msg1)
	consumeAndCheck(t, c3, msg2)

	test.CheckFail(c1.Close(), t)
	test.CheckFail(c2.Close(), t)
	test.CheckFail(c3.Close(), t)
}

func TestFileType(t *testing.T) {
	pt := "file"
	p, _ := initFilePipe(nil, 0, cfg, nil)
	test.Assert(t, p.Type() == pt, "type should be "+pt)
}

func TestFileEncryption(t *testing.T) {
	AESKey := "12345678901234567890123456789012"
	testFileBasic(1, AESKey, "", t)
}

func TestFileVerifyHMAC(t *testing.T) {
	AESKey := "12345678901234567890123456789012"
	HMACKey := "qwertyuiopasdfghjklzxcvbnmqwerty"
	testFileBasic(1, AESKey, HMACKey, t)
}

func TestFileEncryptionBinary(t *testing.T) {
	deleteTestTopics(t)

	AESKey := "12345678901234567890123456789012"
	HMACKey := "qwertyuiopasdfghjklzxcvbnmqwerty"

	fp := &filePipe{datadir: baseDir, maxFileSize: 1024, AESKey: AESKey, HMACKey: HMACKey, verifyHMAC: true}

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

func TestFileCompression(t *testing.T) {
	cfg.PipeCompression = true
	defer func() { cfg.PipeCompression = false }()
	testFileBasic(1, "", "", t)
}

func TestFileCompressionAndEncryption(t *testing.T) {
	cfg.PipeCompression = true
	defer func() { cfg.PipeCompression = false }()
	AESKey := "12345678901234567890123456789012"
	HMACKey := "qwertyuiopasdfghjklzxcvbnmqwerty"
	testFileBasic(1, AESKey, HMACKey, t)
}
