package pipe

import (
	"bytes"
	"crypto"
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/armor"
	"golang.org/x/crypto/openpgp/packet"

	"github.com/stretchr/testify/require"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/test"
)

var baseDir = "/tmp/storagetapper/file_pipe_test"

func deleteTestTopics(t *testing.T) {
	err := os.RemoveAll(baseDir)
	test.CheckFail(err, t)

	err = os.MkdirAll(baseDir, 0770)
	test.CheckFail(err, t)
}

func initTestFilePipe(pcfg *config.PipeConfig, encryption bool, t *testing.T) *filePipe {
	fp := &filePipe{datadir: baseDir, cfg: *pcfg}
	if encryption {
		fp.cfg.Encryption.Enabled = true
		fp.cfg.Encryption.PublicKey, fp.cfg.Encryption.PrivateKey = genTestKeys(t)
		fp.cfg.Encryption.SigningKey = fp.cfg.Encryption.PrivateKey
	}
	return fp
}

func testFileBasic(pcfg *config.PipeConfig, encryption bool, t *testing.T) {
	p := initTestFilePipe(pcfg, encryption, t)

	startCh = make(chan bool)

	deleteTestTopics(t)
	testLoop(p, t, NOKEY)

	deleteTestTopics(t)
	testLoop(p, t, KEY)

	deleteTestTopics(t)
	testLoop(p, t, BATCH)
}

func TestFileBasic(t *testing.T) {
	pcfg := cfg.Pipe
	pcfg.MaxFileSize = 1024
	testFileBasic(&pcfg, false, t)
}

func TestFileSmall(t *testing.T) {
	pcfg := cfg.Pipe
	pcfg.MaxFileSize = 1
	testFileBasic(&pcfg, false, t)
}

/*
func TestFileHeader(t *testing.T) {
	deleteTestTopics(t)

	fp := initTestFilePipe(&cfg.Pipe, false, t)

	p, err := fp.NewProducer("header-test-topic")
	require.NoError(t, err)

	c, err := fp.NewConsumer("header-test-topic")
	require.NoError(t, err)

	p.SetFormat("json")

	err = p.PushSchema("", []byte("schema-to-test-header"))
	require.NoError(t, err)

	msg := `{"Test" : "file data"}`
	require.NoError(t, p.Push([]byte(msg)))

	require.NoError(t, p.Close())

	require.True(t, c.FetchNext(), "there should be a schema message")
	m, err := c.Pop()
	require.NoError(t, err)

	require.Equal(t, "schema-to-test-header", string(m.([]byte)), "first message should be schema message")

	require.True(t, c.FetchNext(), "there should be exactly one data message")
	m, err = c.Pop()
	require.NoError(t, err)

	require.Equal(t, msg, string(m.([]byte)))

	h := c.(*fileConsumer).header
	require.Equal(t, "json", h.Format)
	require.Equal(t, "schema-to-test-header", string(h.Schema))
	//	require.Equal(t, "79272b31d679f9ff72457002da87e985dad203a281ac84de6b5420631f9cb17c", h.HMAC)
	require.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", h.HMAC)

	require.NoError(t, c.Close())
}
*/

func TestFileBinary(t *testing.T) {
	deleteTestTopics(t)

	fp := initTestFilePipe(&cfg.Pipe, false, t)

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

	test.Assert(t, string(m.([]byte)) == msg1, "read back incorrect first message ")

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

	fp := initTestFilePipe(&cfg.Pipe, false, t)
	fp.cfg.FileDelimited = false

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
	test.Assert(t, err != nil && err.Error() == "cannot consume non delimited file", "should produce an error")

	err = c.Close()
	test.CheckFail(err, t)

	dc, err := ioutil.ReadDir(baseDir)
	test.CheckFail(err, t)
	test.Assert(t, len(dc) == 1, "expect exactly one file in the directory")

	r, err := ioutil.ReadFile(baseDir + "/" + dc[0].Name())
	test.CheckFail(err, t)
	//test.Assert(t, string(r) == `{"Format":"json","HMAC-SHA256":"e8bf2c23a49dda570ac39e0a90683fe305620263f9d50ade99f835d3bc0bb05e"}
	test.Assert(t, string(r) == `firstsecond`, "file content mismatch")
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

	fp := initTestFilePipe(&cfg.Pipe, false, t)

	p, err := fp.NewProducer(topic)
	test.CheckFail(err, t)

	p.SetFormat("json")

	//By default consumers see only messages produced after creation
	//InitialOffset = OffsetNewest
	c1, err := fp.NewConsumer(topic)
	test.CheckFail(err, t)
	c1.SetFormat("json")

	msg1 := `{"Test" : "filedata1"}`
	err = p.Push([]byte(msg1))
	test.CheckFail(err, t)

	//This consumer will not see msg1
	c2, err := fp.NewConsumer(topic)
	test.CheckFail(err, t)
	c2.SetFormat("json")

	//Change default InitialOffset to OffsetOldest
	saveOffset := InitialOffset
	InitialOffset = OffsetOldest
	defer func() { InitialOffset = saveOffset }()

	//This consumers will see both bmesages
	c3, err := fp.NewConsumer(topic)
	test.CheckFail(err, t)
	c3.SetFormat("json")

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
	p, _ := initFilePipe(&cfg.Pipe, nil)
	test.Assert(t, p.Type() == pt, "type should be "+pt)
}

func genTestKeys(t *testing.T) (string, string) {
	e, err := openpgp.NewEntity("fpt_name", "fpt_comment", "fpt@example.com", &packet.Config{DefaultHash: crypto.SHA256})
	test.CheckFail(err, t)

	privKey := bytes.Buffer{}
	w, err := armor.Encode(&privKey, openpgp.PrivateKeyType, nil)
	test.CheckFail(err, t)

	for _, id := range e.Identities {
		err := id.SelfSignature.SignUserId(id.UserId.Id, e.PrimaryKey, e.PrivateKey, nil)
		test.CheckFail(err, t)
	}

	test.CheckFail(e.SerializePrivate(w, nil), t)
	test.CheckFail(w.Close(), t)

	pubKey := bytes.Buffer{}
	w, err = armor.Encode(&pubKey, openpgp.PublicKeyType, nil)
	test.CheckFail(err, t)
	test.CheckFail(e.Serialize(w), t)
	test.CheckFail(w.Close(), t)

	return pubKey.String(), privKey.String()
}

func TestFileEncryption(t *testing.T) {
	pcfg := cfg.Pipe
	pcfg.MaxFileSize = 1
	testFileBasic(&pcfg, true, t)
}

func TestFileEncryptionBinary(t *testing.T) {
	deleteTestTopics(t)

	pcfg := cfg.Pipe
	pcfg.MaxFileSize = 1024

	fp := initTestFilePipe(&pcfg, false, t)

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
	pcfg := cfg.Pipe
	pcfg.Compression = true
	pcfg.MaxFileSize = 1
	testFileBasic(&pcfg, false, t)
}

func TestFileCompressionAndEncryption(t *testing.T) {
	pcfg := cfg.Pipe
	pcfg.MaxFileSize = 1
	pcfg.Compression = true
	testFileBasic(&pcfg, true, t)
}

func TestFileNoHeader(t *testing.T) {
	pcfg := cfg.Pipe
	pcfg.MaxFileSize = 1
	testFileBasic(&pcfg, false, t)
}

func TestFilePartitionKey(t *testing.T) {
	fp := &filePipe{}
	p, err := fp.NewProducer("partition-key-test-topic")
	test.CheckFail(err, t)
	key := "some key"
	test.Assert(t, p.PartitionKey("log", key) == "log", "file pipe should return log for any key")
	key = "other key"
	test.Assert(t, p.PartitionKey("snapshot", key) == "snapshot", "file pipe should return snapshot for any key")
}

func TestFileDumpStat(t *testing.T) {
	deleteTestTopics(t)

	fp := initTestFilePipe(&cfg.Pipe, false, t)
	fp.cfg.EndOfStreamMark = true
	fp.cfg.MaxFileSize = 1

	p, err := fp.NewProducer("header-test-topic/")
	require.NoError(t, err)

	msg := `{"Test" : "msg goes to first file"}`
	require.NoError(t, p.Push([]byte(msg)))

	msg = `{"Test" : "msg goes to second file"}`
	require.NoError(t, p.Push([]byte(msg)))

	require.NoError(t, p.Close())

	b, err := ioutil.ReadFile(baseDir + "/header-test-topic/_DONE")
	require.NoError(t, err)
	re := regexp.MustCompile(`/(.\d+)\.`)
	s := re.ReplaceAllString(string(b), "/1568094981.")
	require.Equal(t, `[{"NumRecs":1,"Hash":"1659724ce4460a14d8ddb1d370191fc73efeac3ba7a0ce067998e25c35c2aab4","FileName":"/tmp/storagetapper/file_pipe_test/header-test-topic/1568094981.001.default"},{"NumRecs":1,"Hash":"cadc2e6510f196d15b82a777ca85cd639ab54002bf10bb90606fc2be0129358a","FileName":"/tmp/storagetapper/file_pipe_test/header-test-topic/1568094981.002.default"}]`, s)
}
