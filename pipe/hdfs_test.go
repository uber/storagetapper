package pipe

import (
	"os"
	"testing"

	"github.com/efirs/hdfs/v2"
	"github.com/uber/storagetapper/test"
)

func deleteTestHdfsTopics(t *testing.T) {
	cp := hdfs.ClientOptions{User: cfg.Pipe.Hadoop.User, Addresses: cfg.Pipe.Hadoop.Addresses}
	client, err := hdfs.NewClient(cp)
	test.CheckFail(err, t)

	err = client.RemoveAll(cfg.Pipe.Hadoop.BaseDir)
	if !os.IsNotExist(err) {
		test.CheckFail(err, t)
	}

	err = client.MkdirAll(cfg.Pipe.Hadoop.BaseDir, 0770) //Pipe calls mkdirall so this may not be needed
	test.CheckFail(err, t)
}

func testHdfsBasic(size int64, pubKey string, privKey string, signKey string, t *testing.T) {
	pcfg := cfg.Pipe
	pcfg.FileDelimited = true
	p, err := initHdfsPipe(&pcfg, nil)
	test.CheckFail(err, t)
	h, ok := p.(*hdfsPipe)
	test.Assert(t, ok, "Unexpected pipe type")
	h.cfg.Encryption.PrivateKey = privKey
	h.cfg.Encryption.PublicKey = pubKey
	h.cfg.Encryption.SigningKey = privKey

	startCh = make(chan bool)

	deleteTestHdfsTopics(t)
	testLoop(p, t, NOKEY)

	deleteTestHdfsTopics(t)
	testLoop(p, t, KEY)
}

func TestHdfsBasic(t *testing.T) {
	testHdfsBasic(1024, "", "", "", t)
}

func TestHdfsSmall(t *testing.T) {
	testHdfsBasic(1, "", "", "", t)
}

func TestHdfsType(t *testing.T) {
	pt := "hdfs"
	p, err := initHdfsPipe(&cfg.Pipe, nil)
	test.CheckFail(err, t)
	test.Assert(t, p.Type() == pt, "type should be "+pt)
}

func TestHdfsEncryption(t *testing.T) {
	pubKey, privKey := genTestKeys(t)
	testHdfsBasic(1, pubKey, privKey, privKey, t)
}

func TestHdfsCompression(t *testing.T) {
	cfg.Pipe.Compression = true
	defer func() { cfg.Pipe.Compression = false }()
	testHdfsBasic(1, "", "", "", t)
}

func TestHdfsCompressionAndEncryption(t *testing.T) {
	cfg.Pipe.Compression = true
	defer func() { cfg.Pipe.Compression = false }()
	pubKey, privKey := genTestKeys(t)
	testHdfsBasic(1, pubKey, privKey, privKey, t)
}
