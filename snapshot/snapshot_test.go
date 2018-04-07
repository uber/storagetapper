package snapshot

import (
	"os"
	"testing"

	"github.com/uber/storagetapper/test"
)

func TestCreateNonExistent(t *testing.T) {
	_, err := InitReader("not_existent_plugin")
	test.Assert(t, err != nil, "should return error")
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	if m.Run() != 0 {
		os.Exit(1)
	}

	runBenchmarks()
}
