package throttle

import (
	"os"
	"testing"
	"time"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/test"
)

var cfg *config.AppConfig

func testStep(n int64, target int64, iops int64, interval int64, samples int64, t *testing.T) {
	log.Debugf("n=%v target=%v iops=%v interval=%v samples=%v", n, target, iops, interval, samples)

	h := New(target, interval, samples)

	d := n * interval / target

	time.Sleep(time.Microsecond * time.Duration(interval))

	for i := 0; i < int(samples); i++ {
		h.Advice(iops)
		time.Sleep(time.Microsecond * time.Duration(interval))
	}

	var sum int64
	//for i := 0; i < 10; i++ {
	for n > 0 {
		s := h.Advice(iops)
		if s < 0 || s > interval {
			t.Fatalf("Throttle time is out of bounds: %v", s)
		}
		sum += s
		time.Sleep(time.Microsecond * time.Duration(interval))
		n -= target
	}

	log.Debugf("Total duration: %v, throttled time: %v", d, sum)
	log.Debugf("Got: %v, Expected: %v", float64(sum)/float64(d), float64(iops-target)/float64(iops))

	if target > iops {
		if sum != 0 {
			t.Fatalf("No throttle should be applied, got %v, maximum %v", sum, d)
		}
	} else if iops*(n/target) < n {
		if sum != d {
			t.Fatalf("Maximum throttle should be applied, got %v, maximum %v", sum, d)
		}
	} else if int64((float64(sum)*100/float64(d)))-int64((float64(iops-target)*100/float64(iops))) != 0 && int64((float64(sum)*1000/float64(d)+5)/100)-int64((float64(iops-target)*1000/float64(iops)+5)/100) != 0 {
		t.Fatalf("Got: %v, Expected: %v", int64((float64(sum)*1000/float64(d)+5)/100), int64((float64(iops-target)*1000/float64(iops)+5)/100))
	}
}

func TestThrottleBasic(t *testing.T) {
	t.Skip("Sleep based test is unstable in Travis")
	for i := int64(1); i <= 128; i *= 2 {
		for j := int64(1); j <= 128; j *= 2 {
			testStep(1024*i, i, j, 200, 5, t)
		}
	}
}

func TestZeroThrottleBasic(t *testing.T) {
	for j := int64(1); j <= 128; j *= 2 {
		h := New(0, 200, 5)
		s := h.Advice(j)
		if s != 0 {
			t.Fatalf("No throttling should happen for zero target")
		}
	}
}

func TestMain(m *testing.M) {
	cfg = test.LoadConfig()
	os.Exit(m.Run())
}
