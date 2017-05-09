package throttle

import "time"

//Throttle implements generic Throttler
type Throttle struct {
	CheckInterval int64
	NumSamples    int64
	Target        int64

	current int64

	samples     []int64
	sumSamples  int64
	samplesHand int64

	ticker *time.Ticker
}

//New creates new Throttler
func New(target int64, checkInterval int64, numSamples int64) *Throttle {
	return &Throttle{checkInterval, numSamples, target, 0, make([]int64, numSamples), 0, 0, time.NewTicker(time.Microsecond * time.Duration(checkInterval))}
}

func (t *Throttle) tick() int64 {
	t.sumSamples -= t.samples[t.samplesHand]
	t.sumSamples += t.current
	t.samples[t.samplesHand] = t.current
	t.current = 0
	t.samplesHand++
	if t.samplesHand >= t.NumSamples {
		t.samplesHand = 0
	}

	avg := t.sumSamples / int64(t.NumSamples)
	if avg <= t.Target {
		return 0
	}

	return t.CheckInterval - t.CheckInterval*t.Target/(avg)
}

//Advice return a sleep advice to satisfy throttling requirements
func (t *Throttle) Advice(add int64) int64 {
	var s int64

	t.current += add

	if t.Target == 0 {
		return 0
	}

	select {
	case <-t.ticker.C:
		s = t.tick()
	default:
	}

	return s
}
