// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import "time"

type counter interface {
	Update(value int64)
	Tag(tags map[string]string)
}

type timer interface {
	Start()
	Stop()
	Record(time.Duration)
	Tag(tags map[string]string)
}

type metricsFactory interface {
	InitCounter(name string) counter
	InitTimer(name string) timer
}

type metricsConstructor func(*Metrics) error

var metricsInit metricsConstructor = noopMetricsInit

// Metrics contains statistical variables we report for monitoring
type Metrics struct {
	factory metricsFactory

	NumTablesRegistered *Counter // +
	IdleWorkers         *ProcessCounter
	//TODO: ERRORS
}

//Events represents the common structure for event based metrics
type Events struct {
	NumWorkers    *ProcessCounter
	EventsRead    *Counter
	EventsWritten *Counter
	BytesRead     *Counter
	BytesWritten  *Counter
	BatchSize     *Timer

	ReadLatency    *Timer
	ProduceLatency *Timer
}

//Snapshot contains metrics related to snapshot reader
type Snapshot struct {
	Events
}

//Streamer contains metrics related to event streamer
type Streamer struct {
	Events
	TimeInBuffer *Timer
}

//BinlogReader contains metrics related to binlog reader
type BinlogReader struct {
	Events

	BinlogRowEventsWritten   *Counter
	BinlogQueryEventsWritten *Counter
	BinlogUnhandledEvents    *Counter

	TimeToEncounter *Timer

	NumTablesIngesting *Counter
}

//getEventsMetrics returns the Events metrics object for a given process (BinlogReader, Snapshot or Streamer)
func getEventsMetrics(process string, tags map[string]string) Events {
	c := GetGlobal()
	return Events{
		NumWorkers:     ProcessCounterInit(c.factory, "num_"+process+"_workers", tags),
		EventsRead:     CounterInit(c.factory, process+"_events_read", tags),
		EventsWritten:  CounterInit(c.factory, process+"_events_written", tags),
		BytesRead:      CounterInit(c.factory, process+"_bytes_read", tags),
		BytesWritten:   CounterInit(c.factory, process+"_bytes_written", tags),
		BatchSize:      TimerInit(c.factory, process+"_batch_size", tags),
		ReadLatency:    TimerInit(c.factory, process+"_read_latency", tags),
		ProduceLatency: TimerInit(c.factory, process+"_produce_latency", tags),
	}
}

//GetBinlogReaderMetrics initializes and returns a Binlog metrics object
func GetBinlogReaderMetrics(tags map[string]string) *BinlogReader {
	c := GetGlobal()
	return &BinlogReader{
		Events: getEventsMetrics("binlog", tags),

		BinlogRowEventsWritten:   CounterInit(c.factory, "binlog_row_events_written", tags),
		BinlogQueryEventsWritten: CounterInit(c.factory, "binlog_query_events_written", tags),
		BinlogUnhandledEvents:    CounterInit(c.factory, "binlog_unhandled_events", tags),
		TimeToEncounter:          TimerInit(c.factory, "time_to_encounter", tags),
		NumTablesIngesting:       CounterInit(c.factory, "num_tables_ingesting", tags),
	}
}

//GetStreamerMetrics initializes and returns a Streamer metrics object
func GetStreamerMetrics(tags map[string]string) *Streamer {
	c := GetGlobal()
	return &Streamer{
		Events:       getEventsMetrics("streamer", tags),
		TimeInBuffer: TimerInit(c.factory, "time_in_buffer", tags),
	}
}

//GetSnapshotMetrics initializes and returns a Snapshot metrics object
func GetSnapshotMetrics(tags map[string]string) *Snapshot {
	//ls := GetGlobal().LocalScope
	return &Snapshot{
		Events: getEventsMetrics("snapshot", tags),
	}
}

var m *Metrics

//Init initializes global metrics structure
func Init() error {
	m = new(Metrics)
	if err := metricsInit(m); err != nil {
		return err
	}
	m.IdleWorkers = ProcessCounterInit(m.factory, "idle", nil)
	m.NumTablesRegistered = CounterInit(m.factory, "num_tables_registered", nil)
	return nil
}

//GetGlobal return singleton instance of metrics
func GetGlobal() *Metrics {
	return m
}
