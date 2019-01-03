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
}

type timer interface {
	Start()
	Stop()
	Record(time.Duration)
}

type scope interface {
	InitCounter(name string) counter
	InitTimer(name string) timer
	SubScope(name string) scope
	Tagged(tags map[string]string) scope
}

type metricsConstructor func() (scope, error)

var metricsInit metricsConstructor = noopMetricsInit
var _ metricsConstructor = tallyMetricsInit

//IdleWorkers count idle workers
var IdleWorkers *ProcessCounter

//TODO: ERRORS

//State contains metrics related to state
type State struct {
	NumTablesRegistered *Counter

	Sync         *ProcessCounter
	SyncDuration *Timer

	TableReg         *ProcessCounter
	TableRegDuration *Timer

	TableDereg         *ProcessCounter
	TableDeregDuration *Timer

	SyncErrors *Counter
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
	Errors      *Counter
	Duration    *Timer
	SizeRead    *Counter
	SizeWritten *Counter
	ThrottledUs *Counter
}

//Streamer contains metrics related to event streamer
type Streamer struct {
	Events
	TimeInBuffer *Timer
	Errors       *Counter
	LockLost     *Counter
}

//ChangelogReader contains metrics related to changelog reader
type ChangelogReader struct {
	Events

	ChangelogRowEventsWritten   *Counter
	ChangelogAlterTableEvents   *Counter
	ChangelogQueryEventsWritten *Counter
	ChangelogUnhandledEvents    *Counter

	TimeToEncounter *Timer

	NumTablesIngesting *Counter

	Errors *Counter

	LockLost *Counter
}

//Validation contains metrics related to validation
type Validation struct {
	NumValidations           *ProcessCounter
	ValidationRowsProcessed  *Counter
	ValidatedInsertEvents    *Counter
	ValidatedDeleteEvents    *Counter
	ValidatedSchemaEvents    *Counter
	ValidationFailed         *Counter
	ValidationSuccess        *Counter
	ValidationSuccessBatch   *Counter
	ValidationSchemaErrors   *Counter
	ValidationEventTypeError *Counter
	SnapshotBase             *Snapshot
	SnapshotRef              *Snapshot
}

//FilePipeMetrics ...
type FilePipeMetrics struct {
	BytesWritten *Counter
	BytesRead    *Counter
	FilesCreated *Counter
	FilesOpened  *Counter
	FilesClosed  *Counter // == FilesCreated
}

//getEventsMetrics returns the Events metrics object for a given process (ChangelogReader, Snapshot or Streamer)
func getEventsMetrics(s scope, process string) Events {
	return Events{
		NumWorkers:     ProcessCounterInit(s, "num_"+process+"_workers"),
		EventsRead:     CounterInit(s, process+"_events_read"),
		EventsWritten:  CounterInit(s, process+"_events_written"),
		BytesRead:      CounterInit(s, process+"_bytes_read"),
		BytesWritten:   CounterInit(s, process+"_bytes_written"),
		BatchSize:      TimerInit(s, process+"_batch_size"),
		ReadLatency:    TimerInit(s, process+"_read_latency"),
		ProduceLatency: TimerInit(s, process+"_produce_latency"),
	}
}

//NewStateMetrics initializes and returns a State metrics object
func NewStateMetrics() *State {
	s := getGlobal()
	return &State{
		NumTablesRegistered: CounterInit(s, "num_tables_registered"),

		Sync:         ProcessCounterInit(s, "state_sync"),
		SyncDuration: TimerInit(s, "state_sync_duration"),

		TableReg:         ProcessCounterInit(s, "state_sync_table_reg"),
		TableRegDuration: TimerInit(s, "state_sync_table_reg_duration"),

		TableDereg:         ProcessCounterInit(s, "state_sync_table_dereg"),
		TableDeregDuration: TimerInit(s, "state_sync_table_dereg_duration"),

		SyncErrors: CounterInit(s, "state_sync_errors"),
	}
}

//NewChangelogReaderMetrics initializes and returns a Changelog metrics object
func NewChangelogReaderMetrics(tags map[string]string) *ChangelogReader {
	s := getGlobal().Tagged(tags)
	return &ChangelogReader{
		Events: getEventsMetrics(s, "changelog"),

		ChangelogRowEventsWritten:   CounterInit(s, "changelog_row_events_written"),
		ChangelogQueryEventsWritten: CounterInit(s, "changelog_query_events_written"),
		ChangelogUnhandledEvents:    CounterInit(s, "changelog_unhandled_events"),
		TimeToEncounter:             TimerInit(s, "time_to_encounter"),
		NumTablesIngesting:          CounterInit(s, "num_tables_ingesting"),
		Errors:                      CounterInit(s, "changelog_error"),
		ChangelogAlterTableEvents:   CounterInit(s, "changelog_alter_table_events"),
		LockLost:                    CounterInit(s, "changelog_lock_lost"),
	}
}

//NewStreamerMetrics initializes and returns a Streamer metrics object
func NewStreamerMetrics(tags map[string]string) *Streamer {
	s := getGlobal().Tagged(tags)
	return &Streamer{
		Events:       getEventsMetrics(s, "streamer"),
		TimeInBuffer: TimerInit(s, "time_in_buffer"),
		Errors:       CounterInit(s, "streamer_error"),
		LockLost:     CounterInit(s, "streamer_lock_lost"),
	}
}

//NewSnapshotMetrics initializes and returns a Snapshot metrics object
func NewSnapshotMetrics(prefix string, tags map[string]string) *Snapshot {
	s := getGlobal().Tagged(tags)
	return &Snapshot{
		Events:      getEventsMetrics(s, prefix+"snapshot"),
		Errors:      CounterInit(s, prefix+"snapshot_error"),
		Duration:    TimerInit(s, prefix+"snapshot_duration"),
		SizeRead:    CounterInit(s, prefix+"snapshot_size_read"),
		SizeWritten: CounterInit(s, prefix+"snapshot_size_written"),
		ThrottledUs: CounterInit(s, prefix+"snapshot_throttled_us"),
	}
}

//NewValidationMetrics initializes and returns a Validation metrics object
func NewValidationMetrics(tags map[string]string) *Validation {
	s := getGlobal().Tagged(tags)
	return &Validation{
		NumValidations:           ProcessCounterInit(s, "num_validations"),
		ValidationRowsProcessed:  CounterInit(s, "validation_rows_processed"),
		ValidatedInsertEvents:    CounterInit(s, "validated_insert_events"),
		ValidatedDeleteEvents:    CounterInit(s, "validated_delete_events"),
		ValidatedSchemaEvents:    CounterInit(s, "validated_schema_events"),
		ValidationFailed:         CounterInit(s, "validation_failed"),
		ValidationSuccess:        CounterInit(s, "validation_success"),
		ValidationSuccessBatch:   CounterInit(s, "validation_success_batch"),
		ValidationEventTypeError: CounterInit(s, "validation_event_type_errors"),
		SnapshotBase:             NewSnapshotMetrics("validation_", tags),
		SnapshotRef:              NewSnapshotMetrics("validation_", tags),
	}
}

//NewFilePipeMetrics initializes and returns a FilePipeMetrics object
func NewFilePipeMetrics(prefix string, tags map[string]string) *FilePipeMetrics {
	s := getGlobal().Tagged(tags)
	return &FilePipeMetrics{
		BytesWritten: CounterInit(s, prefix+"_bytes_written"),
		BytesRead:    CounterInit(s, prefix+"_bytes_read"),
		FilesOpened:  CounterInit(s, prefix+"_files_opened"),
		FilesClosed:  CounterInit(s, prefix+"_files_closed"),
	}
}

var m scope

//Init initializes global metrics structure
func Init() error {
	var err error
	if m, err = metricsInit(); err != nil {
		return err
	}
	IdleWorkers = ProcessCounterInit(m, "idle")
	return nil
}

//getGlobal return global metrics scope
func getGlobal() scope {
	return m
}
