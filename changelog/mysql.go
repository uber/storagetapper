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

package changelog

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/pool"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"

	"golang.org/x/net/context" //"context"
)

// SeqnoSaveInterval is the number of messages after which seqno gets persisted in state
const SeqnoSaveInterval = 1000000
const eventsBatchSize = 16

type table struct {
	id           int64
	dead         bool
	producer     pipe.Producer
	rawSchema    string
	schemaGtid   string
	service      string
	encoder      encoder.Encoder
	output       string
	version      int
	outputFormat string
	params       string

	snapshottedAt time.Time
}

type mysqlReader struct {
	gtidSet   *mysql.MysqlGTIDSet
	seqNo     uint64
	masterCI  *db.Addr
	tables    map[string]map[string]map[string][]*table
	numTables int
	bufPipe   pipe.Pipe
	dbl       db.Loc
	inputType string
	ctx       context.Context
	log       log.Logger
	tpool     pool.Thread
	metrics   *metrics.ChangelogReader
	batchSize int
	curGTID   replication.GTIDEvent

	workerID string

	heartbeatTime time.Time
}

func init() {
	registerPlugin("mysql", createMySQLReader)
}

func createMySQLReader(c context.Context, _ *config.AppConfig, bp pipe.Pipe,
	tp pool.Thread) (Reader, error) {

	return &mysqlReader{ctx: c, tpool: tp, bufPipe: bp, inputType: types.InputMySQL, batchSize: eventsBatchSize, heartbeatTime: time.Now()}, nil
}

type queryHandler struct {
	regexp   string
	handler  func(*mysqlReader, *replication.QueryEvent, [][]string) bool
	compiled *regexp.Regexp
}

var queryHandlers = []queryHandler{
	//Handle all four combination of quotes for alter table: `a`.`b`, `a`.b,
	//a.`b`, a.b
	{regexp: "(?mis)^\\s*(?:/\\*[^\\*]*\\*/)?\\s*alter\\s+table\\s+(?:([^`][^\\.]*)\\.)?(\\w+)\\s+(.+)", handler: handleAlterTable},
	{regexp: "(?mis)^\\s*(?:/\\*[^\\*]*\\*/)?\\s*alter\\s+table\\s+(?:`([^`]+)`\\.)?`([^`]+)`\\s+(.+)", handler: handleAlterTable},
	{regexp: "(?mis)^\\s*(?:/\\*[^\\*]*\\*/)?\\s*alter\\s+table\\s+([^`][^\\.]*)\\.`([^`]+)`\\s+(.+)", handler: handleAlterTable},
	{regexp: "(?mis)^\\s*(?:/\\*[^\\*]*\\*/)?\\s*alter\\s+table\\s+`([^`]+)`\\.(\\w+)\\s+(.+)", handler: handleAlterTable},
	//Handle only fully quoted and fully unquoted cases for rename table: `a`.`b`, a.b
	{regexp: "(?mi)(^\\s*(?:/\\*[^\\*]*\\*/)?\\s*rename\\s+table\\s+)|(?:(?:\\s*,\\s*)?(?:([^`][^\\.]*)\\.)?(\\w+)\\s+TO\\s+(?:([^`][^\\.]*)\\.)?(\\w+))", handler: handleRenameTable},
	{regexp: "(?mi)(^\\s*rename\\s+table\\s+)|(?:(?:\\s*,\\s*)?(?:`([^`]+)`\\.)?`([^`]+)`\\s+TO\\s+(?:`([^`]+)`\\.)?`([^`]+)`)", handler: handleRenameTable},
}

var thisInstanceCluster string
var injectAlterFailure = false

/*ThisInstanceCluster returns the cluster name name this instance's binlog
* mysqlReader is working on. This is used by local streamers to identify tables they
* have to stream*/
func ThisInstanceCluster() string {
	return thisInstanceCluster
}

func getTags(cluster string, input string) map[string]string {
	return map[string]string{"cluster": cluster, "input": input}
}

func (b *mysqlReader) binlogFormat() string {
	var rf string
	masterDB, err := db.Open(b.masterCI)
	if log.E(err) {
		return ""
	}
	defer func() { log.EL(b.log, masterDB.Close()) }()
	err = masterDB.QueryRow("SELECT @@global.binlog_format").Scan(&rf)
	if log.E(err) {
		return ""
	}
	if rf == "" {
		log.EL(b.log, fmt.Errorf("invalid (empty) binlog format"))
		return ""
	}
	log.Debugf("Master's binlog format: %s", rf)
	return rf
}

func (b *mysqlReader) pushSchema(tver []*table) bool {
	seqno := b.nextSeqNo()
	if seqno == 0 {
		log.Errorf("Failed to generate next seqno. Current seqno:%+v", b.seqNo)
		return false
	}

	buffered := config.Get().ChangelogBuffer

	for i := 0; i < len(tver); i++ {
		t := tver[i]

		bd, err := t.encoder.EncodeSchema(seqno)
		if log.EL(b.log, err) {
			return false
		}

		if bd == nil {
			continue
		}

		if buffered && encoder.Internal.Type() != t.encoder.Type() {
			bd, err = encoder.WrapEvent(t.outputFormat, "", bd, seqno)
			if log.EL(b.log, err) {
				return false
			}
		}

		err = t.producer.PushSchema("", bd)
		if log.E(err) {
			return false
		}

		log.Debugf("Pushed schema for id=%v, seqno=%v", t.id, seqno)
	}
	return true
}

func (b *mysqlReader) createProducer(tn string, t *state.Row) (pipe.Producer, error) {
	var err error
	pipe2 := b.bufPipe

	format := encoder.Internal.Type()
	if !config.Get().ChangelogBuffer {
		pipe2, err = pipe.CacheGet(t.Output, &t.Params.Pipe, state.GetDB())
		if err != nil {
			b.log.WithFields(log.Fields{"service": t.Service, "db": t.Db, "table": t.Table, "pipe": t.Output}).Errorf("%v", err)
			return nil, err
		}
		format = t.OutputFormat
	}

	p, err := pipe2.NewProducer(tn)
	if err != nil {
		return nil, err
	}

	p.SetFormat(format)

	return p, nil
}

func (b *mysqlReader) addNewTable(t *state.Row) bool {
	b.log.Infof("Adding table to MySQL binlog reader (%v,%v,%v,%v,%v,%v)", t.Service, t.Db, t.Table, t.Output, t.Version, t.OutputFormat)
	enc, err := encoder.Create(t.OutputFormat, t.Service, t.Db, t.Table, t.Input, t.Output, t.Version)
	if log.EL(b.log, err) {
		return false
	}

	if !schema.HasPrimaryKey(enc.Schema()) {
		b.log.Errorf("Table %v doesn't have a primary key. Won't ingest the table", t.Table)
		return true
	}

	pn, err := config.Get().GetChangelogTopicName(t.Service, t.Db, t.Table, t.Input, t.Output, t.Version, t.SnapshottedAt)
	if log.EL(b.log, err) {
		return false
	}

	p, err := b.createProducer(pn, t)
	if log.EL(b.log, err) {
		return false
	}

	nt := &table{t.ID, false, p, t.RawSchema, t.SchemaGtid, t.Service, enc, t.Output, t.Version, t.OutputFormat, t.ParamsRaw, t.SnapshottedAt}

	if b.tables[t.Db][t.Table][t.Service] == nil {
		b.tables[t.Db][t.Table][t.Service] = make([]*table, 0)
	}
	b.tables[t.Db][t.Table][t.Service] = append(b.tables[t.Db][t.Table][t.Service], nt)

	b.log.Infof("New table added to MySQL binlog reader (%v,%v,%v,%v,%v,%v), will produce to: %v", t.Service, t.Db, t.Table, t.Output, t.Version, t.OutputFormat, pn)

	return true
}

func (b *mysqlReader) removeDeletedTables() (count uint) {
	var s string
	for dbn := range b.tables {
		for tbn := range b.tables[dbn] {
			for svc := range b.tables[dbn][tbn] {
				for i := 0; i < len(b.tables[dbn][tbn][svc]); {
					t := b.tables[dbn][tbn][svc][i]
					if t.dead {
						b.log.Infof("Table with id %v removed from binlog reader", t.id)
						err := t.producer.Close()
						log.EL(b.log, err)
						s := b.tables[dbn][tbn][svc]
						s[i] = s[len(s)-1]
						s[len(s)-1] = nil
						b.tables[dbn][tbn][svc] = s[:len(s)-1]
					} else {
						t.dead = true
						count++
						s += fmt.Sprintf("%v,", t.id)
						i++
					}
				}
				if len(b.tables[dbn][tbn][svc]) == 0 {
					delete(b.tables[dbn][tbn], svc)
				}
			}
			if len(b.tables[dbn][tbn]) == 0 {
				delete(b.tables[dbn], tbn)
			}
		}
		if len(b.tables[dbn]) == 0 {
			delete(b.tables, dbn)
		}
	}
	b.log.Debugf("Working on %v tables with ids: %s", count, s)
	return
}

func (b *mysqlReader) closeTableProducers() {
	for dbn, sdb := range b.tables {
		for tbn, svc := range sdb {
			for svcn, tver := range svc {
				for i := 0; i < len(tver); i++ {
					b.log.Debugf("Closing producer for service=%v, table=%v", tver[i].service, tver[i].id)
					log.EL(b.log, tver[i].producer.Close())
					tver[i] = nil
				}
				delete(b.tables[dbn][tbn], svcn)
			}
			delete(b.tables[dbn], tbn)
		}
		delete(b.tables, dbn)
	}
}

func findInVersionArray(a []*table, output string, version int) int {
	j := 0
	for ; j < len(a) && (a[j].version != version || a[j].output != output); j++ {
	}
	return j
}

func (b *mysqlReader) reloadState() bool {
	st, err := state.GetCond("cluster=? AND input='mysql'", b.dbl.Cluster)
	if err != nil {
		b.log.Errorf("Failed to read state, Error: %v", err.Error())
		return false
	}

	b.log.Debugf("reloadState")

	for i := 0; i < len(st); i++ {
		t := &st[i]

		if b.tables[t.Db] == nil {
			b.tables[t.Db] = make(map[string]map[string][]*table)
		}

		if b.seqNo == 0 {
			b.seqNo = t.SeqNo
		}

		if b.tables[t.Db][t.Table] == nil {
			b.tables[t.Db][t.Table] = make(map[string][]*table, 0)
		}

		if b.tables[t.Db][t.Table][t.Service] == nil {
			b.tables[t.Db][t.Table][t.Service] = make([]*table, 0)
		}

		tver := b.tables[t.Db][t.Table][t.Service]

		j := findInVersionArray(tver, t.Output, t.Version)

		if j < len(tver) {
			/* Table was deleted and inserted again. Reinitialize */
			if tver[j].id != t.ID {
				err = tver[j].producer.Close()
				log.EL(b.log, err)
				tver[j] = tver[len(tver)-1]
				tver[len(tver)-1] = nil
				b.tables[t.Db][t.Table][t.Service] = tver[:len(tver)-1]
			} else {
				tver[j].dead = false
				if t.SnapshotTimeChanged(tver[j].snapshottedAt) {
					pn, err := config.Get().GetChangelogTopicName(t.Service, t.Db, t.Table, t.Input, t.Output, t.Version, t.SnapshottedAt)
					if log.EL(b.log, err) {
						return false
					}

					err = tver[j].producer.Close()
					log.EL(b.log, err)

					tver[j].producer, err = b.createProducer(pn, t)
					if log.EL(b.log, err) {
						return false
					}
					tver[j].snapshottedAt = t.SnapshottedAt
				}
			}
		} else if !b.addNewTable(t) {
			return false
		}
	}

	c := b.removeDeletedTables()

	b.metrics.NumTablesIngesting.Set(int64(c))

	if b.bufPipe.Type() == "local" && b.tpool != nil {
		b.tpool.Adjust(c + 1)
	}

	if c == 0 {
		log.Debugf("No tables remaining. Finish binlog reader")
		return false
	}

	b.numTables = int(c)

	return true
}

/* Generates next seqno, seqno is used as a logical time in the produced events */
/* Saves seqno in the state every SeqnoSaveInterval */
func (b *mysqlReader) nextSeqNo() uint64 {
	b.seqNo++
	if b.seqNo%SeqnoSaveInterval == 0 && !b.updateState(false) {
		return 0
	}
	return b.seqNo
}

func (b *mysqlReader) updateState(inc bool) bool {
	log.Debugf("Updating state")

	if !state.RefreshClusterLock(b.dbl.Cluster, b.workerID) {
		return false
	}

	if !b.reloadState() {
		return false
	}

	/* Skip all seqNo possibly used before restart.*/
	if inc {
		b.seqNo += SeqnoSaveInterval
	}

	if log.E(state.SaveBinlogState(b.dbl.Cluster, util.SortedGTIDString(b.gtidSet), b.seqNo)) {
		return false
	}

	if !db.IsValidConn(&b.dbl, db.Slave, b.masterCI, b.inputType) {
		return false
	}

	/*
		//Push schema down the stream for all just added tables
		for _, d := range b.tables {
			for _, t := range d {
				if t.justAdded {
					log.Debugf("Pushing schema for table: %v", t.id)
					if !b.pushSchema(t) {
						return false
					}
					log.Debugf("Pushed schema for table: %v", t.id)
					t.justAdded = false
				}
			}
		}
	*/

	state.EmitRegisteredTablesCount()
	b.metrics.NumWorkers.Emit()

	log.Debugf("Updating state finished")

	return true
}

func gtidToString(v *replication.GTIDEvent) string {
	u, err := uuid.FromBytes(v.SID)
	s := "error"
	if !log.E(err) {
		s = u.String()
	}

	return fmt.Sprintf("%v:%v", s, v.GNO)
}

func (b *mysqlReader) produceRow(tp int, t *table, ts time.Time, row *[]interface{}) error {
	var err error
	buffered := config.Get().ChangelogBuffer
	seqno := b.nextSeqNo()
	if seqno == 0 {
		return fmt.Errorf("failed to generate next seqno. Current seqno: %+v", b.seqNo)
	}
	key := encoder.GetRowKey(t.encoder.Schema(), row)
	if buffered && b.bufPipe.Type() == "local" {
		err = t.producer.PushBatch(key, &types.RowMessage{Type: tp, Key: key, Data: row, SeqNo: seqno, Timestamp: ts})
	} else {
		var bd []byte
		bd, err = t.encoder.Row(tp, row, seqno, ts)
		if err != nil {
			var masterGTID string
			if strings.Contains(err.Error(), "column count mismatch") {
				var err1 error
				masterGTID, err1 = db.GetCurrentGTID(b.masterCI)
				log.E(err1)
			}
			b.log.WithFields(log.Fields{"state gtid": util.SortedGTIDString(b.gtidSet), "event gtid": gtidToString(&b.curGTID), "master gtid": masterGTID, "table": t.id}).Errorf(err.Error())
			return err
		}
		if !buffered {
			key = t.producer.PartitionKey("log", key)
		} else if t.encoder.Type() != encoder.Internal.Type() {
			bd, err = encoder.WrapEvent(t.outputFormat, key, bd, seqno)
			if log.EL(b.log, err) {
				return err
			}
		}
		err = t.producer.PushBatch(key, bd)
		b.metrics.BytesWritten.Inc(int64(len(bd)))
	}
	//log.Debugf("Pushed to buffer. seqno=%v, table=%v", seqno, t.id)
	if shutdown.Initiated() {
		return nil
	}
	if err != nil {
		b.log.Errorf("Type: %v, Error: %v", tp, err.Error())
		return err
	}
	b.metrics.ChangelogRowEventsWritten.Inc(1)
	return nil
}

func (b *mysqlReader) handleRowsEventLow(ev *replication.BinlogEvent, t *table) bool {
	var err error

	re := ev.Event.(*replication.RowsEvent)
	ts := time.Unix(int64(ev.Header.Timestamp), 0)

	/*
		bb := new(bytes.Buffer)
		ev.Dump(bb)
		fmt.Fprintf(os.Stderr, "Handle rows event %+v", bb.String())
	*/

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		//TODO: Produce as a batch
		for i := 0; i < len(re.Rows) && err == nil; i++ {
			err = b.produceRow(types.Insert, t, ts, &re.Rows[i])
		}
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for i := 0; i < len(re.Rows) && err == nil; i++ {
			err = b.produceRow(types.Delete, t, ts, &re.Rows[i])
		}
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		for i := 0; i < len(re.Rows) && err == nil; i += 2 {
			if !strings.HasSuffix(t.outputFormat, "_idempotent") {
				err = b.produceRow(types.Delete, t, ts, &re.Rows[i])
			}
			if err == nil {
				err = b.produceRow(types.Insert, t, ts, &re.Rows[i+1])
			}
		}
	default:
		err = fmt.Errorf("not supported event type %v", ev.Header.EventType)
	}

	return !log.E(err)
}

func (b *mysqlReader) handleRowsEvent(ev *replication.BinlogEvent, dbn string, tbn string) bool {
	if b.tables[dbn] == nil || b.tables[dbn][tbn] == nil {
		return true
	}

	//Produce event to all outputs and versions of the table
	for _, svc := range b.tables[dbn][tbn] {
		if svc == nil {
			continue
		}
		for ver := range svc {
			if !b.handleRowsEventLow(ev, svc[ver]) {
				return false
			}
		}
	}

	return true
}

func handleAlterTable(b *mysqlReader, qe *replication.QueryEvent, m [][]string) bool {
	//Make sure that we have up to date state before deciding whether this
	// alter table is for being ingested table or not
	if !b.updateState(false) {
		return false
	}
	dbname := m[0][1]
	table := m[0][2]
	if dbname == "" {
		dbname = util.BytesToString(qe.Schema)
	}
	d := b.tables[dbname]
	if d != nil && d[table] != nil {
		svc := d[table]
		b.log.WithFields(log.Fields{"db": dbname, "table": table, "alter": m[0][3]}).Debugf("detected alter statement of being ingested table")
		b.metrics.ChangelogAlterTableEvents.Inc(1)

		if strings.Contains(strings.ToLower(m[0][3]), " foreign key ") {
			log.Debugf("Skipping foreign key alter")
			return true
		}

		if injectAlterFailure {
			log.Errorf("Exited due to failure injection")
			return false
		}

		for _, tver := range svc {
			for i := range tver {
				t := tver[i]
				newGtid := util.SortedGTIDString(b.gtidSet)
				if !schema.MutateTable(state.GetNoDB(), t.service, dbname, table, m[0][3], t.encoder.Schema(), &t.rawSchema) ||
					!state.ReplaceSchema(t.service, b.dbl.Cluster, t.encoder.Schema(), t.rawSchema, t.schemaGtid, newGtid, b.inputType, t.output, t.version, t.outputFormat, t.params) {
					b.log.WithFields(log.Fields{"db": dbname, "table": table, "alter": m[0][3]}).Warnf("error executing alter table")
					return false
				}

				t.schemaGtid = newGtid

				err := t.encoder.UpdateCodec()
				if log.EL(b.log, err) {
					return false
				}
				log.Debugf("Updated codec. id=%v", t.id)
			}

			if !b.pushSchema(tver) {
				return false
			}

			b.metrics.ChangelogQueryEventsWritten.Inc(int64(len(tver)))
		}

		return true
	}
	b.log.WithFields(log.Fields{"query": util.BytesToString(qe.Query), "db": util.BytesToString(qe.Schema)}).Debugf("Unhandled query (alter). cluster: " + b.dbl.Cluster)
	return true
}

func handleRenameTable(b *mysqlReader, qe *replication.QueryEvent, m [][]string) bool {
	s := util.BytesToString(qe.Query)
	renameRE := regexp.MustCompile(`(?i)(^\s*(?:/\*[^\*]*\*/)?\s*rename\s+table\s+)`)
	r := renameRE.FindAllStringSubmatch(s, -1)
	if len(r) == 0 || len(m) < 2 {
		b.log.WithFields(log.Fields{"query": util.BytesToString(qe.Query), "db": util.BytesToString(qe.Schema)}).Debugf("Unhandled query (rename). cluster: " + b.dbl.Cluster)
		return true
	}
	for _, t := range m {
		dbname := t[4]
		table := t[5]
		if dbname == "" {
			dbname = util.BytesToString(qe.Schema)
		}
		d := b.tables[dbname]
		if d != nil && len(d[table]) > 0 {
			svc := d[table]
			b.log.WithFields(log.Fields{"db": dbname, "table": table, "rename": t[0]}).Debugf("detected rename statement of being ingested table")
			b.metrics.ChangelogAlterTableEvents.Inc(1)

			for _, tver := range svc {
				newGtid := util.SortedGTIDString(b.gtidSet)
				ts, rs := state.PullCurrentSchema(&db.Loc{Service: tver[0].service, Cluster: b.dbl.Cluster, Name: dbname}, table, types.InputMySQL)
				if ts == nil {
					return false
				}

				for i := range tver {
					t := tver[i]
					t.rawSchema = rs
					ets := t.encoder.Schema()
					ets.DBName = ts.DBName
					ets.TableName = ts.TableName
					ets.Columns = ts.Columns
					if !state.ReplaceSchema(t.service, b.dbl.Cluster, t.encoder.Schema(), t.rawSchema, t.schemaGtid, newGtid, b.inputType, t.output, t.version, t.outputFormat, t.params) {
						return false
					}

					t.schemaGtid = newGtid

					err := t.encoder.UpdateCodec()
					if log.EL(b.log, err) {
						return false
					}
					log.Debugf("Updated codec. id=%v", t.id)
				}

				if !b.pushSchema(tver) {
					return false
				}

				b.metrics.ChangelogQueryEventsWritten.Inc(int64(len(tver)))
			}

			return true
		}
	}
	return true
}

func (b *mysqlReader) handleQueryEvent(ev *replication.BinlogEvent) bool {
	qe := ev.Event.(*replication.QueryEvent)

	s := util.BytesToString(qe.Query)
	if s == "BEGIN" || s == "COMMIT" {
		return true
	}

	if strings.HasPrefix(s, "UPDATE `heartbeat`.`heartbeat`") || strings.HasPrefix(s, "FLUSH ") {
		return true
	}

	b.log.Debugf("handleQueryEvent %+v", s)

	matched := false
	for _, v := range queryHandlers {
		m := v.compiled.FindAllStringSubmatch(s, -1)
		if len(m) > 0 {
			matched = true
			b.log.Debugf("Match result: %q", m)

			if !v.handler(b, qe, m) {
				return false
			}
		}
	}

	if !matched {
		b.log.WithFields(log.Fields{"query": s, "db": util.BytesToString(qe.Schema)}).Debugf("Unhandled query. cluster: " + b.dbl.Cluster)
	}
	return true
}

func (b *mysqlReader) incGTID(v *replication.GTIDEvent) bool {
	if b.curGTID.SID == nil {
		b.curGTID = *v
		return true
	}

	u, err := uuid.FromBytes(b.curGTID.SID)
	if log.E(err) {
		return false
	}

	if s, ok := b.gtidSet.Sets[u.String()]; ok {
		l := &s.Intervals[len(s.Intervals)-1]
		if l.Stop == b.curGTID.GNO {
			l.Stop++
			b.curGTID = *v
			return true
		}
		b.log.Infof("non-sequential gtid GNO: %+v %+v", l.Stop, b.curGTID.GNO)
	}

	gtid := fmt.Sprintf("%s:%d", u.String(), b.curGTID.GNO)
	us, err := mysql.ParseUUIDSet(gtid)
	if log.E(err) {
		return false
	}

	b.gtidSet.AddSet(us)

	b.log.WithFields(log.Fields{"gtid": gtid, "out_gtid_set": util.SortedGTIDString(b.gtidSet)}).Debugf("non-sequential gtid event")

	b.curGTID = *v

	return true
}

func (b *mysqlReader) handleEvent(ev *replication.BinlogEvent) bool {
	if ev.Header.Timestamp != 0 {
		b.metrics.TimeToEncounter.Record(time.Duration(time.Now().Unix()-int64(ev.Header.Timestamp)) * time.Second)
	}
	switch v := ev.Event.(type) {
	case *replication.FormatDescriptionEvent:
		b.log.Infof("ServerVersion: %+v, BinlogFormatVersion: %+v, ChecksumAlgorithm: %+v", util.BytesToString(v.ServerVersion), v.Version, v.ChecksumAlgorithm)
	case *replication.RowsEvent:
		if !b.handleRowsEvent(ev, util.BytesToString(v.Table.Schema), util.BytesToString(v.Table.Table)) {
			return false
		}
	case *replication.QueryEvent:
		if !b.handleQueryEvent(ev) {
			return false
		}
	case *replication.GTIDEvent:
		if !b.incGTID(v) {
			return false
		}
	case *replication.TableMapEvent:
		//It's already in RowsEvent, not need to handle separately
	case *replication.XIDEvent:
		//ignoring
	default:
		if ev.Header.EventType != replication.HEARTBEAT_EVENT {
			b.metrics.ChangelogUnhandledEvents.Inc(1)
			bb := new(bytes.Buffer)
			ev.Dump(bb)
			b.log.Debugf("Unhandled binlog event: %+v", bb.String())
		}
	}

	return true
}

type result struct {
	ev  *replication.BinlogEvent
	err error
}

func (b *mysqlReader) commitBatch() bool {
	//TODO: Commit only tables which had data in this batch
	w := b.metrics.ProduceLatency
	w.Start()
	for _, sdb := range b.tables {
		for _, svc := range sdb {
			for _, tver := range svc {
				for i := 0; i < len(tver); i++ {
					err := tver[i].producer.PushBatchCommit()
					if log.EL(b.log, err) {
						w.Stop()
						return false
					}
				}
			}
		}
	}
	w.Stop()

	return true
}

func (b *mysqlReader) processBatch(msg *result, msgCh chan *result) bool {
	var i int
L:
	for {
		if msg.err != nil {
			merr, ok := msg.err.(*mysql.MyError)
			if ok && merr.Code == 1236 {
				serverGtid, err := db.GetCurrentGTID(b.masterCI)
				if err != nil {
					b.log.Errorf("Error fetchching gtid from server: %v", err)
				}
				purgedGtid, err := db.GetPurgedGTID(b.masterCI)
				if err != nil {
					b.log.Errorf("Error fetchching purged gtid from server: %v", err)
				}
				b.log.WithFields(log.Fields{"my_gtid_set": strings.Replace(util.SortedGTIDString(b.gtidSet), ",", ",\n", -1), "server_gtid_set": serverGtid, "purged_gtid": purgedGtid, "host": b.masterCI.Host, "port": b.masterCI.Port, "user": b.masterCI.User}).Errorf("Error connecting to master: %v", merr)
				return false
			} else if msg.err.Error() != "context canceled" {
				b.log.Errorf("BinlogReadEvents: %v", msg.err.Error())
				return false
			}
			break L //Shutting down, let it commit current  batch
		}

		if !b.handleEvent(msg.ev) {
			return false
		}

		i++
		//TODO: Unify with streamer logic. Extract MaxBatchSize from per table
		//pipe config
		if i >= b.batchSize*b.numTables {
			break
		}

		select {
		case msg = <-msgCh:
		default:
			break L //No messages for now, break the loop and commit whatever we pushed to the batch already
		}
	}

	b.heartbeatTime = time.Now()

	b.metrics.EventsRead.Inc(int64(i))
	b.metrics.BatchSize.Record(time.Duration(i) * time.Millisecond)

	return b.commitBatch()
}

//eventFetcher is blocking call to buffered channel converter
func (b *mysqlReader) eventFetcher(ctx context.Context, s *replication.BinlogStreamer, wg *sync.WaitGroup, msgCh chan *result, exitCh chan bool) {
	defer wg.Done()
L:
	for {
		msg := &result{}
		msg.ev, msg.err = s.GetEvent(ctx)
		select {
		case msgCh <- msg:
			if msg.err != nil && msg.err.Error() != "context canceled" {
				break L
			}
		case <-exitCh:
			break L
		}
	}
	b.log.Debugf("Finished MySQL binlog reader helper goroutine")
}

func (b *mysqlReader) fetchFirstEvent(stateUpdateTicker *time.Ticker, watchdogTicker *time.Ticker, msgCh chan *result) (bool, *result) {
	defer b.metrics.ReadLatency.Start().Stop()
	select {
	case <-watchdogTicker.C:
		if b.heartbeatTime.Add(config.Get().ChangelogWatchdogInterval).After(time.Now()) {
			return true, nil
		}
		b.log.Errorf("Watchdog expired")
	case <-stateUpdateTicker.C:
		if b.updateState(false) {
			return true, nil
		}
	case msg, ok := <-msgCh:
		return ok, msg
	case <-shutdown.InitiatedCh():
		b.incGTID(&replication.GTIDEvent{})
		g := util.SortedGTIDString(b.gtidSet)
		if !log.EL(b.log, state.SaveBinlogState(b.dbl.Cluster, g, b.seqNo)) {
			b.log.WithFields(log.Fields{"gtid": g, "SeqNo": b.seqNo}).Infof("Binlog state saved")
			return false, nil
		}
	}
	b.metrics.Errors.Inc(1)
	return false, nil
}

func (b *mysqlReader) readEvents(c *db.Addr, stateUpdateInterval time.Duration) {
	id := rand.Uint32()
	for id == 0 {
		id = rand.Uint32()
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID:  id,
		Flavor:    "mysql",
		Host:      c.Host,
		Port:      c.Port,
		User:      c.User,
		Password:  c.Pwd,
		ParseTime: true,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	streamer, err := syncer.StartSyncGTID(b.gtidSet.Clone())
	if log.EL(b.log, err) {
		b.metrics.Errors.Inc(1)
		return
	}
	defer syncer.Close()

	stateUpdateTicker := time.NewTicker(stateUpdateInterval)
	defer stateUpdateTicker.Stop()
	watchdogTicker := time.NewTicker(config.Get().ChangelogWatchdogInterval)
	defer watchdogTicker.Stop()

	defer b.closeTableProducers()
	if !b.updateState(true) {
		b.metrics.Errors.Inc(1)
		return
	}

	b.log.WithFields(log.Fields{"gtid": util.SortedGTIDString(b.gtidSet), "SeqNo": b.seqNo}).Infof("Binlog start")

	msgCh, exitCh := make(chan *result, b.batchSize), make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	defer func() { cancel(); close(exitCh); wg.Wait() }()

	/*This goroutine is to multiplex blocking streamer.GetEvent and tickCh*/
	go b.eventFetcher(ctx, streamer, &wg, msgCh, exitCh)

	for {
		more, msg := b.fetchFirstEvent(stateUpdateTicker, watchdogTicker, msgCh)
		if !more {
			break
		}
		if msg != nil && !b.processBatch(msg, msgCh) {
			b.metrics.Errors.Inc(1)
			break
		}
	}

	b.log.Debugf("Finishing MySQL binlog reader")
}

func (b *mysqlReader) start(cfg *config.AppConfig) bool {
	var err error

	h, _ := os.Hostname()
	w := log.GenWorkerID()
	b.workerID = h + "." + w

	b.dbl.Service, b.dbl.Cluster, b.dbl.Name, err = state.GetClusterTask(types.InputMySQL, b.workerID, cfg.LockExpireTimeout)
	if err != nil || b.dbl.Service == "" {
		return false
	}

	b.metrics = metrics.NewChangelogReaderMetrics(getTags(b.dbl.Cluster, b.inputType))

	b.metrics.NumWorkers.Inc()
	defer func() {
		if err != nil {
			b.metrics.Errors.Inc(1)
		}
		b.metrics.NumWorkers.Dec()
	}()

	thisInstanceCluster = b.dbl.Cluster

	b.log = log.WithFields(log.Fields{"worker_id": w, "cluster": b.dbl.Cluster})

	b.log.Infof("Starting MySQL binlog reader")

	// Get slave's connection info. Connecting to slave guarantees that we will connect to DC local slave
	b.masterCI, err = db.GetConnInfo(&b.dbl, db.Slave, b.inputType)
	if err != nil {
		return true
	}

	rf := b.binlogFormat()
	if rf != "ROW" {
		if rf != "" {
			b.log.Errorf("binlog format is %s. row binlog format required. skipping", rf)
		}
		return true
	}

	b.tables = make(map[string]map[string]map[string][]*table)

	for i := 0; i < len(queryHandlers); i++ {
		queryHandlers[i].compiled = regexp.MustCompile(queryHandlers[i].regexp)
	}

	// Start reading binlogs from the gtid set saved in the state
	var gtid string
	gtid, _, err = state.GetGTID(b.dbl.Cluster)
	if log.EL(b.log, err) {
		return true
	}

	/* If no gtid in the state get current gtid set from master */
	if gtid == "" {
		gtid, err = db.GetCurrentGTID(b.masterCI)
		if err != nil {
			return true
		}
		err = state.SetGTID(b.dbl.Cluster, gtid)
		if err != nil {
			b.log.Errorf("Error saving gtid. gtid %v. Error: %v", gtid, err.Error())
		}
	}

	var s mysql.GTIDSet
	s, err = mysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		b.log.Errorf("Invalid gtid: '%v' Error: %v", gtid, err.Error())
		return true
	}
	b.gtidSet = s.(*mysql.MysqlGTIDSet)

	b.readEvents(b.masterCI, cfg.StateUpdateInterval)

	b.log.Infof("MySQL Binlog reader finished")

	return true
}

func (b *mysqlReader) Worker() bool {
	return b.start(config.Get())
}
