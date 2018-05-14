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
	"golang.org/x/net/context" //"context"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/db"
	"github.com/uber/storagetapper/encoder"
	"github.com/uber/storagetapper/lock"
	"github.com/uber/storagetapper/log"
	"github.com/uber/storagetapper/metrics"
	"github.com/uber/storagetapper/pipe"
	"github.com/uber/storagetapper/pool"
	"github.com/uber/storagetapper/schema"
	"github.com/uber/storagetapper/shutdown"
	"github.com/uber/storagetapper/state"
	"github.com/uber/storagetapper/types"
	"github.com/uber/storagetapper/util"
)

const seqnoSaveInterval = 1000000

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
}

type mysqlReader struct {
	gtidSet       *mysql.MysqlGTIDSet
	seqNo         uint64
	masterCI      *db.Addr
	tables        map[string]map[string][]*table
	numTables     int
	bufPipe       pipe.Pipe
	outPipes      *map[string]pipe.Pipe
	dbl           db.Loc
	ctx           context.Context
	log           log.Logger
	alterRE       *regexp.Regexp
	alterQuotedRE *regexp.Regexp
	tpool         pool.Thread
	metrics       *metrics.BinlogReader
	batchSize     int
	lock          lock.Lock
}

func init() {
	registerPlugin("mysql", createMySQLReader)
}

func createMySQLReader(c context.Context, cfg *config.AppConfig, bp pipe.Pipe, op *map[string]pipe.Pipe, tp pool.Thread) (Reader, error) {
	return &mysqlReader{ctx: c, tpool: tp, bufPipe: bp, outPipes: op}, nil
}

var thisInstanceCluster string

/*ThisInstanceCluster returns the cluster name name this instance's binlog
* mysqlReader is working on. This is used by local streamers identify tables they
* have to stream*/
func ThisInstanceCluster() string {
	return thisInstanceCluster
}

// GetClusterTag return the cluster tag
func getClusterTag(cName string) map[string]string {
	return map[string]string{"cluster": cName}
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
	log.Debugf("Master's binlog format: %s", rf)
	return rf
}

func (b *mysqlReader) pushSchema(tver []*table) bool {
	seqno := b.nextSeqNo()
	if seqno == 0 {
		log.Errorf("Failed to generate next seqno. Current seqno:%+v", b.seqNo)
		return false
	}

	for i := 0; i < len(tver); i++ {
		t := tver[i]
		log.Debugf("format vvvvv %v %v %v", t.outputFormat, t.encoder.Type(), encoder.Internal.Type())
		if t.outputFormat != "json" && t.outputFormat != "msgpack" {
			continue
		}

		bd, err := t.encoder.EncodeSchema(seqno)
		if log.EL(b.log, err) {
			return false
		}

		if bd == nil {
			return true
		}

		err = t.producer.PushSchema("", bd)
		if log.E(err) {
			return false
		}

		log.Debugf("Pushed schema for id=%v, seqno=%v", t.id, seqno)
	}
	return true
}

func (b *mysqlReader) addNewTable(st state.Type, i int) bool {
	t := st[i]

	b.log.Infof("Adding table to MySQL binlog reader (%v,%v,%v,%v,%v), will produce to: %v", t.Service, t.Db, t.Table, t.Output, t.Version, t.OutputFormat)
	enc, err := encoder.Create(t.OutputFormat, t.Service, t.Db, t.Table)
	if log.EL(b.log, err) {
		return false
	}

	if !schema.HasPrimaryKey(enc.Schema()) {
		b.log.Errorf("Table %v doesn't have a primary key. Won't ingest the table", t.Table)
		return true
	}

	pn, err := config.Get().GetChangelogTopicName(t.Service, t.Db, t.Table, t.Input, t.Output, t.Version)
	if log.EL(b.log, err) {
		return false
	}
	pipe := b.bufPipe

	format := encoder.Internal.Type()
	if !config.Get().ChangelogBuffer {
		var ok bool
		pipe, ok = (*b.outPipes)[t.Output]
		if !ok {
			b.log.Errorf("Service: %v Db: %v Table: %v. Unknown pipe: %v", t.Service, t.Db, t.Table, t.Output)
			return false
		}
		format = t.OutputFormat
	}

	p, err := pipe.NewProducer(pn)
	if err != nil {
		return false
	}

	p.SetFormat(format)

	b.log.Infof("New table added to MySQL binlog reader (%v,%v,%v,%v,%v,%v), will produce to: %v", t.Service, t.Db, t.Table, t.Output, t.Version, t.OutputFormat, pn)

	nt := &table{t.ID, false, p, t.RawSchema, t.SchemaGtid, t.Service, enc, t.Output, t.Version, t.OutputFormat}

	if b.tables[t.Db][t.Table] == nil {
		b.tables[t.Db][t.Table] = make([]*table, 0)
	}
	b.tables[t.Db][t.Table] = append(b.tables[t.Db][t.Table], nt)

	return true
}

func (b *mysqlReader) removeDeletedTables() (count uint) {
	for m := range b.tables {
		for n := range b.tables[m] {
			for i := 0; i < len(b.tables[m][n]); {
				t := b.tables[m][n][i]
				if t.dead {
					b.log.Infof("Table with id %v removed from binlog reader", t.id)
					err := t.producer.Close()
					log.EL(b.log, err)
					s := b.tables[m][n]
					s[i] = s[len(s)-1]
					s[len(s)-1] = nil
					b.tables[m][n] = s[:len(s)-1]
					if len(b.tables[m][n]) == 0 {
						delete(b.tables[m], n)
					}
				} else {
					t.dead = true
					count++
					log.Debugf("%v", t.id)
					i++
				}
			}
		}
		if len(b.tables[m]) == 0 {
			delete(b.tables, m)
		}
	}
	return
}

func (b *mysqlReader) closeTableProducers() {
	for m, sdb := range b.tables {
		for n, tver := range sdb {
			for i := 0; i < len(tver); i++ {
				b.log.Debugf("Closing producer for service=%v, table=%v", tver[i].service, tver[i].id)
				log.EL(b.log, tver[i].producer.Close())
				tver[i] = nil
			}
			delete(b.tables[m], n)
		}
		delete(b.tables, m)
	}
}

func (b *mysqlReader) reloadState() bool {
	st, err := state.GetCond("cluster=? AND input='mysql'", b.dbl.Cluster)
	if err != nil {
		b.log.Errorf("Failed to read state, Error: %v", err.Error())
		return false
	}

	b.log.Debugf("reloadState")

	for i, t := range st {
		if b.tables[t.Db] == nil {
			b.tables[t.Db] = make(map[string][]*table)
		}

		if b.seqNo == 0 {
			b.seqNo = t.SeqNo
		}

		if b.tables[t.Db][t.Table] == nil {
			b.tables[t.Db][t.Table] = make([]*table, 0)
		}

		tver := b.tables[t.Db][t.Table]

		var j int
		for ; j < len(tver) && (tver[j].version != t.Version || tver[j].output != t.Output); j++ {
		}

		if j < len(tver) {
			/* Table was deleted and inserted again. Reinitialize */
			if tver[j].id != t.ID {
				err = tver[j].producer.Close()
				log.EL(b.log, err)
				tver[j] = tver[len(tver)-1]
				tver[len(tver)-1] = nil
				b.tables[t.Db][t.Table] = tver[:len(tver)-1]
			} else {
				tver[j].dead = false
			}
		} else if !b.addNewTable(st, i) {
			return false
		}
	}

	c := b.removeDeletedTables()

	b.metrics.NumTablesIngesting.Set(int64(c))

	if b.bufPipe.Type() == "local" {
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
/* Saves seqno in the state every seqnoSaveInterval */
func (b *mysqlReader) nextSeqNo() uint64 {
	b.seqNo++
	if b.seqNo%seqnoSaveInterval == 0 && !b.updateState(false) {
		return 0
	}
	return b.seqNo
}

func (b *mysqlReader) updateState(inc bool) bool {
	log.Debugf("Updating state")

	if !b.lock.Refresh() {
		return false
	}

	if !b.reloadState() {
		return false
	}

	/* Skip all seqNo possibly used before restart.*/
	if inc {
		b.seqNo += seqnoSaveInterval
	}

	if log.E(state.SaveBinlogState(&b.dbl, b.gtidSet.String(), b.seqNo)) {
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

	metrics.GetGlobal().NumTablesRegistered.Emit()
	b.metrics.NumWorkers.Emit()

	log.Debugf("Updating state finished")

	return true
}

func (b *mysqlReader) wrapEvent(outputFormat string, key string, bd []byte, seqno uint64) ([]byte, error) {
	akey := make([]interface{}, 1)
	akey[0] = key

	cfw := types.CommonFormatEvent{
		Type:      outputFormat,
		Key:       akey,
		SeqNo:     seqno,
		Timestamp: time.Now().UnixNano(),
		Fields:    nil,
	}

	cfb, err := encoder.Internal.CommonFormat(&cfw)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(cfb)
	buf.Write(bd)

	return buf.Bytes(), nil
}

func (b *mysqlReader) produceRow(tp int, t *table, row *[]interface{}) error {
	var err error
	buffered := config.Get().ChangelogBuffer
	seqno := b.nextSeqNo()
	if seqno == 0 {
		return fmt.Errorf("Failed to generate next seqno. Current seqno:%+v", b.seqNo)
	}
	key := encoder.GetRowKey(t.encoder.Schema(), row)
	if buffered && b.bufPipe.Type() == "local" {
		err = t.producer.PushBatch(key, &types.RowMessage{Type: tp, Key: key, Data: row, SeqNo: seqno})
	} else {
		var bd []byte
		bd, err = t.encoder.Row(tp, row, seqno)
		if log.EL(b.log, err) {
			return err
		}
		if !buffered {
			key = t.producer.PartitionKey("log", key)
		} else if t.encoder.Type() != encoder.Internal.Type() {
			bd, err = b.wrapEvent(t.outputFormat, key, bd, seqno)
			if log.EL(b.log, err) {
				return err
			}
		}
		err = t.producer.PushBatch(key, bd)
	}
	//log.Debugf("Pushed to buffer. seqno=%v, table=%v", seqno, t.id)
	if shutdown.Initiated() {
		return nil
	}
	if err != nil {
		b.log.Errorf("Type: %v, Error: %v", tp, err.Error())
		return err
	}
	b.metrics.BinlogRowEventsWritten.Inc(1)
	return nil
}

func (b *mysqlReader) handleRowsEventLow(ev *replication.BinlogEvent, t *table) bool {
	var err error

	re := ev.Event.(*replication.RowsEvent)

	//	log.Debugf("Handle rows event %+v. tableid=%v, latency=%v, now=%v, timestamp=%v", ev.Header.EventType, t.id, time.Now().Unix()-int64(ev.Header.Timestamp), time.Now().Unix(), int64(ev.Header.Timestamp))

	/*
		bb := new(bytes.Buffer)
		ev.Dump(bb)
		fmt.Fprintf(os.Stderr, "Handle rows event %+v", bb.String())
	*/

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		//TODO: Produce as a batch
		for i := 0; i < len(re.Rows) && err == nil; i++ {
			err = b.produceRow(types.Insert, t, &re.Rows[i])
		}
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for i := 0; i < len(re.Rows) && err == nil; i++ {
			err = b.produceRow(types.Delete, t, &re.Rows[i])
		}
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		for i := 0; i < len(re.Rows) && err == nil; i += 2 {
			err = b.produceRow(types.Delete, t, &re.Rows[i])
			if err == nil {
				err = b.produceRow(types.Insert, t, &re.Rows[i+1])
			}
		}
	default:
		err = fmt.Errorf("Not supported event type %v", ev.Header.EventType)
	}

	return !log.E(err)
}

func (b *mysqlReader) handleRowsEvent(ev *replication.BinlogEvent, dbn string, tn string) bool {
	if b.tables[dbn] == nil || b.tables[dbn][tn] == nil {
		return true
	}

	//Produce event to all outputs and versions of the table
	for i := 0; i < len(b.tables[dbn][tn]); i++ {
		if !b.handleRowsEventLow(ev, b.tables[dbn][tn][i]) {
			return false
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

	log.Debugf("REGEXP: %q\n", b.alterQuotedRE.FindAllStringSubmatch(s, -1))

	m := b.alterRE.FindAllStringSubmatch(s, -1)

	if len(m) == 0 {
		m = b.alterQuotedRE.FindAllStringSubmatch(s, -1)
	}

	b.log.Debugf("Match result: %q", m)

	if len(m) == 1 {
		/*Make sure that we have up to date state before deciding whether this
		* alter table is for being ingested table or not */
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
			tver := d[table]
			b.log.Debugf("detected alter statement of being ingested table '%v.%v', mutation '%v'", dbname, table, m[0][3])
			for i := 0; i < len(tver); i++ {
				t := tver[i]
				if !schema.MutateTable(state.GetNoDB(), t.service, dbname, table, m[0][3], t.encoder.Schema(), &t.rawSchema) ||
					!state.ReplaceSchema(t.service, b.dbl.Cluster, t.encoder.Schema(), t.rawSchema, t.schemaGtid, b.gtidSet.String(), "mysql", t.output, t.version, t.outputFormat) {
					return false
				}

				t.schemaGtid = b.gtidSet.String()

				err := t.encoder.UpdateCodec()
				if log.EL(b.log, err) {
					return false
				}
				log.Debugf("Updated codec. id=%v", t.id)
			}

			if !b.pushSchema(tver) {
				return false
			}

			b.metrics.BinlogQueryEventsWritten.Inc(int64(len(tver)))

			return true
		}
	}

	b.log.Debugf("Unhandled query. Query: %+v, Schema: %+v", s, util.BytesToString(qe.Schema))
	return true
}

func (b *mysqlReader) incGTID(v *replication.GTIDEvent) bool {
	u, err := uuid.FromBytes(v.SID)
	if log.E(err) {
		return false
	}

	if s, ok := b.gtidSet.Sets[u.String()]; ok {
		l := &s.Intervals[len(s.Intervals)-1]
		if l.Stop == v.GNO {
			l.Stop++
			return true
		}
		b.log.Infof("non-sequential gtid event: %+v %+v", l.Stop, v.GNO)
	}

	gtid := fmt.Sprintf("%s:%d", u.String(), v.GNO)
	b.log.Infof("non-sequential gtid event: %+v", gtid)
	b.log.Infof("out gtid set: %+v", b.gtidSet.String())
	us, err := mysql.ParseUUIDSet(gtid)
	if log.E(err) {
		return false
	}

	b.gtidSet.AddSet(us)

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
			b.metrics.BinlogUnhandledEvents.Inc(1)
			bb := new(bytes.Buffer)
			ev.Dump(bb)
			b.log.Debugf("Unhandled binlog event: %+v", bb.String())
			fmt.Fprintf(os.Stderr, bb.String())
		}
	}

	return true
}

type result struct {
	ev  *replication.BinlogEvent
	err error
}

func (b *mysqlReader) processBatch(msg *result, msgCh chan *result) bool {
	var i, s int
L:
	for {
		if msg.err != nil {
			if msg.err.Error() != "context canceled" {
				b.log.Errorf("BinlogReadEvents: %v", msg.err.Error())
				return false
			}
			break L //Shutting down, let it commit current  batchu
		}

		s++
		if !b.handleEvent(msg.ev) {
			return false
		}

		i++
		if i >= b.batchSize*b.numTables {
			break
		}

		select {
		case msg = <-msgCh:
		default:
			break L //No messages for now, break the loop and commit whatever we pushed to the batch already
		}
	}

	b.metrics.EventsRead.Inc(int64(s))
	b.metrics.BatchSize.Record(time.Duration(s * 1000000))

	//TODO: Commit only tables which had data in this batch
	w := b.metrics.ProduceLatency
	w.Start()
	for _, sdb := range b.tables {
		for _, tver := range sdb {
			for i := 0; i < len(tver); i++ {
				err := tver[i].producer.PushBatchCommit()
				if log.EL(b.log, err) {
					w.Stop()
					return false
				}
			}
		}
	}
	w.Stop()

	return true
}

//eventFetcher is blocking call to buffered channel converter
func (b *mysqlReader) eventFetcher(ctx context.Context, s *replication.BinlogStreamer, wg *sync.WaitGroup, msgCh chan *result, exitCh chan bool) {
	defer wg.Done()
L:
	for {
		msg := &result{}
		//b.metrics.ReadLatency.Start()
		msg.ev, msg.err = s.GetEvent(ctx)
		//b.metrics.ReadLatency.Stop()
		select {
		case msgCh <- msg:
			if msg.err != nil && msg.err.Error() != "context canceled" {
				break L
			}
		case <-exitCh:
			break L
		}
	}
	log.Debugf("Finished MySQL binlog reader helper goroutine")
}

func (b *mysqlReader) readEvents(c *db.Addr, stateUpdateTimeout int) {
	id := rand.Uint32()
	for id == 0 {
		id = rand.Uint32()
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID: id,
		Flavor:   "mysql",
		Host:     c.Host,
		Port:     c.Port,
		User:     c.User,
		Password: c.Pwd,
	}

	syncer := replication.NewBinlogSyncer(&cfg)
	streamer, err := syncer.StartSyncGTID(b.gtidSet)
	if log.E(err) {
		return
	}
	defer syncer.Close()

	updateTicker := time.NewTicker(time.Second * time.Duration(stateUpdateTimeout))
	defer updateTicker.Stop()

	defer b.closeTableProducers()
	if !b.updateState(true) {
		return
	}

	b.log.WithFields(log.Fields{"gtid": b.gtidSet.String(), "SeqNo": b.seqNo}).Infof("Binlog start")

	msgCh, exitCh := make(chan *result, b.batchSize), make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	defer func() { cancel(); close(exitCh); wg.Wait() }()

	/*This goroutine is to multiplex blocking streamer.GetEvent and tickCh*/
	go b.eventFetcher(ctx, streamer, &wg, msgCh, exitCh)

M:
	for !shutdown.Initiated() {
		select {
		case <-updateTicker.C:
			if !b.updateState(false) {
				break M
			}
		case msg, ok := <-msgCh:
			if !ok || !b.processBatch(msg, msgCh) {
				break M
			}
		case <-shutdown.InitiatedCh():
		}
	}

	b.log.Debugf("Finishing MySQL binlog reader")

	if !log.EL(b.log, state.SaveBinlogState(&b.dbl, b.gtidSet.String(), b.seqNo)) {
		b.log.WithFields(log.Fields{"gtid": b.gtidSet.String(), "SeqNo": b.seqNo}).Infof("Binlog state saved")
	}
}

func (b *mysqlReader) lockCluster(lock lock.Lock, st state.Type) bool {
	if len(st) == 0 {
		return false
	}
	for pos, j := rand.Int()%len(st), 0; j < len(st); j++ {
		r := st[pos]
		ln := "cluster." + r.Cluster
		log.Debugf("Trying to lock: " + ln)
		if lock.TryLock(ln) {
			b.dbl.Cluster = r.Cluster
			b.dbl.Service = r.Service
			b.dbl.Name = r.Db
			return true
		}
		pos = (pos + 1) % len(st)
	}

	return false
}

func (b *mysqlReader) start(cfg *config.AppConfig) bool {
	st, err := state.GetCond("input='mysql'")
	if log.E(err) {
		return true
	}

	b.lock = lock.Create(state.GetDbAddr(), 1)
	defer b.lock.Close()

	if !b.lockCluster(b.lock, st) {
		if len(st) != 0 {
			log.Debugf("Couldn't lock any cluster")
		}
		return false /* Couldn't lock any cluster, no binlog-readers needed */
	}

	bTags := getClusterTag(b.dbl.Cluster)
	b.metrics = metrics.GetBinlogReaderMetrics(bTags)
	log.Debugf("Initializing metrics for MySQL binlog reader, tagged with tags: %v ", bTags)

	b.metrics.NumWorkers.Inc()
	defer b.metrics.NumWorkers.Dec()

	thisInstanceCluster = b.dbl.Cluster
	b.batchSize = cfg.PipeBatchSize

	b.log = log.WithFields(log.Fields{"cluster": b.dbl.Cluster})

	b.log.Infof("Starting MySQL binlog reader")

	/* Get Master's connection info */
	b.masterCI = db.GetInfo(&b.dbl, db.Master)
	if b.masterCI == nil {
		return true
	}

	rf := b.binlogFormat()
	if rf != "ROW" {
		b.log.Errorf("Binlog format is %s", rf)
		b.log.Errorf("Row binlog format required. Skipping")
		return true
	}

	b.tables = make(map[string]map[string][]*table)
	//Golang doesn't support backreferences so next doesn't work:
	//b.alterRE = regexp.MustCompile("^\\s*alter\\s+table\\s+(?:(`)?(\\w+)(`)?\\.)?(`)?(\\w+)(`)?\\s+(.*)$")
	//And we will not handle cases when table is backticked and database is not
	//and vice versa
	b.alterRE = regexp.MustCompile(`(?im)^\s*alter\s+table\s+(?:(\w+)\.)?(\w+)\s+([^$]+)$`)
	//b.alterQuotedRE = regexp.MustCompile("(?i)^\\s*alter\\s+table\\s+(?:`([^\\.]+)`\\.)?`([^`]+)`\\s+([^$]*)$")
	b.alterQuotedRE = regexp.MustCompile(`(?i)^\s*alter\s+table\s+(?:` + "`" + `([^\.]+)` + "`" + `\.)?` + "`" + `([^` + "`" + `]+)` + "`" + `\s+([^$]*)$`)

	/* Start reading binlogs from the gtid set saved in the state */
	gtid, err := state.GetGTID(&b.dbl)
	if log.EL(b.log, err) {
		return true
	}

	/* If no gtid in the state get current gtid set from master */
	if gtid == "" {
		gtid, err = state.GetCurrentGTID(b.masterCI)
		if err != nil {
			return true
		}
		err = state.SetGTID(&b.dbl, gtid)
		if err != nil {
			b.log.Errorf("Error saving gtid. gtid %v. Error: %v", gtid, err.Error())
		}
	}

	s, err := mysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		b.log.Errorf("Invalid gtid: '%v' Error: %v", gtid, err.Error())
		return true
	}
	b.gtidSet = s.(*mysql.MysqlGTIDSet)

	b.readEvents(b.masterCI, cfg.StateUpdateTimeout)

	b.log.Infof("MySQL Binlog reader finished")

	return true
}

func (b *mysqlReader) Worker() bool {
	return b.start(config.Get())
}
