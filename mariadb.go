package epidemic

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

// column cache
var columnCache = map[string][]*Column{}

type masterInfo struct {
	sync.RWMutex
	pos       mysql.Position
	gset      mysql.GTIDSet
	timestamp uint32
}

func (m *masterInfo) Update(pos mysql.Position) {
	m.Lock()
	m.pos = pos
	m.Unlock()
}

func (m *masterInfo) UpdateTimestamp(ts uint32) {
	m.Lock()
	m.timestamp = ts
	m.Unlock()
}

func (m *masterInfo) UpdateGTIDSet(gset mysql.GTIDSet) {
	m.Lock()
	m.gset = gset
	m.Unlock()
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()
	return m.pos
}

func (m *masterInfo) Timestamp() uint32 {
	m.RLock()
	defer m.RUnlock()
	return m.timestamp
}

func (m *masterInfo) GTIDSet() mysql.GTIDSet {
	m.RLock()
	defer m.RUnlock()
	if m.gset == nil {
		return nil
	}
	return m.gset.Clone()
}

func (m *masterInfo) Save() error {
	// TODO: save to file
	return nil
}

// mariadbRow implement Row
type mariadbRow struct {
	tableInfo *TableInfo
	action    Action
	rawData   interface{}
	data      []interface{}
	oldData   []interface{}
}

func (mr *mariadbRow) TableInfo() *TableInfo {
	return mr.tableInfo
}

func (mr *mariadbRow) Dump(w io.Writer) {
	fmt.Fprintf(w, "TableInfo: %v\n", mr.tableInfo)
	fmt.Fprintf(w, "Action: %v\n", mr.action)
	fmt.Fprintf(w, "RawData: %v\n", mr.rawData)
	fmt.Fprintln(w)
}

func (mr *mariadbRow) Action() Action {
	return mr.action
}

func (mr *mariadbRow) RawData() interface{} {
	return mr.rawData
}

func (mr *mariadbRow) GetFieldValue(name string) (interface{}, error) {
	if mr.tableInfo.Columns == nil {
		return nil, errors.New("columns is nil")
	}
	if mr.data == nil {
		return nil, errors.New("data not found")
	}
	var index = -1
	for i, column := range mr.tableInfo.Columns {
		if column.Name == name {
			index = i
		}
	}
	if index == -1 {
		return nil, errors.Errorf("column [%s] not found", name)
	}
	return mr.data[index], nil
}

func (mr *mariadbRow) GetOldFiledValue(name string) (interface{}, error) {
	if mr.tableInfo.Columns == nil {
		return nil, errors.New("columns is nil")
	}
	if mr.oldData == nil {
		return nil, errors.New("old data not found")
	}
	var index = -1
	for i, column := range mr.tableInfo.Columns {
		if column.Name == name {
			index = i
		}
	}
	if index == -1 {
		return nil, errors.Errorf("column [%s] not found", name)
	}
	return mr.oldData[index], nil
}

// mariadb .
type mariadb struct {
	mutex sync.Mutex

	*MariadbOption

	master *masterInfo

	ctx    context.Context
	cancel context.CancelFunc

	syncer *replication.BinlogSyncer

	connLock sync.Mutex
	conn     *client.Conn

	// row event chan
	rc chan Row
	// error chan
	ec chan error
}

// MariadbOption .
type MariadbOption struct {
	// master server id
	ServerID uint32
	// ip addr
	Host string
	// port
	Port uint16
	// user
	User string
	// password
	Password string
	// charset
	Charset string
	// position path
	Path string
}

func (m *mariadb) Context() context.Context {
	return m.ctx
}

// Open implement Epidemic
func (m *mariadb) Open() error {
	streamer, err := m.syncer.StartSync(mysql.Position{})
	if err != nil {
		return err
	}
	go func() {
		for {
			ev, err := streamer.GetEvent(m.ctx)
			if err != nil {
				m.ec <- err
				return
			}

			pos := m.master.Position()
			pos.Pos = ev.Header.LogPos

			switch e := ev.Event.(type) {
			case *replication.MariadbGTIDEvent:
			case *replication.RotateEvent:
				pos.Name = string(e.NextLogName)
				pos.Pos = uint32(e.Position)
				m.master.Update(pos)
				m.master.UpdateTimestamp(ev.Header.Timestamp)
				fmt.Println(pos)
			case *replication.XIDEvent:
				if e.GSet != nil {
					m.master.UpdateGTIDSet(e.GSet)
				}
				m.master.Update(pos)
				m.master.UpdateTimestamp(ev.Header.Timestamp)
			case *replication.RowsEvent:
				row, err := m.handleRowsEvent(ev)
				if err != nil {
					log.Fatal(err)
				}
				m.rc <- row
			}
		}
	}()
	return nil
}

// Close implement Epidemic
func (m *mariadb) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.cancel()
	m.syncer.Close()
	m.connLock.Lock()
	m.conn.Close()
	m.conn = nil
	m.connLock.Unlock()
	return nil
}

// Read implement Epidemic
func (m *mariadb) Read() (Row, error) {
	select {
	case row := <-m.rc:
		return row, nil
	case err := <-m.ec:
		return nil, err
	}
}

// Dump implement Epidemic
func (m *mariadb) Dump(w io.Writer) {
	fmt.Fprint(w, "Type: Mariadb\n")
	fmt.Fprintf(w, "Host: %s\n", m.Host)
	fmt.Fprintf(w, "Port: %d\n", m.Port)
	fmt.Fprintf(w, "ServerID: %d\n", m.ServerID)
	fmt.Fprintf(w, "User: %s\n", m.User)
	fmt.Fprintf(w, "Charset: %s\n", m.Charset)
	fmt.Fprintln(w)
}

func (m *mariadb) handleGTIDEvent(ev *replication.BinlogEvent) error {
	// TODO: GTID实现
	return nil
}

func (m *mariadb) handleRowsEvent(ev *replication.BinlogEvent) (Row, error) {
	event := ev.Event.(*replication.RowsEvent)
	var action Action
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = ActInsert
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = ActUpdate
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = ActDelete
	default:
		return nil, errors.Errorf("%s not supported now", ev.Header.EventType)
	}

	schema := string(event.Table.Schema)
	table := string(event.Table.Table)

	row := new(mariadbRow)
	row.rawData = event.Rows
	row.action = action
	columns, err := m.getColumns(schema, table)
	if err != nil {
		return nil, err
	}
	row.tableInfo = &TableInfo{
		Schema:  schema,
		Table:   table,
		Columns: columns,
	}
	if len(event.Rows) == 1 {
		// insert delete
		row.data = event.Rows[0]
		row.oldData = row.data
	} else if len(event.Rows) == 2 {
		// update
		row.oldData = event.Rows[0]
		row.data = event.Rows[1]
	}
	return row, nil
}

func (m *mariadb) getColumns(schema1, table string) ([]*Column, error) {
	key := fmt.Sprintf("%s.%s", schema1, table)
	if colums, ok := columnCache[key]; ok {
		return colums, nil
	}
	t, err := schema.NewTable(m, schema1, table)
	if err != nil {
		return nil, err
	}
	columns := []*Column{}
	for _, c := range t.Columns {
		columns = append(columns, &Column{
			Name:      c.Name,
			Type:      c.Type,
			Collation: c.Collation,
		})
	}
	columnCache[key] = columns
	return columns, nil
}

// Execute a SQL
func (m *mariadb) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	m.connLock.Lock()
	defer m.connLock.Unlock()

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if m.conn == nil {
			m.conn, err = client.Connect(fmt.Sprintf("%s:%d", m.Host, m.Port), m.User, m.Password, "")
			if err != nil {
				return nil, err
			}
		}

		rr, err = m.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			m.conn.Close()
			m.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

// NewMariadb .
func NewMariadb(ctx context.Context, option *MariadbOption) Epidemic {
	mariadb := mariadb{}
	mariadb.MariadbOption = option
	mariadb.rc = make(chan Row)
	mariadb.ec = make(chan error)
	mariadb.master = &masterInfo{}
	mariadb.ctx, mariadb.cancel = context.WithCancel(ctx)
	config := replication.BinlogSyncerConfig{
		ServerID: option.ServerID,
		Flavor:   "mariadb",
		Host:     option.Host,
		Port:     option.Port,
		User:     option.User,
		Password: option.Password,
		Charset:  option.Charset,
	}
	mariadb.syncer = replication.NewBinlogSyncer(config)
	return &mariadb
}
