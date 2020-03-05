package epidemic

import (
	"context"
	"fmt"
	"io"
)

// Action .
type Action uint8

func (act Action) String() string {
	if act == ActInsert {
		return "Insert"
	} else if act == ActUpdate {
		return "Update"
	} else if act == ActDelete {
		return "Delete"
	}
	return ""
}

const (
	// ActInsert .
	ActInsert = 1 << iota
	// ActUpdate .
	ActUpdate
	// ActDelete .
	ActDelete
)

// Opener .
type Opener interface {
	Open() error
}

// Closer .
type Closer interface {
	Close() error
}

// Reader .
type Reader interface {
	Read() (Row, error)
}

// Dumper .
type Dumper interface {
	Dump(w io.Writer)
}

// Contexter .
type Contexter interface {
	Context() context.Context
}

// TableInfo .
type TableInfo struct {
	Schema  string
	Table   string
	Columns []*Column
}

func (tableInfo TableInfo) String() string {
	return fmt.Sprintf("[Schema: %s, Table: %v, Columns: %v]", tableInfo.Schema, tableInfo.Table, tableInfo.Columns)
}

// Column .
type Column struct {
	Name      string
	Type      int
	Collation string
}

func (column Column) String() string {
	return fmt.Sprintf("[Name: %s, Type: %d, Collation: %s]", column.Name, column.Type, column.Collation)
}

// Row .
type Row interface {
	Dumper

	TableInfo() *TableInfo
	Action() Action
	RawData() interface{}
	GetFieldValue(name string) (interface{}, error)
	GetOldFiledValue(name string) (interface{}, error)
}

// Epidemic .
type Epidemic interface {
	Contexter
	Opener
	Closer
	Reader
	Dumper
}
