package types

import (
	"unsafe"
)

const (
	COLUMN_USERNAME_SIZE uint32 = 32
	COLUMN_EMAIL_SIZE    uint32 = 255

	PAGE_SIZE          uint32 = 4096
	TABLE_MAX_PAGES    int    = 100
)

type Row struct {
	ID       uint32
	Username [COLUMN_USERNAME_SIZE]byte
	Email    [COLUMN_EMAIL_SIZE]byte
}

var (
	ID_SIZE        = uint32(unsafe.Sizeof(((Row{}).ID)))
	USERNAME_SIZE  = COLUMN_USERNAME_SIZE
	EMAIL_SIZE     = COLUMN_EMAIL_SIZE
	ID_OFFSET      = uint32(0)
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

type Table struct {
	NumRows uint32
	Pages   [TABLE_MAX_PAGES][]byte
}

const (
	DefaultPrompt       = "goDB> "
	UnrecognizedPromt   = "Unrecognized command:"
	UnrecognizedKeyword = "unrecognized keyword"
)

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandEmpty
	MetaCommandUnrecognized
)

type ExecuteResult int

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognizedStatement
	PrepareSyntaxError
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type        StatementType
	RowToInsert Row
}
