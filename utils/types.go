package utils

const (
	DefaultPrompt = "goDB> "
	UnrecognizedPromt = "Unrecognized command:"
	UnrecognizedKeyword = "unrecognized keyword"
)

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandEmpty
	MetaCommandUnrecognized
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type StatementType
}