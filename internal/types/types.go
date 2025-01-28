package types

// Различные константы и типы для парсинга и исполнения

const (
	DefaultPrompt       = "goDB> "
	UnrecognizedPromt   = "Unrecognized command:"
	UnrecognizedKeyword = "unrecognized keyword"
)

// MetaCommandResult для .команд
type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandEmpty
	MetaCommandUnrecognized
)

// ExecuteResult для результатов выполнения (полная таблица, успех и т.п.)
type ExecuteResult int

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
)

// PrepareResult для стадий парсинга
type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognizedStatement
	PrepareSyntaxError
)

// Типы команд
type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

// Statement хранит данные, необходимые для INSERT / SELECT
type Statement struct {
	Type     StatementType
	ID       uint32
	Username string
	Email    string
}
