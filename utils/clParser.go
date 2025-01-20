package utils

import (
	"fmt"
	"os"
)

func DoMetaCommand(buffer *Buffer) MetaCommandResult {
	if len(buffer.keywords) == 0 {
		return MetaCommandEmpty
	}
	// if we know such command then success
	if buffer.keywords[0] == "exit" {
		os.Exit(int(MetaCommandSuccess))
	}
	return MetaCommandUnrecognized
}

func PrepareSrarement(buffer *Buffer, statement *Statement) PrepareResult {
	switch buffer.keywords[0] {
		case "insert":
			statement.Type = StatementInsert
			return PrepareSuccess
		case "select":
			statement.Type = StatementSelect
			return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

func ExecuteStatement(statement *Statement) {
	switch statement.Type {
	case StatementInsert:
		fmt.Println("Executing insert statement")
	case StatementSelect:
		fmt.Println("Executing select statement")
	}
}