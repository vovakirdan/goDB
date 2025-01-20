package utils

import (
	"fmt"
	"os"
)

func DoMetaCommand(commandContext string) MetaCommandResult {
	// if we know such command then success
	if commandContext == ".exit" {
		os.Exit(int(MetaCommandSuccess))
	}
	return MetaCommandUnrecognized
}

func PrepareSrarement(input string, statement *Statement) PrepareResult {
	if len(input) < 6 {
		return PrepareUnrecognizedStatement
	}
	// if startswith insert
	if input[:6] == "insert" {
		statement.Type = StatementInsert
		return PrepareSuccess
	}
	// if startswith select
	if input[:6] == "select" {
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