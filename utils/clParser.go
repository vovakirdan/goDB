package utils

import "os"

// "fmt"
// "os"

func DoMetaCommand(commandContext string) MetaCommandResult {
	// if we know such command then success
	if commandContext == ".exit" {
		os.Exit(int(MetaCommandSuccess))
	}
	return MetaCommandUnrecognized
}

func PrepareSrarement(input string, statement *Statement) PrepareResult {
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