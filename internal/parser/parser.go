package parser

import (
	"fmt"
	"os"
	"strconv"

	"goDB/internal/buffer"
	"goDB/internal/table"
	"goDB/internal/types"
)

// DoMetaCommand checks meta-commands like "exit"
func DoMetaCommand(b *buffer.Buffer, t *types.Table) types.MetaCommandResult {
	if len(b.Keywords()) == 0 {
		return types.MetaCommandEmpty
	}
	// If we know such command, then success
	if b.Keywords()[0] == "exit" {
		// In the C tutorial, we free the table. You can mimic that or just exit:
		table.FreeTable(t)
		os.Exit(int(types.MetaCommandSuccess))
	}
	return types.MetaCommandUnrecognized
}

// PrepareStatement parses the user input to fill Statement fields
func PrepareStatement(b *buffer.Buffer, statement *types.Statement) types.PrepareResult {
	if len(b.Keywords()) == 0 {
		return types.PrepareUnrecognizedStatement
	}

	switch b.Keywords()[0] {
	case "insert":
		statement.Type = types.StatementInsert
		// We expect: insert <id> <username> <email>
		if b.NWords() < 4 {
			return types.PrepareSyntaxError
		}

		// Parse ID
		idVal, err := strconv.Atoi(b.Keywords()[1])
		if err != nil {
			return types.PrepareSyntaxError
		}
		statement.RowToInsert.ID = uint32(idVal)

		// Copy username, email into fixed-size arrays
		copy(statement.RowToInsert.Username[:], b.Keywords()[2])
		copy(statement.RowToInsert.Email[:], b.Keywords()[3])

		return types.PrepareSuccess

	case "select":
		statement.Type = types.StatementSelect
		return types.PrepareSuccess

	default:
		return types.PrepareUnrecognizedStatement
	}
}

// ExecuteStatement runs INSERT/SELECT logic against our table
func ExecuteStatement(statement *types.Statement, t *types.Table) {
	switch statement.Type {
	case types.StatementInsert:
		res := table.ExecuteInsert(statement, t)
		if res == types.ExecuteTableFull {
			fmt.Println("Error: Table full.")
		} else {
			fmt.Println("Executed.")
		}

	case types.StatementSelect:
		table.ExecuteSelect(t)
		fmt.Println("Executed.")
	}
}
