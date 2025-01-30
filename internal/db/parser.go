package db

import (
	"fmt"
	"os"
	"strconv"

	"goDB/internal/buffer"
	"goDB/internal/types"
)

// DoMetaCommand проверяет мета-команды
func DoMetaCommand(b *buffer.Buffer, t *Table) (types.MetaCommandResult, *Table) {
	if len(b.Keywords()) == 0 {
		return types.MetaCommandEmpty, t
	}
	cmd := b.Keywords()[0]

	switch cmd {
    case "exit":
        // Close if open
        if t != nil {
            _ = DbClose(t)
        }
        os.Exit(int(types.MetaCommandSuccess))

    case "open":
        // Usage: ".open mydb.db"
        if len(b.Keywords()) < 2 {
            fmt.Println("Usage: .open <filename>")
            return types.MetaCommandSuccess, t
        }
        filename := b.Keywords()[1]
        newTable, err := DbOpen(filename)
        if err != nil {
            fmt.Printf("Error opening database file: %v\n", err)
            return types.MetaCommandSuccess, t
        }
        fmt.Println("Database opened successfully!")
        return types.MetaCommandSuccess, newTable

    case "btree":
        // Print B-Tree of current DB (if open)
        if t == nil {
            fmt.Println("No database is open.")
            return types.MetaCommandSuccess, t
        }
        fmt.Println("Tree:")
        printLeafNode(t)
        return types.MetaCommandSuccess, t

    case "constants":
        printConstants()
        return types.MetaCommandSuccess, t

    default:
        // Unrecognized meta command
        return types.MetaCommandUnrecognized, t
    }

	return types.MetaCommandUnrecognized, t
}

// PrepareStatement разбирает INSERT/SELECT
func PrepareStatement(b *buffer.Buffer, statement *types.Statement) types.PrepareResult {
	words := b.Keywords()
	if len(words) == 0 {
		return types.PrepareUnrecognizedStatement
	}

	switch words[0] {
	case "insert":
		statement.Type = types.StatementInsert
		if len(words) < 4 {
			return types.PrepareSyntaxError
		}
		idVal, err := strconv.Atoi(words[1])
		if err != nil {
			return types.PrepareSyntaxError
		}
		statement.ID = uint32(idVal)
		statement.Username = words[2]
		statement.Email = words[3]
		return types.PrepareSuccess

	case "select":
		statement.Type = types.StatementSelect
		return types.PrepareSuccess

	default:
		return types.PrepareUnrecognizedStatement
	}
}

// ExecuteStatement
func ExecuteStatement(stmt *types.Statement, t *Table) {
	switch stmt.Type {
	case types.StatementInsert:
		executeInsert(stmt, t)
	case types.StatementSelect:
		executeSelect(t)
	}
}

// INSERT с учётом поиска позиции и проверки дубликатов
func executeInsert(stmt *types.Statement, t *Table) {
	// Проверим, не заполнен ли лист
	page0, err := getPage(t.pager, t.rootPageNum)
	if err != nil {
		fmt.Println("Error reading root page:", err)
		return
	}
	numCells := *leafNodeNumCells(page0)
	if numCells >= LeafNodeMaxCells {
		// Не реализован сплит
		fmt.Println("Error: Table full (need split).")
		return
	}

	key := stmt.ID
	cursor := TableFind(t, key)

	// Check duplicates
	if cursor.CellNum < numCells {
		existingKey := *leafNodeKey(page0, cursor.CellNum)
		if existingKey == key {
			fmt.Println("Error: Duplicate key.")
			return
		}
	}

	// form row
	var r Row
	r.ID = stmt.ID
	copy(r.Username[:], stmt.Username)
	copy(r.Email[:], stmt.Email)

	// Вставляем
	err = leafNodeInsert(cursor, r.ID, &r)
	if err != nil {
		fmt.Println("Error during insert:", err)
		return
	}

	fmt.Println("Executed.")
}

// SELECT (просто обходим все ячейки)
func executeSelect(t *Table) {
	c := TableStart(t)
	var r Row

	for !c.EndOfTable {
		slot, err := CursorValue(c)
		if err != nil {
			fmt.Println("Error reading row from cursor:", err)
			return
		}
		deserializeRow(slot, &r)
		PrintRow(&r)
		CursorAdvance(c)
	}
	fmt.Println("Executed.")
}

// Debug Print
func printConstants() {
	fmt.Println("Constants:")
	fmt.Printf("ROW_SIZE: %d\n", rowSize)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNodeMaxCells)
}

func printLeafNode(t *Table) {
	page0, err := getPage(t.pager, t.rootPageNum)
	if err != nil {
		fmt.Println("Error reading page 0:", err)
		return
	}
	numCells := *leafNodeNumCells(page0)
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		keyPtr := leafNodeKey(page0, i)
		key := *keyPtr
		fmt.Printf("  - %d : %d\n", i, key)
	}
}
