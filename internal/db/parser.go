package db

import (
	"fmt"
	"os"
	"strconv"

	"goDB/internal/buffer"
	"goDB/internal/types"
)

// DoMetaCommand проверяет мета-команды
func DoMetaCommand(b *buffer.Buffer, t *Table) types.MetaCommandResult {
	if len(b.Keywords()) == 0 {
		return types.MetaCommandEmpty
	}
	cmd := b.Keywords()[0]

	if cmd == "exit" || cmd == ".exit" {
		// Закрываем и выходим
		_ = DbClose(t)
		os.Exit(int(types.MetaCommandSuccess))
	} else if cmd == ".btree" {
		// Вывести структуру единственного листа (page=0)
		fmt.Println("Tree:")
		printLeafNode(t)
		return types.MetaCommandSuccess
	} else if cmd == ".constants" {
		printConstants()
		return types.MetaCommandSuccess
	}

	return types.MetaCommandUnrecognized
}

// PrepareStatement разбирает INSERT, SELECT
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

// ExecuteStatement использует Cursor
func ExecuteStatement(stmt *types.Statement, t *Table) {
	switch stmt.Type {
	case types.StatementInsert:
		executeInsert(stmt, t)
	case types.StatementSelect:
		executeSelect(t)
	}
}

// executeInsert: вставляем в leaf node
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

	// Создаем курсор на "конец"
	c := TableEnd(t)

	// Формируем Row
	var r Row
	r.ID = stmt.ID
	copy(r.Username[:], stmt.Username)
	copy(r.Email[:], stmt.Email)

	// Вставляем
	err = leafNodeInsert(c, r.ID, &r)
	if err != nil {
		fmt.Println("Error during insert:", err)
		return
	}
	// Увеличилось число ячеек leaf
	fmt.Println("Executed.")
}

// executeSelect: печатаем все строки leaf
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

// --------------------------------
// Мелкие вспомогательные обёртки
// --------------------------------

func printConstants() {
	fmt.Println("Constants:")
	fmt.Printf("ROW_SIZE: %d\n", rowSize)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNodeMaxCells)
}

// printLeafNode выводит ключи, хранящиеся в page=0 (единственный лист)
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
