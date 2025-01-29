package parser

import (
    "fmt"
    "os"
    "strconv"

    "goDB/internal/buffer"
    "goDB/internal/table"
    "goDB/internal/types"
)

// DoMetaCommand проверяет мета-команды начинающиеся с '.'
func DoMetaCommand(b *buffer.Buffer, t *table.Table) types.MetaCommandResult {
    if len(b.Keywords()) == 0 {
        return types.MetaCommandEmpty
    }
    if b.Keywords()[0] == "exit" {
		_ = table.DbClose(t)
        os.Exit(int(types.MetaCommandSuccess))
    }
    return types.MetaCommandUnrecognized
}

// PrepareStatement парсит ввод пользователя и инициализирует Statement
func PrepareStatement(b *buffer.Buffer, statement *types.Statement) types.PrepareResult {
    words := b.Keywords()
    if len(words) == 0 {
        return types.PrepareUnrecognizedStatement
    }

    switch words[0] {
    case "insert":
        statement.Type = types.StatementInsert
        // Ожидаем: insert <id> <username> <email>
        if len(words) < 4 {
            return types.PrepareSyntaxError
        }

        // Парсим id
        idVal, err := strconv.Atoi(words[1])
        if err != nil {
            return types.PrepareSyntaxError
        }
        statement.ID = uint32(idVal)

        // Копируем username и email
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

// ExecuteStatement — теперь используем Cursor
func ExecuteStatement(stmt *types.Statement, t *table.Table) {
    switch stmt.Type {
    case types.StatementInsert:
        executeInsert(stmt, t)

    case types.StatementSelect:
        executeSelect(t)
    }
}

// executeInsert: ставит курсор в конец, пишет новую строку, увеличивает numRows
func executeInsert(stmt *types.Statement, t *table.Table) {
    // Проверка переполнения
    if t.NumRows >= t.MaxRows() {
        fmt.Println("Error: Table full.")
        return
    }

	// Создаём курсор на конец
	c := table.TableEnd(t)
	// Получаем "срез" байт, куда будем сериализовать
	slot, err := table.CursorValue(c)
    if err != nil {
        fmt.Printf("Error obtaining slot: %v\n", err)
        return
    }

    // Формируем новую Row
    var r table.Row
    r.ID = stmt.ID
    // Копируем username/email в фиксированную длину массива
    copy(r.Username[:], stmt.Username)
    copy(r.Email[:], stmt.Email)

	// Сериализуем в slot
	table.SerializeRow(&r, slot)

    // Увеличиваем счётчик строк
    t.NumRows++

    fmt.Println("Executed.")
}

// executeSelect: ставит курсор в начало, идём по строкам, печатаем
func executeSelect(t *table.Table) {
    c := table.TableStart(t)
    var r table.Row

    for !c.EndOfTable {
        slot, err := table.CursorValue(c)
        if err != nil {
            fmt.Printf("Error reading row: %v\n", err)
            return
        }
        // Десериализуем
        table.DeserializeRow(slot, &r)
        table.PrintRow(&r)

        // Двигаем курсор
        table.CursorAdvance(c)
    }

    fmt.Println("Executed.")
}
