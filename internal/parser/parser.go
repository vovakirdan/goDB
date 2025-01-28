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
		t.Close()
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

// ExecuteStatement обрабатывает INSERT и SELECT
func ExecuteStatement(stmt *types.Statement, t *table.Table) {
    switch stmt.Type {
    case types.StatementInsert:
        // Вставляем новую строку
        err := t.Insert(stmt.ID, stmt.Username, stmt.Email)
        if err != nil {
            fmt.Println("Error: Table full.")
        } else {
            fmt.Println("Executed.")
        }

    case types.StatementSelect:
        // Выводим все строки
        t.SelectAll()
        fmt.Println("Executed.")
    }
}
