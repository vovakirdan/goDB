package main

import (
    "bufio"
    "fmt"
    "os"

    "goDB/internal/buffer"
    "goDB/internal/db"
    "goDB/internal/types"
    "goDB/internal/utils"
)

func main() {
    var t *db.Table

    // Создаём буфер ввода
    b := buffer.NewBuffer()
    scanner := bufio.NewScanner(os.Stdin)

    for {
        utils.PrintPrompt()
        if !scanner.Scan() {
            break
        }
        inputText := scanner.Text()

        b.Read(inputText)
        if b.NWords() == 0 {
            continue
        }

        // Мета-команды (нач. с '.')
        if b.IsSysCommand() {
            result, newTable := db.DoMetaCommand(b, t)
            if newTable != nil {
                t = newTable // Assign the newly opened table
            }

            switch result {
            case types.MetaCommandEmpty:
                utils.PrintEmptyCommand()
                continue
            case types.MetaCommandUnrecognized:
                utils.PrintUnrecognizedCommand(b.Keywords()[0])
                continue
            case types.MetaCommandSuccess:
                continue
            }
        }

        if t == nil {
            fmt.Println("No database is open. Use '.open <filename>' to open or create a database file.")
            continue
        }

        // Подготавливаем Statement
        var stmt types.Statement
        switch db.PrepareStatement(b, &stmt) {
        case types.PrepareSuccess:
            // proceed
        case types.PrepareSyntaxError:
            utils.PrintSyntaxError(b)
            continue
        case types.PrepareUnrecognizedStatement:
            utils.PrintUnrecognizedKeyword(b.Keywords()[0])
            continue
        }

        // Выполняем Statement
        db.ExecuteStatement(&stmt, t)
    }
	if t != nil {
        _ = db.DbClose(t)
    }
}
