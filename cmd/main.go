package main

import (
    "bufio"
    "fmt"
    "os"

    "goDB/internal/buffer"
    "goDB/internal/parser"
    "goDB/internal/table"
    "goDB/internal/types"
    "goDB/internal/utils"
)

func main() {
    // Пример: go run ./cmd/main.go mydb.db
    if len(os.Args) < 2 {  // todo implement .open command
        fmt.Println("Must supply a database filename. E.g.: go run ./cmd/main.go mydb.db")
        os.Exit(1)
    }
    filename := os.Args[1]

    // Вместо NewTable — открываем (создаём) "persisted" таблицу
    t, err := table.OpenTable(filename)
    if err != nil {
        fmt.Printf("Error opening table from file %s: %v\n", filename, err)
        os.Exit(1)
    }

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
            switch parser.DoMetaCommand(b, t) {
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

        // Подготавливаем Statement
        var stmt types.Statement
        switch parser.PrepareStatement(b, &stmt) {
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
        parser.ExecuteStatement(&stmt, t)
    }
}
