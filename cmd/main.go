package main

import (
    "bufio"
    "fmt"
    "os"

    "goDB/internal/buffer"
    "goDB/internal/parser"
    "goDB/internal/table"
    "goDB/internal/types"
)

func main() {
    // Create a Table
    t := table.NewTable()

    // Create a new buffer for user input
    b := buffer.NewBuffer()

    // Start reading lines
    scanner := bufio.NewScanner(os.Stdin)
    for {
        fmt.Print("goDB> ")
        if !scanner.Scan() {
            break
        }
        inputText := scanner.Text()
        
        // Fill the buffer
        b.Read(inputText)
		if b.NWords() == 0 {
			continue
		}

        // Check if it’s a meta command:
        if b.IsSysCommand() {
            switch parser.DoMetaCommand(b, t) {
            case types.MetaCommandEmpty:
                fmt.Println("Empty command.")
                continue
            case types.MetaCommandUnrecognized:
                fmt.Println("Unrecognized command.")
                continue
            case types.MetaCommandSuccess:
                continue
            }
        }

        // Prepare the statement
        var stmt types.Statement
        switch parser.PrepareStatement(b, &stmt) {
        case types.PrepareSuccess:
            // proceed
        case types.PrepareSyntaxError:
            fmt.Println("Syntax error. Could not parse statement.")
            continue
        case types.PrepareUnrecognizedStatement:
            fmt.Printf("Unrecognized keyword at start of '%s'\n", b.Keywords()[0])
            continue
        }

        // Execute the statement
        parser.ExecuteStatement(&stmt, t)
    }
}
