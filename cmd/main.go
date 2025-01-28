package main

import (
    "bufio"
    "os"

    "goDB/internal/buffer"
    "goDB/internal/parser"
    "goDB/internal/table"
    "goDB/internal/types"
	"goDB/internal/utils"
)

func main() {
    // Create a Table
    t := table.NewTable()

    // Create a new buffer for user input
    b := buffer.NewBuffer()

    // Start reading lines
    scanner := bufio.NewScanner(os.Stdin)
    for {
        utils.PrintPrompt()
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
                utils.PrintEmptyCommand()
                continue
            case types.MetaCommandUnrecognized:
                utils.PrintUnrecognizedCommand(b.Keywords()[0])
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
            utils.PrintSyntaxError(b)
            continue
        case types.PrepareUnrecognizedStatement:
			utils.PrintUnrecognizedKeyword(b.Keywords()[0])
            continue
        }

        // Execute the statement
        parser.ExecuteStatement(&stmt, t)
    }
}
