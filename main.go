package main

import (
	"bufio"
	"os"

	"goDB/utils"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	buffer := NewBuffer()

	for {
		utils.PrintPrompt()
		scanner.Scan()
		if len(scanner.Text()) == 0 {
			continue
		}
		buffer.read(scanner.Text())
		if buffer.sysCommand {
			switch (utils.DoMetaCommand(buffer.buffer)) {
				case utils.MetaCommandSuccess:
					continue
				case utils.MetaCommandEmpty:
					utils.PrintEmptyCommand()
					continue
				case utils.MetaCommandUnrecognized:
					utils.PrintUnrecognizedCommand(buffer.buffer)
					continue
			}
		}
		var statement utils.Statement
		switch (utils.PrepareSrarement(buffer.buffer, &statement)) {
		case utils.PrepareSuccess:
			continue
		case utils.PrepareUnrecognizedStatement:
			utils.PrintUnrecognizedKeyword(buffer.keywords[0])
			continue
		}

		// execute statement
		utils.ExecuteStatement(&statement)
	}
}