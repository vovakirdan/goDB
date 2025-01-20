package main

import (
	"bufio"
	"os"

	"goDB/utils"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	buffer := utils.NewBuffer()

	for {
		utils.PrintPrompt()
		scanner.Scan()
		if len(scanner.Text()) == 0 {
			continue
		}
		buffer.Read(scanner.Text())
		if buffer.IsSysCommand() {
			switch (utils.DoMetaCommand(buffer)) {
				case utils.MetaCommandSuccess:
					continue
				case utils.MetaCommandEmpty:
					utils.PrintEmptyCommand()
					continue
				case utils.MetaCommandUnrecognized:
					utils.PrintUnrecognizedCommand(buffer.Buffer())
					continue
			}
		}
		var statement utils.Statement
		switch (utils.PrepareSrarement(buffer, &statement)) {
		case utils.PrepareSuccess:
			continue
		case utils.PrepareUnrecognizedStatement:
			utils.PrintUnrecognizedKeyword(buffer.Keywords()[0])
			continue
		}

		// execute statement
		utils.ExecuteStatement(&statement)
	}
}