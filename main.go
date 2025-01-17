package main

import (
	"bufio"
	"os"
	"strings"

	"goDB/utils"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		utils.PrintPrompt()
		scanner.Scan()
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, ".") {
			switch (utils.DoMetaCommand(line)) {
				case utils.MetaCommandSuccess:
					continue
				case utils.MetaCommandUnrecognized:
					utils.PrintUnrecognizedCommand(line)
					continue
			}
		}
		var statement utils.Statement
		switch (utils.PrepareSrarement(line, &statement)) {
		case utils.PrepareSuccess:
			// do nothing
		case utils.PrepareUnrecognizedStatement:
			utils.PrintUnrecognizedKeyword(line)
			continue
		}

		// execute statement
	}
}