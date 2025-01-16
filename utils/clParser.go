package utils

import (
	"fmt"
	"os"
)

func DoComand(call Call, commandContext string) {
	switch call {
	case Exit:
		os.Exit(0)
	case NewLine:
		fmt.Println(commandContext)
		PrintPrompt()
	}
}

func ParseCommand(line string) (Call, string) {
	if line == ".exit" {
		return Exit, line
	}
	return NewLine, line
}