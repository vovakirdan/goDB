package main

import (
	"bufio"
	"os"
	"strings"

	"goDB/utils"
)

func main() {
	utils.PrintPrompt()
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, ".") {
			call, context := utils.ParseCommand(line)
			utils.DoComand(call, context)
		}
	}
}