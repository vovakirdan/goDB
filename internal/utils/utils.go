package utils

import (
	"strings"
	"fmt"

	"goDB/internal/types"
)

// createCharacterSet creates a character set from a phrase.
// This is just an example function you had.
func createCharacterSet(phrase string) []string {
	var set []string
	lenPhrase := len(phrase)
	for i := 0; i < lenPhrase; i++ {
		set = append(set, phrase[0:i]+strings.Repeat(" ", lenPhrase-i+1))
	}
	set = append(set, phrase)
	for i := lenPhrase - 1; i >= 0; i-- {
		set = append(set, phrase[0:i]+strings.Repeat(" ", lenPhrase-i+1))
	}
	return set
}

func PrintPrompt() {
	fmt.Print(types.DefaultPrompt)
}

func PrintUnrecognizedCommand(prompt string) {
	fmt.Printf("%s %s\n", types.UnrecognizedPromt, strings.Trim(prompt, "."))
}

func PrintEmptyCommand() {
	fmt.Println("Empty command passed")
}

func PrintUnrecognizedKeyword(prompt string) {
	// e.g. user typed "selrct", we highlight it
	fmt.Printf("%s ...\n", prompt)
	fmt.Printf("%s %s\n", strings.Repeat("^", len(prompt)), types.UnrecognizedKeyword)
}

func PrintHelp() {
	fmt.Println("Help:")
}
