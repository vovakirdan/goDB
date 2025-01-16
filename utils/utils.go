package utils

import (
	"strings"
	"fmt"
	// "time"

	// "github.com/briandowns/spinner"
)

// createCharacterSet creates a character set from a phrase to show it in a spinner
/*someSet := createCharacterSet("hello world")
	s := spinner.New(someSet, 100*time.Millisecond)
	s.Start()
	time.Sleep(4 * time.Second)
	s.Stop()*/
func createCharacterSet(phrase string) []string {
	var set []string
	lenPhrase := len(phrase)
	for i := 0; i < lenPhrase; i++ {
		// phrase[0:i] + spaces * (lenPhrase - i + 1)
		set = append(set, phrase[0:i]+strings.Repeat(" ", lenPhrase-i+1))
	}
	set = append(set, phrase)
	for i := lenPhrase - 1; i >= 0; i-- {
		set = append(set, phrase[0:i]+strings.Repeat(" ", lenPhrase-i+1))
	}
	return set
} 


func PrintPrompt() {
	fmt.Print(Prompt)
}