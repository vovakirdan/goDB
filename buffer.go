package main

import (
	"strings"
)

// implement buffer that can:
// count symbols, count words (keywords)

type Buffer struct {
	buffer string
	keywords []string
	nWords int
	nSymbols int
	sysCommand bool
}

func NewBuffer() *Buffer {
	return &Buffer{}
}

func (b *Buffer) read(input string) {
	b.buffer = strings.TrimSpace(input)
	b.sysCommand = strings.HasPrefix(input, ".")
	b.keywords = strings.Fields(input)
	if b.sysCommand {
		b.keywords = b.keywords[:0]
	}
	b.nSymbols = len(input)
	b.nWords = len(b.keywords)
}
