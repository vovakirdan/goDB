package main

import (
	"strings"
)

// Buffer хранит введённую строку, список ключевых слов и некоторую статистику
type Buffer struct {
	buffer     string   // исходная строка
	keywords   []string // разбитые слова (токены)
	nWords     int
	nSymbols   int
	sysCommand bool     // признак, что строка начинается с '.'
}

// NewBuffer создаёт новый буфер
func NewBuffer() *Buffer {
	return &Buffer{}
}

// read считывает ввод пользователя и заполняет структуру Buffer
func (b *Buffer) read(input string) {
	input = strings.TrimSpace(input)

	b.buffer = input
	b.sysCommand = strings.HasPrefix(input, ".")

	if b.sysCommand {
		trimmed := strings.TrimLeft(input, ".")
		trimmed = strings.TrimSpace(trimmed)

		if len(trimmed) == 0 {
			b.keywords = nil
		} else {
			b.keywords = strings.Fields(trimmed)
		}
	} else {
		b.keywords = strings.Fields(input)
	}

	b.nSymbols = len(input)
	b.nWords = len(b.keywords)
}
