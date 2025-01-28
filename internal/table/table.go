package table

import (
    "errors"
    "fmt"
)

// Row описывает одну строку в таблице
type Row struct {
    ID       uint32
    Username string
    Email    string
}

// Table — упрощенная "таблица"
// Храним все строки в слайсе.
// Указываем maxRows, чтобы при переполнении выдавать "Table full".
type Table struct {
    rows    []Row
    maxRows uint32
}

// NewTable создает новую "таблицу"
func NewTable() *Table {
    return &Table{
        rows:    make([]Row, 0),
        maxRows: 1400, // аналогично тому, что было (1400 строк лимит)
    }
}

// Insert добавляет новую строку, если не превысили лимит
func (t *Table) Insert(id uint32, username, email string) error {
    // Проверка на переполнение
    if uint32(len(t.rows)) >= t.maxRows {
        return errors.New("table is full")
    }
    newRow := Row{ID: id, Username: username, Email: email}
    t.rows = append(t.rows, newRow)
    return nil
}

// SelectAll выводит все строки
func (t *Table) SelectAll() {
    for _, r := range t.rows {
        printRow(r)
    }
}

// Вспомогательная функция для печати строки
func printRow(r Row) {
    fmt.Printf("(%d, %s, %s)\n", r.ID, r.Username, r.Email)
}
