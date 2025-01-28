package table

import (
    "bufio"
    "errors"
    "fmt"
    "os"
    "strconv"
    "strings"
)

// Row описывает одну строку в таблице
type Row struct {
    ID       uint32
    Username string
    Email    string
}

// Table — хранит:
//  - rows: список строк
//  - filename: куда сохраняем
//  - maxRows: лимит кол-ва строк
type Table struct {
    rows     []Row
    maxRows  uint32
    filename string
}

// OpenTable загружает/создаёт таблицу из файла.
func OpenTable(filename string) (*Table, error) {
    t := &Table{
        rows:     []Row{},
        maxRows:  1400,
        filename: filename,
    }

    // Проверяем, существует ли файл
    file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
    if os.IsNotExist(err) {
        // Файл не существует => создадим пустой
        return t, nil
    } else if err != nil {
        // Какая-то иная ошибка (нет прав и т.д.)
        return nil, err
    }
    defer file.Close()

    // Считываем построчно
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        // Допустим формат "id|username|email"
        parts := strings.Split(line, "|")
        if len(parts) != 3 {
            continue // или return nil, fmt.Errorf("corrupt line: %s", line)
        }
        // Парсим id
        id64, err := strconv.ParseUint(parts[0], 10, 32)
        if err != nil {
            continue // или return nil, fmt.Errorf("invalid id: %s", parts[0])
        }
        r := Row{
            ID:       uint32(id64),
            Username: parts[1],
            Email:    parts[2],
        }
        t.rows = append(t.rows, r)
    }

    return t, nil
}

// Close записывает все данные t.rows обратно в файл.
// Если нужно было бы выгружать только изменённые строки — усложнили бы логику.
func (t *Table) Close() error {
    // Открываем/перезаписываем файл заново
    file, err := os.OpenFile(t.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
    if err != nil {
        return err
    }
    defer file.Close()

    // Пишем каждую строку в формате "id|username|email\n"
    for _, row := range t.rows {
        line := fmt.Sprintf("%d|%s|%s\n", row.ID, row.Username, row.Email)
        _, err := file.WriteString(line)
        if err != nil {
            return err
        }
    }
    return nil
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
