package table

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

// Параметры "страниц" и лимит страниц
const (
	PageSize      = 4096
	TableMaxPages = 100
)

// Структура Row в стиле "C": ID=4 байта, Username=32, Email=255 (итого 291)
type Row struct {
	ID       uint32
	Username [32]byte
	Email    [255]byte
}

// Для вычисления размера Row используем unsafe
var (
	idSize       = uint32(unsafe.Sizeof((Row{}).ID)) // 4
	usernameSize = uint32(len((Row{}).Username))     // 32
	emailSize    = uint32(len((Row{}).Email))        // 255
	rowSize      = idSize + usernameSize + emailSize // 4+32+255=291
)

// Сколько строк вмещается в одну страницу
var rowsPerPage = PageSize / rowSize

// Всего строк помещается в TableMaxPages страниц
var tableMaxRows = rowsPerPage * TableMaxPages

// Pager хранит дескриптор файла, длину файла и кэш страниц
type Pager struct {
	file       *os.File
	fileLength int64
	pages      [TableMaxPages][]byte // каждая страница — слайс на 4096 байт
}

// Table ссылается на Pager и хранит количество строк (numRows)
type Table struct {
	pager   *Pager
	NumRows uint32
}

type Cursor struct {
    Table       *Table
    RowNum      uint32
    EndOfTable  bool
}

func (t *Table) MaxRows() uint32 {
	return tableMaxRows
}

// DbOpen открывает (или создаёт) файл БД и возвращает Table (аналог db_open)
func DbOpen(filename string) (*Table, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("unable to open file: %v", err)
	}

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat error: %v", err)
	}
	fileLength := st.Size()

	pager := &Pager{
		file:       f,
		fileLength: fileLength,
	}
	// Изначально ни одна страница не загружена
	// pager.pages[...] = nil

	// Вычисляем, сколько строк в файле (каждая строка = rowSize байт)
	numRows := uint32(fileLength / int64(rowSize))

	t := &Table{
		pager:   pager,
		NumRows: numRows,
	}
	return t, nil
}

// DbClose сбрасывает (flush) все страницы на диск и закрывает файл (аналог db_close)
func DbClose(t *Table) error {
	pager := t.pager

	// Полных страниц:
	numFullPages := t.NumRows / rowsPerPage

	// Сбрасываем все полные страницы
	for i := uint32(0); i < numFullPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		err := pagerFlush(pager, i, PageSize)
		if err != nil {
			return err
		}
		pager.pages[i] = nil
	}

	// Возможно, есть неполная страница
	additionalRows := t.NumRows % rowsPerPage
	if additionalRows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			bytesToWrite := additionalRows * rowSize
			err := pagerFlush(pager, pageNum, bytesToWrite)
			if err != nil {
				return err
			}
			pager.pages[pageNum] = nil
		}
	}

	// Закрываем файл
	err := pager.file.Close()
	if err != nil {
		return fmt.Errorf("error closing db file: %v", err)
	}

	// Обнуляем ссылки (для повторения C-логики). GC сам всё освободит.
	for i := 0; i < TableMaxPages; i++ {
		pager.pages[i] = nil
	}
	return nil
}

// pagerFlush записывает size байт страницы pageNum на диск
func pagerFlush(p *Pager, pageNum uint32, size uint32) error {
	if p.pages[pageNum] == nil {
		return errors.New("tried to flush null page")
	}
	offset := int64(pageNum) * PageSize

	// Пишем нужные байты
	n, err := p.file.WriteAt(p.pages[pageNum][:size], offset)
	if err != nil && err != io.EOF {
		return fmt.Errorf("write error: %v", err)
	}
	if n < int(size) {
		return fmt.Errorf("could not write full data, wrote %d bytes", n)
	}
	return nil
}

// getPage загружает страницу pageNum в кэш, если её нет
func getPage(p *Pager, pageNum uint32) ([]byte, error) {
	if pageNum >= TableMaxPages {
		return nil, fmt.Errorf("page number %d out of bounds (%d)", pageNum, TableMaxPages)
	}
	if p.pages[pageNum] == nil {
		// Создаём буфер для этой страницы
		p.pages[pageNum] = make([]byte, PageSize)

		// Сколько страниц реально в файле
		fullPages := uint32(p.fileLength / PageSize)
		if p.fileLength%PageSize != 0 {
			fullPages += 1
		}

		// Если наша страница попадает в файл — читаем
		if pageNum < fullPages {
			offset := int64(pageNum) * PageSize
			_, err := p.file.ReadAt(p.pages[pageNum], offset)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("read error at page %d: %v", pageNum, err)
			}
		}
	}

	return p.pages[pageNum], nil
}

// RowSlot вычисляет, где (в какой странице) лежит строка rowNum, и возвращает срез
func RowSlot(t *Table, rowNum uint32) ([]byte, error) {
	if rowNum >= tableMaxRows {
		return nil, errors.New("rowNum out of table max size")
	}
	pageNum := rowNum / rowsPerPage
	page, err := getPage(t.pager, pageNum)
	if err != nil {
		return nil, err
	}

	rowOffset := rowNum % rowsPerPage
	byteOffset := rowOffset * rowSize
	return page[byteOffset : byteOffset+rowSize], nil
}

// TableStart возвращает курсор на начало таблицы
func TableStart(tbl *Table) *Cursor {
    c := &Cursor{
        Table:      tbl,
        RowNum:     0,
        EndOfTable: (tbl.NumRows == 0),
    }
    return c
}

// TableEnd возвращает курсор на позицию "за последней строкой"
func TableEnd(tbl *Table) *Cursor {
    c := &Cursor{
        Table:      tbl,
        RowNum:     tbl.NumRows,
        EndOfTable: true, // "за концом"
    }
    return c
}

// CursorValue даёт указатель (срез) на текущий row в памяти
func CursorValue(c *Cursor) ([]byte, error) {
    rowNum := c.RowNum
    pageNum := rowNum / rowsPerPage
    slotOffset := (rowNum % rowsPerPage) * rowSize

    page, err := getPage(c.Table.pager, pageNum)
    if err != nil {
        return nil, err
    }
    return page[slotOffset : slotOffset+rowSize], nil
}

// CursorAdvance двигает курсор вперёд
func CursorAdvance(c *Cursor) {
    c.RowNum++
    if c.RowNum >= c.Table.NumRows {
        c.EndOfTable = true
    }
}

// PrintRow выводит строку в формате "(id, username, email)"
func PrintRow(r *Row) {
	uname := TrimNull(r.Username[:])
	email := TrimNull(r.Email[:])
	fmt.Printf("(%d, %s, %s)\n", r.ID, uname, email)
}

// SerializeRow / DeserializeRow: копируют поля Row в байтовый срез и обратно
func SerializeRow(source *Row, dest []byte) {
	// ID (4 байта, little-endian)
	PutUint32(dest[0:4], source.ID)
	// username (32 байт)
	copy(dest[4:4+32], source.Username[:])
	// email (255 байт)
	copy(dest[4+32:4+32+255], source.Email[:])
}

func DeserializeRow(src []byte, dest *Row) {
	dest.ID = GetUint32(src[0:4])
	copy(dest.Username[:], src[4:4+32])
	copy(dest.Email[:], src[4+32:4+32+255])
}

// TrimNull убирает нулевые байты (\0) в конце
func TrimNull(b []byte) string {
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}

// PutUint32 / GetUint32 — для чтения/записи ID в little-endian
func PutUint32(buf []byte, x uint32) {
	buf[0] = byte(x)
	buf[1] = byte(x >> 8)
	buf[2] = byte(x >> 16)
	buf[3] = byte(x >> 24)
}

func GetUint32(buf []byte) uint32 {
	return uint32(buf[0]) |
		uint32(buf[1])<<8 |
		uint32(buf[2])<<16 |
		uint32(buf[3])<<24
}
