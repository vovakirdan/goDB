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
	numRows uint32
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
		numRows: numRows,
	}
	return t, nil
}

// DbClose сбрасывает (flush) все страницы на диск и закрывает файл (аналог db_close)
func DbClose(t *Table) error {
	pager := t.pager

	// Полных страниц:
	numFullPages := t.numRows / rowsPerPage

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
	additionalRows := t.numRows % rowsPerPage
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
			n, err := p.file.ReadAt(p.pages[pageNum], offset)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("read error at page %d: %v", pageNum, err)
			}
			// n прочитанных байт, если меньше PageSize — значит страница частично заполнена
			_ = n
		}
	}

	return p.pages[pageNum], nil
}

// rowSlot вычисляет, где (в какой странице) лежит строка rowNum, и возвращает срез
func rowSlot(t *Table, rowNum uint32) ([]byte, error) {
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

// InsertRow добавляет новую строку в таблицу (аналог execute_insert)
func (t *Table) InsertRow(id uint32, username, email string) error {
	if t.numRows >= tableMaxRows {
		return errors.New("table is full")
	}

	// Готовим Row
	var r Row
	r.ID = id
	copy(r.Username[:], []byte(username))
	copy(r.Email[:], []byte(email))

	// Находим ячейку
	slot, err := rowSlot(t, t.numRows)
	if err != nil {
		return err
	}
	// Сериализуем
	serializeRow(&r, slot)

	// Увеличиваем счётчик
	t.numRows++
	return nil
}

// SelectAll — читает все строки по порядку, печатает их
func (t *Table) SelectAll() {
	var r Row
	for i := uint32(0); i < t.numRows; i++ {
		slot, err := rowSlot(t, i)
		if err != nil {
			fmt.Printf("Error reading row %d: %v\n", i, err)
			return
		}
		deserializeRow(slot, &r)
		printRow(&r)
	}
}

// printRow выводит строку в формате "(id, username, email)"
func printRow(r *Row) {
	uname := trimNull(r.Username[:])
	email := trimNull(r.Email[:])
	fmt.Printf("(%d, %s, %s)\n", r.ID, uname, email)
}

// serializeRow / deserializeRow: копируют поля Row в байтовый срез и обратно
func serializeRow(source *Row, dest []byte) {
	// ID (4 байта, little-endian)
	putUint32(dest[0:4], source.ID)
	// username (32 байт)
	copy(dest[4:4+32], source.Username[:])
	// email (255 байт)
	copy(dest[4+32:4+32+255], source.Email[:])
}

func deserializeRow(src []byte, dest *Row) {
	dest.ID = getUint32(src[0:4])
	copy(dest.Username[:], src[4:4+32])
	copy(dest.Email[:], src[4+32:4+32+255])
}

// trimNull убирает нулевые байты (\0) в конце
func trimNull(b []byte) string {
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}

// putUint32 / getUint32 — для чтения/записи ID в little-endian
func putUint32(buf []byte, x uint32) {
	buf[0] = byte(x)
	buf[1] = byte(x >> 8)
	buf[2] = byte(x >> 16)
	buf[3] = byte(x >> 24)
}

func getUint32(buf []byte) uint32 {
	return uint32(buf[0]) |
		uint32(buf[1])<<8 |
		uint32(buf[2])<<16 |
		uint32(buf[3])<<24
}
