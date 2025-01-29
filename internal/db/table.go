package db

import (
	"fmt"
	"io"
	"os"
	"unsafe"
)

// ---------------------------
// ROW и предыдущие вещи
// ---------------------------

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
const (
	idSize       = 4                                 // uint32
	usernameSize = 32                                // [32]byte
	emailSize    = 255                               // [255]byte
	rowSize      = idSize + usernameSize + emailSize // = 291
)

// ---------------------------
// B-Tree Node Layout constants
// ---------------------------

// NodeType (NODE_INTERNAL=1 / NODE_LEAF=2) — или iota
type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

// Common Node Header Layout
const (
	NodeTypeSize         = 1 // byte
	NodeTypeOffset       = 0
	IsRootSize           = 1 // byte
	IsRootOffset         = NodeTypeOffset + NodeTypeSize
	ParentPointerSize    = 4 // uint32
	ParentPointerOffset  = IsRootOffset + IsRootSize
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize
)

// Leaf Node Header Layout
const (
	LeafNodeNumCellsSize   = 4 // uint32
	LeafNodeNumCellsOffset = CommonNodeHeaderSize
	LeafNodeHeaderSize     = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

// Leaf Node Body Layout
const (
	LeafNodeKeySize     = 4 // uint32
	LeafNodeKeyOffset   = 0
	LeafNodeValueSize   = rowSize
	LeafNodeValueOffset = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize    = LeafNodeKeySize + LeafNodeValueSize

	LeafNodeSpaceForCells = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells      = LeafNodeSpaceForCells / LeafNodeCellSize
)

// ---------------------------
// Pager & Table
// ---------------------------

type Pager struct {
	file       *os.File
	fileLength int64
	numPages   uint32
	pages      [TableMaxPages][]byte
}

type Table struct {
	pager       *Pager
	rootPageNum uint32
}

// Cursor указывает на (page_num, cell_num) внутри B-Tree
type Cursor struct {
	Table      *Table
	PageNum    uint32
	CellNum    uint32
	EndOfTable bool
}

// ---------------------------
// Функции для работы с узлами
// ---------------------------

// leafNodeNumCells возвращает указатель на поле "количество ячеек" в leaf
func leafNodeNumCells(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LeafNodeNumCellsOffset]))
}

// leafNodeCell возвращает указатель на начало cell
func leafNodeCell(node []byte, cellNum uint32) []byte {
	start := LeafNodeHeaderSize + cellNum*LeafNodeCellSize
	return node[start : start+LeafNodeCellSize]
}

// leafNodeKey возвращает указатель на key внутри cell
func leafNodeKey(node []byte, cellNum uint32) *uint32 {
	cell := leafNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cell[LeafNodeKeyOffset]))
}

// leafNodeValue возвращает срез, где лежит сериализованный Row
func leafNodeValue(node []byte, cellNum uint32) []byte {
	cell := leafNodeCell(node, cellNum)
	return cell[LeafNodeValueOffset : LeafNodeValueOffset+LeafNodeValueSize]
}

// initializeLeafNode инициализирует пустой лист
func initializeLeafNode(node []byte) {
	*leafNodeNumCells(node) = 0
}

// leafNodeInsert вставляет (key, row) в позицию cursor.CellNum
// пока не умеем split (если full — ошибка)
func leafNodeInsert(c *Cursor, key uint32, row *Row) error {
	page, err := getPage(c.Table.pager, c.PageNum)
	if err != nil {
		return err
	}
	node := page

	numCells := *leafNodeNumCells(node)
	if numCells >= LeafNodeMaxCells {
		// Узел заполнен, сплит не реализован
		return fmt.Errorf("need to implement splitting a leaf node")
	}

	// Сдвигаем ячейки вправо, если вставка не в конец
	if c.CellNum < numCells {
		for i := numCells; i > c.CellNum; i-- {
			src := leafNodeCell(node, i-1)
			dst := leafNodeCell(node, i)
			copy(dst, src)
		}
	}

	// Увеличиваем счётчик ячеек
	*leafNodeNumCells(node) += 1

	// Заполняем новую ячейку: key + сериализованный Row
	*leafNodeKey(node, c.CellNum) = key
	destValue := leafNodeValue(node, c.CellNum)
	serializeRow(row, destValue)

	return nil
}

// ---------------------------
// DbOpen / DbClose
// ---------------------------

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

	// Определяем, сколько полных страниц в файле
	pager.numPages = uint32(fileLength / PageSize)
	if fileLength%PageSize != 0 {
		return nil, fmt.Errorf("db file is not a whole number of pages. Corrupt file")
	}

	// Заполняем pages[i] = nil
	for i := 0; i < TableMaxPages; i++ {
		pager.pages[i] = nil
	}

	t := &Table{
		pager:       pager,
		rootPageNum: 0, // для упрощения
	}

	// Если файл пуст, инициализируем страницу 0 как пустой лист
	if pager.numPages == 0 {
		page0, _ := getPage(pager, 0)
		initializeLeafNode(page0)
		pager.numPages = 1
	}

	return t, nil
}

func DbClose(t *Table) error {
	p := t.pager
	// Запишем все страницы
	for i := uint32(0); i < p.numPages; i++ {
		if p.pages[i] == nil {
			continue
		}
		err := pagerFlush(p, i)
		if err != nil {
			return err
		}
		p.pages[i] = nil
	}
	err := p.file.Close()
	if err != nil {
		return fmt.Errorf("error closing file: %v", err)
	}
	return nil
}

// ---------------------------
// Pager Helpers
// ---------------------------

func getPage(p *Pager, pageNum uint32) ([]byte, error) {
	if pageNum >= TableMaxPages {
		return nil, fmt.Errorf("page %d out of bounds (max %d)", pageNum, TableMaxPages)
	}
	// Если страница не в кэше, загружаем
	if p.pages[pageNum] == nil {
		p.pages[pageNum] = make([]byte, PageSize)

		// Если страница внутри файла, читаем
		if pageNum < p.numPages {
			offset := int64(pageNum) * PageSize
			n, err := p.file.ReadAt(p.pages[pageNum], offset)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("read page %d error: %v (n=%d)", pageNum, err, n)
			}
		}
	}
	return p.pages[pageNum], nil
}

func pagerFlush(p *Pager, pageNum uint32) error {
	if p.pages[pageNum] == nil {
		return fmt.Errorf("flush null page %d", pageNum)
	}
	offset := int64(pageNum) * PageSize
	n, err := p.file.WriteAt(p.pages[pageNum], offset)
	if err != nil {
		return fmt.Errorf("write error page %d: %v", pageNum, err)
	}
	if n < PageSize {
		return fmt.Errorf("did not write full page: %d/%d", n, PageSize)
	}
	return nil
}

// ---------------------------
// Cursor logic
// ---------------------------

// TableStart: курсор на начало (page=rootPageNum, cell=0)
func TableStart(tbl *Table) *Cursor {
	c := &Cursor{
		Table:      tbl,
		PageNum:    tbl.rootPageNum,
		CellNum:    0,
		EndOfTable: false,
	}
	page, _ := getPage(tbl.pager, c.PageNum)
	numCells := *leafNodeNumCells(page)
	if numCells == 0 {
		c.EndOfTable = true
	}
	return c
}

// TableEnd: курсор на "конец" (после последнего cell)
func TableEnd(tbl *Table) *Cursor {
	c := &Cursor{
		Table:      tbl,
		PageNum:    tbl.rootPageNum,
		EndOfTable: true,
	}
	page, _ := getPage(tbl.pager, c.PageNum)
	numCells := *leafNodeNumCells(page)
	c.CellNum = numCells
	return c
}

// CursorValue: возвращает указатель (slice) на текущую строку
func CursorValue(c *Cursor) ([]byte, error) {
	page, err := getPage(c.Table.pager, c.PageNum)
	if err != nil {
		return nil, err
	}
	// leafNodeValue
	return leafNodeValue(page, c.CellNum), nil
}

// CursorAdvance: двигаем курсор вперёд на 1
func CursorAdvance(c *Cursor) {
	page, _ := getPage(c.Table.pager, c.PageNum)
	numCells := *leafNodeNumCells(page)
	c.CellNum++
	if c.CellNum >= numCells {
		c.EndOfTable = true
	}
}

// ---------------------------
// Serialization of Row
// ---------------------------

// SerializeRow копирует поля Row в dest
func serializeRow(source *Row, dest []byte) {
	putUint32(dest[0:4], source.ID)
	copy(dest[4:4+32], source.Username[:])
	copy(dest[4+32:4+32+255], source.Email[:])
}

// DeserializeRow копирует из src в структуру Row
func deserializeRow(src []byte, dest *Row) {
	dest.ID = getUint32(src[0:4])
	copy(dest.Username[:], src[4:4+32])
	copy(dest.Email[:], src[4+32:4+32+255])
}

// putUint32 / getUint32 — для ID
func putUint32(buf []byte, x uint32) {
	buf[0] = byte(x)
	buf[1] = byte(x >> 8)
	buf[2] = byte(x >> 16)
	buf[3] = byte(x >> 24)
}
func getUint32(buf []byte) uint32 {
	return uint32(buf[0]) |
		(uint32(buf[1]) << 8) |
		(uint32(buf[2]) << 16) |
		(uint32(buf[3]) << 24)
}

// PrintRow для отладки
func PrintRow(r *Row) {
	uname := trimNull(r.Username[:])
	email := trimNull(r.Email[:])
	fmt.Printf("(%d, %s, %s)\n", r.ID, uname, email)
}

func trimNull(b []byte) string {
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}
