package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

// ---------------------------
// ROW
// ---------------------------

const (
	PageSize      = 4096
	TableMaxPages = 100
)

type Row struct {
	ID       uint32
	Username [32]byte
	Email    [255]byte
}

// rowSize = 4 + 32 + 255 = 291
const rowSize = 4 + 32 + 255

// ---------------------------
// B-Tree Layout
// ---------------------------

type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

// Common Node Header
const (
	NodeTypeSize         = 1
	NodeTypeOffset       = 0
	IsRootSize           = 1
	IsRootOffset         = NodeTypeOffset + NodeTypeSize
	ParentPointerSize    = 4
	ParentPointerOffset  = IsRootOffset + IsRootSize
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize
)

// Leaf Node Header
const (
	LeafNodeNumCellsSize   = 4
	LeafNodeNumCellsOffset = CommonNodeHeaderSize
	LeafNodeHeaderSize     = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

// Leaf Node Body
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
// Pager / Table
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

type Cursor struct {
	Table      *Table
	PageNum    uint32
	CellNum    uint32
	EndOfTable bool
}

// ---------------------------
// Node "header" and "body" helpers
// ---------------------------

// Return the node's type from the first byte
func getNodeType(node []byte) NodeType {
	return NodeType(node[NodeTypeOffset])
}

func setNodeType(node []byte, t NodeType) {
	node[NodeTypeOffset] = byte(t)
}

// Leaf
func leafNodeNumCells(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LeafNodeNumCellsOffset]))
}

func leafNodeCell(node []byte, cellNum uint32) []byte {
	start := LeafNodeHeaderSize + cellNum*LeafNodeCellSize
	return node[start : start+LeafNodeCellSize]
}

func leafNodeKey(node []byte, cellNum uint32) *uint32 {
	cell := leafNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cell[LeafNodeKeyOffset]))
}

func leafNodeValue(node []byte, cellNum uint32) []byte {
	cell := leafNodeCell(node, cellNum)
	return cell[LeafNodeValueOffset : LeafNodeValueOffset+LeafNodeValueSize]
}

// Init leaf
func initializeLeafNode(node []byte) {
	setNodeType(node, NodeLeaf)
	*leafNodeNumCells(node) = 0
}

// Insert key/value into leaf
func leafNodeInsert(c *Cursor, key uint32, row *Row) error {
	page, err := getPage(c.Table.pager, c.PageNum)
	if err != nil {
		return err
	}
	numCells := *leafNodeNumCells(page)
	if numCells >= LeafNodeMaxCells {
		return errors.New("Need to implement splitting a leaf node.")
	}

	// If not at end, shift cells to the right
	if c.CellNum < numCells {
		for i := numCells; i > c.CellNum; i-- {
			dst := leafNodeCell(page, i)
			src := leafNodeCell(page, i-1)
			copy(dst, src)
		}
	}
	*leafNodeNumCells(page) += 1
	*leafNodeKey(page, c.CellNum) = key
	dest := leafNodeValue(page, c.CellNum)
	serializeRow(row, dest)
	return nil
}

// ---------------------------
// DbOpen / DbClose
// ---------------------------

func DbOpen(filename string) (*Table, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileLen := st.Size()

	p := &Pager{
		file:       f,
		fileLength: fileLen,
	}
	p.numPages = uint32(fileLen / PageSize)
	if fileLen%PageSize != 0 {
		return nil, fmt.Errorf("db file not multiple of page size")
	}

	t := &Table{
		pager:       p,
		rootPageNum: 0,
	}

	// init pages
	for i := 0; i < TableMaxPages; i++ {
		p.pages[i] = nil
	}

	// If no pages, create page0 as leaf
	if p.numPages == 0 {
		page0, _ := getPage(p, 0)
		initializeLeafNode(page0)
		p.numPages = 1
	}
	return t, nil
}

func DbClose(t *Table) error {
	p := t.pager
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
	return p.file.Close()
}

// ---------------------------
// Pager logic
// ---------------------------

func getPage(p *Pager, pageNum uint32) ([]byte, error) {
	if pageNum >= TableMaxPages {
		return nil, fmt.Errorf("page %d out of bounds (max %d)", pageNum, TableMaxPages)
	}
	if p.pages[pageNum] == nil {
		p.pages[pageNum] = make([]byte, PageSize)
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
		return fmt.Errorf("not fully written page %d: wrote %d/%d", pageNum, n, PageSize)
	}
	return nil
}

// ---------------------------
// Cursor logic
// ---------------------------

func TableStart(t *Table) *Cursor {
	// find position for smallest key => cell=0
	cursor := &Cursor{
		Table:      t,
		PageNum:    t.rootPageNum,
		CellNum:    0,
		EndOfTable: false,
	}
	page, _ := getPage(t.pager, t.rootPageNum)
	if *leafNodeNumCells(page) == 0 {
		cursor.EndOfTable = true
	}
	return cursor
}

func CursorValue(c *Cursor) ([]byte, error) {
	page, err := getPage(c.Table.pager, c.PageNum)
	if err != nil {
		return nil, err
	}
	return leafNodeValue(page, c.CellNum), nil
}

func CursorAdvance(c *Cursor) {
	page, _ := getPage(c.Table.pager, c.PageNum)
	numCells := *leafNodeNumCells(page)
	c.CellNum++
	if c.CellNum >= numCells {
		c.EndOfTable = true
	}
}

// ---------------------------
// Searching for Key
// ---------------------------

// tableFind — ищет нужный ключ в tree
func TableFind(t *Table, key uint32) *Cursor {
	rootPage, _ := getPage(t.pager, t.rootPageNum)
	typ := getNodeType(rootPage)
	if typ == NodeLeaf {
		return leafNodeFind(t, t.rootPageNum, key)
	} else {
		// internal not implemented
		panic("searching internal node not implemented")
	}
}

// leafNodeFind — бинарный поиск среди ячеек
func leafNodeFind(t *Table, pageNum, key uint32) *Cursor {
	c := &Cursor{
		Table:   t,
		PageNum: pageNum,
	}

	pg, _ := getPage(t.pager, pageNum)
	numCells := *leafNodeNumCells(pg)

	// binary search
	minIndex := uint32(0)
	onePastMaxIndex := numCells

	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := *leafNodeKey(pg, index)
		if key == keyAtIndex {
			// нашли
			c.CellNum = index
			return c
		} else if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	c.CellNum = minIndex
	return c
}

// ---------------------------
// Row serialization
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
