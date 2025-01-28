package table

import (
	"fmt"
	"unsafe"
	"errors"

    "goDB/internal/types"
)

/*
We define constants:
 - PAGE_SIZE is 4 KB
 - TABLE_MAX_PAGES is some arbitrary limit (100)
 - Row layout uses offsets for id, username, email
*/

/* We store the row in a fixed-size layout*/

func NewTable() *types.Table {
	return &types.Table{
		NumRows: 0,
		Pages:   [types.TABLE_MAX_PAGES][]byte{},
	}
}

func FreeTable(t *types.Table) {
	for i := 0; i < types.TABLE_MAX_PAGES; i++ {
		t.Pages[i] = nil
	}
}

func rowSlot(table *types.Table, rowNum uint32) ([]byte, error) {
	// If rowNum is bigger than total table capacity, return an error
	rowsPerPage := types.PAGE_SIZE / types.ROW_SIZE
	if rowNum >= rowsPerPage*uint32(types.TABLE_MAX_PAGES) {
		return nil, errors.New("row number out of table range")
	}

	pageNum := rowNum / rowsPerPage
	if table.Pages[pageNum] == nil {
		// allocate page on demand
		table.Pages[pageNum] = make([]byte, types.PAGE_SIZE)
	}

	rowOffset := rowNum % rowsPerPage
	byteOffset := rowOffset * types.ROW_SIZE
	return table.Pages[pageNum][byteOffset : byteOffset+types.ROW_SIZE], nil
}

// serializeRow copies the data from Row struct to the compact representation.
func serializeRow(source *types.Row, destination []byte) {
	/* ID */
	copy(destination[types.ID_OFFSET:types.ID_OFFSET+types.ID_SIZE], (*[4]byte)(unsafe.Pointer(&source.ID))[:])

	/* Username */
	copy(destination[types.USERNAME_OFFSET:types.USERNAME_OFFSET+types.USERNAME_SIZE], source.Username[:])

	/* Email */
	copy(destination[types.EMAIL_OFFSET:types.EMAIL_OFFSET+types.EMAIL_SIZE], source.Email[:])
}

// deserializeRow copies raw bytes from memory into the Row struct.
func deserializeRow(source []byte, dest *types.Row) {
	/* ID */
	copy((*[4]byte)(unsafe.Pointer(&dest.ID))[:], source[types.ID_OFFSET:types.ID_OFFSET+types.ID_SIZE])

	/* Username */
	copy(dest.Username[:], source[types.USERNAME_OFFSET:types.USERNAME_OFFSET+types.USERNAME_SIZE])

	/* Email */
	copy(dest.Email[:], source[types.EMAIL_OFFSET:types.EMAIL_OFFSET+types.EMAIL_SIZE])
}

// PrintRow is used by SELECT to show row info in the console.
func PrintRow(r *types.Row) {
	// We must convert the fixed-size arrays to strings, trimming trailing \0s.
	usernameStr := string(r.Username[:])
	usernameStr = trimNulls(usernameStr)

	emailStr := string(r.Email[:])
	emailStr = trimNulls(emailStr)

	fmt.Printf("(%d, %s, %s)\n", r.ID, usernameStr, emailStr)
}

// Helper to trim trailing NULLs from a string
func trimNulls(s string) string {
	return string([]rune(s))
	
	// i := strings.IndexByte(s, 0)
	// if i >= 0 {
	//     return s[:i]
	// }
	// return s
}

// ExecuteInsert puts a new row into the table, if there's space.
func ExecuteInsert(stmt *types.Statement, table *types.Table) types.ExecuteResult {
	// Check if table is full
	rowsPerPage := types.PAGE_SIZE / types.ROW_SIZE
	maxRows := rowsPerPage * uint32(types.TABLE_MAX_PAGES)
	if table.NumRows >= maxRows {
		return types.ExecuteTableFull
	}

	slot, err := rowSlot(table, table.NumRows)
	if err != nil {
		return types.ExecuteTableFull // or handle differently
	}

	serializeRow(&stmt.RowToInsert, slot)
	table.NumRows++

	return types.ExecuteSuccess
}

// ExecuteSelect reads every row in the table and prints them.
func ExecuteSelect(table *types.Table) types.ExecuteResult {
	var row types.Row
	for i := uint32(0); i < table.NumRows; i++ {
		slot, _ := rowSlot(table, i)
		deserializeRow(slot, &row)
		PrintRow(&row)
	}
	return types.ExecuteSuccess
}
