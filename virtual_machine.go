package main

import (
	"fmt"
	"strconv"
)

type Page struct {
	Data [PAGE_SIZE]byte
}

type Table struct {
	NumRows int32
	Pages   [TABLE_MAX_PAGES]*Page
}

func newTable() *Table {
	return &Table{
		NumRows: 0,
	}
}

func doMetaCommand(inputBuffer *InputBuffer) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		return META_COMMAND_EXIT
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
}

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
	EXECUTE_FAILED
)

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.SType {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		return executeSelect(statement, table)
	}
	return EXECUTE_FAILED
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.NumRows >= int32(TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL
	}

	rowToInsert := &statement.RowToInsert
	pagePtr, offset := rowSlot(table, int(table.NumRows))
	serializeRow(rowToInsert, pagePtr, offset)
	table.NumRows += 1
	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	row := Row{}
	for i := 0; i < int(table.NumRows); i++ {
		pagePtr, offset := rowSlot(table, i)
		deserializeRow(pagePtr, offset, &row)
		fmt.Println("\n*********************************************")
		fmt.Println(" th row = ", row)
		fmt.Println("id = ",row.Id)
		fmt.Println("UserName = ",string(row.UserName))
		fmt.Println("UserName = ",string(row.Email))
		fmt.Println("*********************************************\n")
	}
	return EXECUTE_SUCCESS
}

const (
	ID_SIZE         = 10
	USERNAME_SIZE   = 32
	EMAIL_SIZE      = 255
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

func serializeRow(source *Row, page *Page, offset int) {
	//
	IdStr := strconv.Itoa(int(source.Id))
	IdLength := len(IdStr)
	IdLengthStr := strconv.Itoa(IdLength)

	copy(page.Data[offset+ID_OFFSET:], IdLengthStr)
	copy(page.Data[offset+ID_OFFSET+1:], IdStr)

	copy(page.Data[offset+USERNAME_OFFSET:], source.UserName)
	copy(page.Data[offset+EMAIL_OFFSET:], source.Email)
}

func deserializeRow(source *Page, offset int, destination *Row) {
	idLengthStr := source.Data[offset+ID_OFFSET : offset+ID_OFFSET+1]
	idLength, err := strconv.ParseInt(string(idLengthStr), 10, 64)
	if err != nil {

	}
	idStr := source.Data[offset+ID_OFFSET+1:offset+ID_OFFSET+1+int(idLength)]
	idInt, err := strconv.ParseInt(string(idStr), 10, 64)
	if err != nil {

	}
	destination.Id = int32(idInt)

	destination.UserName = source.Data[offset+USERNAME_OFFSET:offset+USERNAME_OFFSET+USERNAME_SIZE]
	destination.Email =  source.Data[offset+EMAIL_OFFSET:offset+EMAIL_OFFSET+EMAIL_SIZE]
}

const (
	PAGE_SIZE       = int(4096)
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

func rowSlot(table *Table, rowTh int) (*Page, int) {
	pageTh := rowTh / ROWS_PER_PAGE // 定位page
	if table.Pages[pageTh] == nil {
		table.Pages[pageTh] = &Page{}
	}
	rowOffset := rowTh % ROWS_PER_PAGE    // 定位在此页中的行数
	rowByteOffset := rowOffset * ROW_SIZE // 计算此行开始的位置
	return table.Pages[pageTh], rowByteOffset
}
