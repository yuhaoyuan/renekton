package main

/*
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

type Pager struct {
	fileDescriptor *os.File
	fileLength     int
	Pages          [TABLE_MAX_PAGES]*[]byte
}

func getPage(pager *Pager, pageIndex int) error {
	if pageIndex > TABLE_MAX_PAGES {
		fmt.Println("getPage pageNum too large")
		return errors.New("getPage pageNum too large")
	}
	if pager.Pages[pageIndex] == nil {
		// miss cache
		tempPage := make([]byte, PAGE_SIZE)
		numPages := pager.fileLength / PAGE_SIZE

		if (pager.fileLength % PAGE_SIZE) > 0 {
			numPages++
		}

		if pageIndex <= numPages {

		}
		pager.Pages[pageIndex] = &tempPage
	}
	return nil
}

type Table struct {
	NumRows int32
	Pager   *Pager
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		dbClose(table)
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
		fmt.Println("id = ", row.Id)
		fmt.Println("UserName = ", string(row.UserName))
		fmt.Println("UserName = ", string(row.Email))
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

func serializeRow(source *Row, page *[]byte, offset int) {
	//
	IdStr := strconv.Itoa(int(source.Id))
	IdLength := len(IdStr)
	IdLengthStr := strconv.Itoa(IdLength)

	copy((*page)[offset+ID_OFFSET:], IdLengthStr)
	copy((*page)[offset+ID_OFFSET+1:], IdStr)

	copy((*page)[offset+USERNAME_OFFSET:], source.UserName)
	copy((*page)[offset+EMAIL_OFFSET:], source.Email)
}

func deserializeRow(source *[]byte, offset int, destination *Row) {
	idLengthStr := (*source)[offset+ID_OFFSET : offset+ID_OFFSET+1]
	idLength, err := strconv.ParseInt(string(idLengthStr), 10, 64)
	if err != nil {
		fmt.Println("deserializeRow parse failed, err = ", err)
		os.Exit(0)
	}
	idStr := (*source)[offset+ID_OFFSET+1 : offset+ID_OFFSET+1+int(idLength)]
	idInt, err := strconv.ParseInt(string(idStr), 10, 64)
	if err != nil {
		fmt.Println("deserializeRow parse failed, err = ", err)
		os.Exit(0)
	}
	destination.Id = int32(idInt)

	destination.UserName = (*source)[offset+USERNAME_OFFSET : offset+USERNAME_OFFSET+USERNAME_SIZE]
	destination.Email = (*source)[offset+EMAIL_OFFSET : offset+EMAIL_OFFSET+EMAIL_SIZE]
}

const (
	PAGE_SIZE       = int(4096)
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

func rowSlot(table *Table, rowTh int) (*[]byte, int) {
	pageTh := rowTh / ROWS_PER_PAGE // 定位page
	var err error
	err = getPage(table.Pager, pageTh)
	if err != nil {
		fmt.Println("rowSlot getPage failed, err = ", err)
	}
	rowOffset := rowTh % ROWS_PER_PAGE    // 定位在此页中的行数
	rowByteOffset := rowOffset * ROW_SIZE // 计算此行开始的位置
	return table.Pager.Pages[pageTh], rowByteOffset
}

func pagerOpen(fileName string) *Pager {
	exPath, _ := os.Getwd()
	filePath := exPath + "/" + fileName
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Unable to open file")
		os.Exit(0)
	}
	fileLength, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		fmt.Println("file.Seek error, err=", err)
		os.Exit(0)
	}

	pager := &Pager{
		fileDescriptor: file,
		fileLength:     int(fileLength),
	}

	return pager
}

func dbOpen(fileName string) *Table {
	pager := pagerOpen(fileName)
	numRows := pager.fileLength / ROW_SIZE
	return &Table{
		NumRows: int32(numRows),
		Pager:   pager,
	}
}

func pagerFlush(pager *Pager, pageTh int) {
	if pager.Pages[pageTh] == nil {
		fmt.Println("Tried to flush null page")
		os.Exit(0)
	}

	_, err := pager.fileDescriptor.Seek(int64(pageTh*PAGE_SIZE), io.SeekCurrent)
	if err != nil {

	}
	_, err = pager.fileDescriptor.Write(*pager.Pages[pageTh])
	if err != nil {

	}
}

func dbClose(table *Table) {
	for i := 0; i < int(table.NumRows)/ROWS_PER_PAGE; i++ {
		pagerFlush(table.Pager, i)
	}
	_ = table.Pager.fileDescriptor.Close()
}
