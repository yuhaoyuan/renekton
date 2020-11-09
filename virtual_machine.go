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

type Page struct {
	pageLength int // 维护当前切片最大长度
	data       *[]byte
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     int
	Pages          [TABLE_MAX_PAGES]*Page
}

func getPage(pager *Pager, pageIndex int) (*Page, error) {
	if pageIndex > TABLE_MAX_PAGES {
		fmt.Println("getPage pageNum too large")
		return nil, errors.New("getPage pageNum too large")
	}
	if pager.Pages[pageIndex] == nil {

		// miss cache
		tempPage := make([]byte, PAGE_SIZE)
		pagesCount := pager.fileLength / PAGE_SIZE
		if (pager.fileLength % PAGE_SIZE) > 0 {
			pagesCount++
		}
		// 访问的page位于文件边缘
		if pageIndex <= pagesCount {
			_, err := pager.fileDescriptor.Seek(int64(pageIndex*PAGE_SIZE), io.SeekStart)
			if err != nil {
				fmt.Println("fileDescriptor seek failed, err = ", err)
				os.Exit(0)
			}
			_, err = pager.fileDescriptor.ReadAt(tempPage, int64(pageIndex*PAGE_SIZE)) // 最多读tempPage的长度
			if err != nil {
				if err == io.EOF {
					pager.Pages[pageIndex] = &Page{
						len(tempPage),
						&tempPage,
					}
					return pager.Pages[pageIndex], nil
				}
				fmt.Println("fileDescriptor read failed, err = ", err)
				os.Exit(0)
			}
			pager.Pages[pageIndex] = &Page{
				len(tempPage),
				&tempPage,
			}
		}
		// 因为golang uint8=0 也会读，所以读完了之后，手动处理一下理论长度
	}
	return pager.Pages[pageIndex], nil
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
	row := &Row{}
	for i := 0; i < int(table.NumRows); i++ {
		pagePtr, offset := rowSlot(table, i)
		row = deserializeRow(pagePtr.data, offset)
		if row != nil {
			fmt.Println("\n*********************************************")
			fmt.Println(" th row = ", row)
			fmt.Println("id = ", row.Id)
			fmt.Println("UserName = ", string(row.UserName))
			fmt.Println("UserName = ", string(row.Email))
			fmt.Println("*********************************************\n")
		}
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

	copy((*page.data)[offset+ID_OFFSET:], IdLengthStr)
	copy((*page.data)[offset+ID_OFFSET+1:], IdStr)

	copy((*page.data)[offset+USERNAME_OFFSET:], source.UserName)
	copy((*page.data)[offset+EMAIL_OFFSET:], source.Email)
	page.pageLength = offset + ROW_SIZE
}

func deserializeRow(source *[]byte, offset int) *Row {
	idLengthStr := (*source)[offset+ID_OFFSET : offset+ID_OFFSET+1]
	if idLengthStr[0] == 0 { // 此处内存为0值
		return nil
	}
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
	destination := &Row{}
	destination.Id = int32(idInt)

	destination.UserName = (*source)[offset+USERNAME_OFFSET : offset+USERNAME_OFFSET+USERNAME_SIZE]
	destination.Email = (*source)[offset+EMAIL_OFFSET : offset+EMAIL_OFFSET+EMAIL_SIZE]
	return destination
}

const (
	PAGE_SIZE       = int(4096)
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

func rowSlot(table *Table, rowTh int) (*Page, int) {
	pageTh := rowTh / ROWS_PER_PAGE // 定位page
	var err error
	page, err := getPage(table.Pager, pageTh)
	if err != nil {
		fmt.Println("rowSlot getPage failed, err = ", err)
	}
	rowOffset := rowTh % ROWS_PER_PAGE    // 定位在此页中的行数
	rowByteOffset := rowOffset * ROW_SIZE // 计算此行开始的位置
	return page, rowByteOffset
}

func pagerOpen(fileName string) *Pager {
	exPath, _ := os.Getwd()
	filePath := exPath + "/" + fileName
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666) //0666表示：创建了一个普通文件，所有人拥有对该文件的读、写权限，但是都不可执行
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
		fmt.Println("pager fileDescriptor Seek failed, err = ", err)
		os.Exit(0)
	}

	// 截断
	data := (*pager.Pages[pageTh].data)[:pager.Pages[pageTh].pageLength]
	_, err = pager.fileDescriptor.Write(data)
	if err != nil {
		fmt.Println("pager fileDescriptor Write failed, err = ", err)
		os.Exit(0)
	}
}

func dbClose(table *Table) {
	pager := table.Pager
	for i := 0; i <= int(table.NumRows)/ROWS_PER_PAGE; i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(table.Pager, i)
	}
	_ = table.Pager.fileDescriptor.Close()
}
