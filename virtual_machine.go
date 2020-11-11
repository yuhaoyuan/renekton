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

func PrintError(errMsg string) {
	fmt.Println(errMsg)
	os.Exit(0)
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
				// 因为golang uint8=0 也会读，所以读完了之后，手动处理一下理论长度
				len(tempPage),
				&tempPage,
			}
			if pageIndex >= pager.pagesCount {
				pager.pagesCount = pageIndex + 1
			}
		}
	}
	return pager.Pages[pageIndex], nil
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		dbClose(table)
		return META_COMMAND_EXIT
	} else if inputBuffer.buffer == ".constants" {
		printConstants()
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
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
	page, err := getPage(table.Pager, table.rootPageCTh)
	if err != nil || leafNodeGetCellsCount(page) > LEAF_NODE_MAX_CELLS {
		fmt.Println("executeInsert failed, err = ", err)
		return EXECUTE_TABLE_FULL
	}

	rowToInsert := &statement.RowToInsert
	curSor := tableEnd(table)

	idStr := strconv.FormatInt(int64(rowToInsert.Id), 10)
	leafNodeInsert(curSor, []byte(idStr), rowToInsert)
	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	row := &Row{}
	curSor := tableStart(table)
	for {
		if curSor.EndOfTable == true {
			break
		}
		data := cursorValue(curSor)
		row = deserializeRow(data, 0)
		if row != nil {
			fmt.Println("\n*********************************************")
			fmt.Println(" th row = ", row)
			fmt.Println("id = ", row.Id)
			fmt.Println("UserName = ", string(row.UserName))
			fmt.Println("UserName = ", string(row.Email))
			fmt.Println("*********************************************\n")
		}
		curSor.advance()
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

// 反序列化，将字符串变成数据
func deserializeRow(source []byte, offset int) *Row {
	idLengthStr := source[offset+ID_OFFSET : offset+ID_OFFSET+1]
	if idLengthStr[0] == 0 { // 此处内存为0值
		return nil
	}
	idLength, err := strconv.ParseInt(string(idLengthStr), 10, 64)
	if err != nil {
		fmt.Println("deserializeRow parse failed, err = ", err)
		os.Exit(0)
	}
	idStr := source[offset+ID_OFFSET+1 : offset+ID_OFFSET+1+int(idLength)]
	idInt, err := strconv.ParseInt(string(idStr), 10, 64)
	if err != nil {
		fmt.Println("deserializeRow parse failed, err = ", err)
		os.Exit(0)
	}
	destination := &Row{}
	destination.Id = int32(idInt)

	destination.UserName = source[offset+USERNAME_OFFSET : offset+USERNAME_OFFSET+USERNAME_SIZE]
	destination.Email = source[offset+EMAIL_OFFSET : offset+EMAIL_OFFSET+EMAIL_SIZE]
	return destination
}

const (
	PAGE_SIZE       = int(4096)
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

func cursorValue(curSor *Cursor) []byte {
	page, err := getPage(curSor.Table.Pager, curSor.PageTh)
	if err != nil {
		fmt.Println("cursorValue.getPage failed , err = ", err)
		os.Exit(1)
	}
	value := leafNodeGetValue(page, curSor.CellTh)

	return value
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
	if pager.pagesCount == 0 {
		rooPage, err := getPage(pager, 0)
		if err != nil {
			PrintError(fmt.Sprintf("dbOpen failed, err = %s", err.Error()))
		}
		initializeLeafNode(rooPage)
	}

	return &Table{
		rootPageCTh: 0,
		Pager:       pager,
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
	for i := 0; i <= pager.pagesCount; i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(table.Pager, i)
	}
	_ = table.Pager.fileDescriptor.Close()
}

// 创建一个位于table开始位置的光标
func tableStart(table *Table) *Cursor {
	cursor := &Cursor{
		Table:  table,
		PageTh: table.rootPageCTh,
		CellTh: 0,
	}

	rootPage, err := getPage(table.Pager, table.rootPageCTh)
	if err != nil {
		fmt.Println("tableStart.getPage failed, err = ", err)
		os.Exit(1)
	}
	cursor.EndOfTable = leafNodeGetCellsCount(rootPage) == 0
	return cursor
}

// 将创建一个table末尾的光标
func tableEnd(table *Table) *Cursor {
	cursor := &Cursor{
		Table:  table,
		PageTh: table.rootPageCTh,
	}

	rootPage, err := getPage(table.Pager, table.rootPageCTh)
	if err != nil {
		fmt.Println("tableEnd.getPage failed, err = ", err)
		os.Exit(1)
	}

	cursor.CellTh = leafNodeGetCellsCount(rootPage)
	return cursor
}
