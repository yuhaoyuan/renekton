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
)

func PrintError(errMsg string) {
	fmt.Println(errMsg)
	os.Exit(0)
}

// 获取已有的节点/申请新的节点
func getPage(pager *Pager, pageIndex uint32) (*Page, error) {
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
			if err != nil && err != io.EOF {
				fmt.Println("fileDescriptor read failed, err = ", err)
				os.Exit(0)
			}
		}
		pager.Pages[pageIndex] = &Page{
			// 因为golang uint8=0 也会读，所以读完了之后，手动处理一下理论长度
			uint32(len(tempPage)),
			&tempPage,
		}
		if pageIndex >= pager.pagesCount {
			pager.pagesCount = pageIndex + 1
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
	EXECUTE_DUPLICATE_KEY
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
	if err != nil {
		os.Exit(0)
	}
	cellsCount := page.LeafNodeGetCellsCount()

	rowToInsert := &statement.RowToInsert
	curSor := tableFind(table, rowToInsert.Id)
	if curSor.CellTh < cellsCount {
		keyAtTh := page.LeafNodeGetKey(uint32(curSor.PageTh))
		if keyAtTh == rowToInsert.Id {
			// 主键冲突
			return EXECUTE_DUPLICATE_KEY
		}
	}

	idStr := NumberToByte(rowToInsert.Id)
	leafNodeInsert(curSor, idStr[:], rowToInsert)
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
			fmt.Println("Email = ", string(row.Email))
			fmt.Println("*********************************************\n")
		}
		curSor.advance()
	}
	return EXECUTE_SUCCESS
}

func serializeRow(source *Row, page *Page, cellTh uint32) {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE

	idStr := NumberToByte(source.Id)
	copy((*page.data)[offset+ID_OFFSET:], idStr[:])

	userNameByte := source.UserName
	userNameLen := len(userNameByte)
	if uint32(userNameLen) < USERNAME_SIZE {
		for i := uint32(0); i < USERNAME_SIZE-uint32(userNameLen); i++ {
			userNameByte = append(userNameByte, uint8(0))
		}
	}
	emailByte := source.Email
	if uint32(len(emailByte)) < EMAIL_SIZE {
		for i := uint32(0); i < EMAIL_SIZE-uint32(userNameLen); i++ {
			emailByte = append(emailByte, uint8(0))
		}
	}

	copy((*page.data)[offset+USERNAME_OFFSET:], userNameByte)
	copy((*page.data)[offset+EMAIL_OFFSET:], emailByte)
	page.pageLength = offset + ROW_SIZE
}

// 反序列化，将字符串变成数据
func deserializeRow(source []byte, offset uint32) *Row {
	idStr := source[offset+ID_OFFSET : offset+ID_OFFSET+ID_SIZE]
	idInt := ByteToNumber(idStr[0:4])

	destination := &Row{}
	destination.Id = idInt
	destination.UserName = source[offset+USERNAME_OFFSET : offset+USERNAME_OFFSET+USERNAME_SIZE]
	destination.Email = source[offset+EMAIL_OFFSET : offset+EMAIL_OFFSET+EMAIL_SIZE]
	return destination
}

const (
	PAGE_SIZE       = uint32(4096)
	TABLE_MAX_PAGES = uint32(100)
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

func cursorValue(curSor *Cursor) []byte {
	page, err := getPage(curSor.Table.Pager, curSor.PageTh)
	if err != nil {
		fmt.Println("cursorValue.getPage failed , err = ", err)
		os.Exit(1)
	}
	value := page.LeafNodeGetValue(curSor.CellTh)

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
		fileLength:     uint32(fileLength),
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
		setNodeRoot(rooPage, true)
	}

	return &Table{
		rootPageCTh: 0,
		Pager:       pager,
	}
}

func pagerFlush(pager *Pager, pageTh uint32) {
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
	for i := uint32(0); i <= pager.pagesCount; i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(table.Pager, i)
	}
	_ = table.Pager.fileDescriptor.Close()
}

// 创建一个位于table开始位置的光标
func tableStart(table *Table) *Cursor {
	cursor := tableFind(table, 0) // 先找到一个最小的叶子节点

	node, err := getPage(table.Pager, cursor.PageTh)
	if err != nil {
		PrintError("tableStart.get 0 Page failed")
		return nil
	}
	cellCount := node.LeafNodeGetCellsCount()
	cursor.EndOfTable = cellCount == 0

	return cursor
}

// 创建一个指向特定位置的光标
func tableFind(table *Table, key uint32) *Cursor {
	rootPage, err := getPage(table.Pager, table.rootPageCTh)
	if err != nil {
		os.Exit(0)
	}
	nodeType := getNodeType(rootPage)
	if nodeType == NODE_LEAF { // 如果是叶子节点对应的page，那么找一个特定的cell
		return leafNodeFind(table, table.rootPageCTh, key)
	}
	return InternalNodeFind(table, table.rootPageCTh, key)
}

func leafNodeFind(table *Table, pageTh uint32, key uint32) *Cursor {
	node, err := getPage(table.Pager, pageTh)
	if err != nil {
		os.Exit(0)
	}
	cellCount := node.LeafNodeGetCellsCount()

	cursor := &Cursor{}
	cursor.Table = table
	cursor.PageTh = uint32(pageTh)

	// 二分
	lIndex := uint32(0)
	rIndex := cellCount
	for lIndex < rIndex {
		mid := (lIndex + rIndex) / 2
		keyAtMid := node.LeafNodeGetKey(mid)
		if keyAtMid == key {
			cursor.PageTh = mid
			return cursor
		}

		if keyAtMid > key {
			rIndex = mid
		} else {
			lIndex = mid + 1
		}
	}
	cursor.CellTh = lIndex
	return cursor
}

// 输出B+树详情
func printTree() {
}
