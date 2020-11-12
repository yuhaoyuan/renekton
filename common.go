package main

import (
	"fmt"
	"math"
	"os"
)

// 磁盘文件偏移量
const (
	ID_SIZE         = 4
	USERNAME_SIZE   = 32
	EMAIL_SIZE      = 255

	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE

	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

const (
	NODE_LEAF = uint8(0) // 空内容

	NODE_TYPE_SIZE   = 1
	NODE_TYPE_OFFSET = 0

	IS_ROOT_SIZE   = 1
	IS_ROOT_OFFSET = NODE_TYPE_SIZE

	PARENT_POINTER_SIZE   = 4
	PARENT_POINTER_OFFSET = NODE_TYPE_SIZE + IS_ROOT_SIZE

	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

/*
 * Leaf Node Header Layout
 */

const (
	LEAF_NODE_CELLS_COUNT_SIZE   = 4
	LEAF_NODE_CELLS_COUNT_OFFSET = COMMON_NODE_HEADER_SIZE

	LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_CELLS_COUNT_OFFSET
)

/*
 * Leaf Node Body Layout
 */

const (
	LEAF_NODE_KEY_SIZE   = 4
	LEAF_NODE_KEY_OFFSET = 0

	LEAF_NODE_VALUE_SIZE   = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE

	LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE

	LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS       = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
)

// 将一个正整数（32位）分解成内存切片
func NumberToByte(n uint32) [4]byte {
	nStr := [4]byte{}
	nStr[0] = byte(n >> 24)
	nStr[1] = byte(n >> 16 & 0x00ff)
	nStr[2] = byte(n >> 8 & 0x0000ff)
	nStr[3] = byte(n & 0x000000ff)
	return nStr
}

func ByteToNumber(nStr []byte) uint32 {
	if len(nStr) < 4 {
		return 0
	}
	number := uint32(0)
	number += uint32(nStr[0]) * uint32(math.Pow(2, 24))
	number += uint32(nStr[1]) * uint32(math.Pow(2, 16))
	number += uint32(nStr[2]) * uint32(math.Pow(2, 8))
	number += uint32(nStr[3])
	return number
}

// 返回此页面的cell数量
func leafNodeGetCellsCount(page *Page) uint32 {
	//  像c语言实现这个函数可以直接取地址内容
	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	tempCellCountStr := (*page.data)[offset : offset+LEAF_NODE_CELLS_COUNT_SIZE]

	cellCount := ByteToNumber(tempCellCountStr)
	return cellCount
}

func leafNodeAddCellsCount(page *Page) {
	/*
		像c语言实现这个函数可以直接取地址内容+1
		为了用golang表达出相关意思， 这种实现方式实属无奈之举

		todo: 尝试用unsafe.Pointer改写一下
	*/
	cellCount := leafNodeGetCellsCount(page)

	if int(cellCount) == LEAF_NODE_MAX_CELLS {
		fmt.Println("can not leafNodeGetCellsCount, because cell full")
		os.Exit(0)
	}
	cellCount += 1
	newCellCountStr := NumberToByte(cellCount)

	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	copy((*page.data)[offset:offset+LEAF_NODE_VALUE_SIZE], newCellCountStr[:])
}

// 返回指定page(节点)中的指定cell值 （key + value）
func leafNodeGetCell(page *Page, cellTh int) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE

	return (*page.data)[offset : offset+LEAF_NODE_CELL_SIZE]
}

// 返回指定page(节点)中的指定cell值 （key）
func leafNodeGetKey(page *Page, cellTh int) uint32 {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE
	keyStr := (*page.data)[offset : offset+LEAF_NODE_KEY_SIZE]

	key := ByteToNumber(keyStr)
	return key
}

func leafNodeSetKey(page *Page, cellTh int, newKey []byte) {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE
	copy((*page.data)[offset:offset+LEAF_NODE_KEY_SIZE], newKey)
}

// 返回指定page(节点)中的指定cell值 （value）
func leafNodeGetValue(page *Page, cellTh int) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE
	return (*page.data)[offset : offset+LEAF_NODE_CELL_SIZE]
}

func leafNodeMove(page *Page, desTh int, sourceTh int) {
	desThStart := LEAF_NODE_HEADER_SIZE + desTh*LEAF_NODE_CELL_SIZE
	desThEnd := desThStart + LEAF_NODE_CELL_SIZE

	sourceThStart := LEAF_NODE_HEADER_SIZE + sourceTh*LEAF_NODE_CELL_SIZE
	sourceThEnd := sourceThStart + LEAF_NODE_CELL_SIZE

	copy((*page.data)[desThStart:desThEnd], (*page.data)[sourceThStart:sourceThEnd])
}

func getNodeType(page *Page) byte {
	offset := NODE_TYPE_OFFSET
	return (*page.data)[offset]
}

func printConstants() {
	fmt.Println("ROW_SIZE: ", ROW_SIZE)
	fmt.Println("COMMON_NODE_HEADER_SIZE: ", COMMON_NODE_HEADER_SIZE)
	fmt.Println("LEAF_NODE_HEADER_SIZE: ", LEAF_NODE_HEADER_SIZE)
	fmt.Println("LEAF_NODE_CELL_SIZE: ", LEAF_NODE_CELL_SIZE)
	fmt.Println("LEAF_NODE_SPACE_FOR_CELLS: ", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Println("LEAF_NODE_MAX_CELLS: ", LEAF_NODE_MAX_CELLS)
}

func leafNodeInsert(cursor *Cursor, key []byte, value *Row) {
	page, err := getPage(cursor.Table.Pager, cursor.PageTh)
	if err != nil {
		fmt.Println("leafNodeInsert getPage failed. err = ", err)
		os.Exit(0)
	}
	cellCount := int(leafNodeGetCellsCount(page))
	if cellCount > LEAF_NODE_MAX_CELLS {
		fmt.Println("Need to implement splitting a leaf node.")
		os.Exit(0)
	}

	if cursor.CellTh < cellCount {
		// 从第i位开始将cells整体往右移动一个单位
		for i := cellCount; i > cursor.CellTh; i-- {
			// 将i-1 复制给i
			leafNodeMove(page, i, i-1)
		}
	}
	leafNodeAddCellsCount(page)
	leafNodeSetKey(page, cursor.CellTh, key)

	offset := LEAF_NODE_HEADER_SIZE + cursor.CellTh*LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE
	serializeRow(value, page, offset)
}

func initializeLeafNode(page *Page) {
	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	copy((*page.data)[offset:offset+LEAF_NODE_VALUE_SIZE], []byte{})
}
