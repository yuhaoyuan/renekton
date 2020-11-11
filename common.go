package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

const (
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
	LEAF_NODE_CELLS_COUNT_SIZE   = 4 // 这意味着我们cell-id 最大是"1111" = 2^4 = 16-1 = 15
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

// 将int转换为二进制字符串
func convertToBin(num int) string {
	s := ""
	if num == 0 {
		return "0"
	}

	for ; num > 0; num /= 2 {
		lsb := num % 2
		s = strconv.Itoa(lsb) + s
	}

	l := len(s)
	if l < 4 {
		for i := 0; i < 4-l; i++ {
			s = fmt.Sprintf("0%s", s)
		}
	}
	return s
}

func convertToInt(data []byte) int {
	result := 0

	for i := 0; i < 4; i++ {
		if data[i] == '1' {
			result += int(math.Pow(2, float64(3-i)))
		}
	}
	return result
}

// 返回此页面的cell数量
func leafNodeGetCellsCount(page *Page) int {
	//  像c语言实现这个函数可以直接取地址内容
	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	tempCellCountStr := (*page.data)[offset : offset+LEAF_NODE_CELLS_COUNT_SIZE]

	cellCount := convertToInt(tempCellCountStr)
	return cellCount
}

func leafNodeAddCellsCount(page *Page) {
	/*
		像c语言实现这个函数可以直接取地址内容+1
		为了用golang表达出相关意思， 这种实现方式实属无奈之举。实际上占用了更多的空间，希望读者能明白

		todo: 尝试用unsafe.Pointer改写一下
	*/
	cellCount := leafNodeGetCellsCount(page)

	if cellCount == 15 {
		fmt.Println("can not leafNodeGetCellsCount, because cell full")
		os.Exit(0)
	}
	cellCount += 1
	newCellCountStr := convertToBin(cellCount)

	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	copy((*page.data)[offset:offset+LEAF_NODE_VALUE_SIZE], newCellCountStr)
}

// 返回指定page(节点)中的指定cell值 （key + value）
func leafNodeGetCell(page *Page, cellTh int) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE

	return (*page.data)[offset : offset+LEAF_NODE_CELL_SIZE]
}

// 返回指定page(节点)中的指定cell值 （key）
func leafNodeGetKey(page *Page, cellTh int) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE
	return (*page.data)[offset : offset+LEAF_NODE_KEY_SIZE]
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
	cellCount := leafNodeGetCellsCount(page)
	if cellCount > LEAF_NODE_MAX_CELLS {
		fmt.Println("Need to implement splitting a leaf node.")
		os.Exit(0)
	}

	if cursor.PageTh < cellCount {
		// 从第i位开始将cells整体往右移动一个单位
		for i := cellCount; i > cursor.PageTh; i-- {
			// 将i-1 复制给i
			leafNodeMove(page, i, i-1)
		}
	}
	leafNodeAddCellsCount(page)
	leafNodeSetKey(page, cursor.PageTh, key)

	offset := LEAF_NODE_HEADER_SIZE + cursor.CellTh*LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE
	serializeRow(value, page, offset)
}

func initializeLeafNode(page *Page) {
	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	copy((*page.data)[offset:offset+LEAF_NODE_VALUE_SIZE], "0000")
}
