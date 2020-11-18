package main

import (
	"fmt"
	"os"
)

/*
叶子节点的api方法
*/

/*
 * Leaf Node Header Layout
 */
const (
	LEAF_NODE_CELLS_COUNT_SIZE   = uint32(4)
	LEAF_NODE_CELLS_COUNT_OFFSET = COMMON_NODE_HEADER_SIZE

	LEAF_NODE_NEXT_LEAF_SIZE   = uint32(4)
	LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_CELLS_COUNT_OFFSET + LEAF_NODE_CELLS_COUNT_SIZE

	LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_CELLS_COUNT_SIZE + LEAF_NODE_NEXT_LEAF_SIZE
)

/*
 * Leaf Node Body Layout
 */
const (
	LEAF_NODE_KEY_SIZE   = uint32(4)
	LEAF_NODE_KEY_OFFSET = uint32(0)

	LEAF_NODE_VALUE_SIZE   = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE

	LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE

	LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	//LEAF_NODE_MAX_CELLS       = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
	LEAF_NODE_MAX_CELLS = uint32(2) // debug用
)

// 初始化叶子节点
func (p *Page) initializeLeafNode() {
	setNodeRoot(p, false)
	setNodeType(p, NODE_LEAF)
	p.LeafNodeSetNextLeaf(0)
}

// 返回此页面的cell数量
func (p *Page) LeafNodeGetCellsCount() uint32 {
	//  像c语言实现这个函数可以直接取地址内容
	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	tempCellCountStr := (*p.data)[offset : offset+LEAF_NODE_CELLS_COUNT_SIZE]

	cellCount := ByteToNumber(tempCellCountStr)
	return cellCount
}

func (p *Page) LeafNodeAddCellsCount() {
	/*
		像c语言实现这个函数可以直接取地址内容+1
		为了用golang表达出相关意思， 这种实现方式实属无奈之举

		todo: 尝试用unsafe.Pointer改写一下
	*/
	cellCount := p.LeafNodeGetCellsCount()

	if cellCount == LEAF_NODE_MAX_CELLS {
		fmt.Println("can not leafNodeGetCellsCount, because cell full")
		os.Exit(0)
	}
	cellCount += 1
	newCellCountStr := NumberToByte(cellCount)

	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	copy((*p.data)[offset:offset+LEAF_NODE_VALUE_SIZE], newCellCountStr[:])
}

func (p *Page) LeafNodeSubCellsCount() {
	cellCount := p.LeafNodeGetCellsCount()

	if cellCount - 1 == LEAF_NODE_MAX_CELLS {
		fmt.Println("can not leafNodeGetCellsCount, because cell full")
		os.Exit(0)
	}
	if cellCount == 0 {
		return
	}
	cellCount -= 1
	newCellCountStr := NumberToByte(cellCount)

	offset := LEAF_NODE_CELLS_COUNT_OFFSET
	copy((*p.data)[offset:offset+LEAF_NODE_VALUE_SIZE], newCellCountStr[:])
}

// 返回指定page(节点)中的指定cell值 （key + value）
func (p *Page) LeafNodeGetCell(cellTh uint32) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE

	return (*p.data)[offset : offset+LEAF_NODE_CELL_SIZE]
}

// 写入kv至指定Node
func (p *Page) LeafNodeSetCell(cellTh uint32, keyAndValue []byte) {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE

	copy((*p.data)[offset:offset+LEAF_NODE_CELL_SIZE], keyAndValue)
}

// 返回指定page(节点)中的指定cell值 （key）
func (p *Page) LeafNodeGetKey(cellTh uint32) uint32 {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE
	keyStr := (*p.data)[offset : offset+LEAF_NODE_KEY_SIZE]

	key := ByteToNumber(keyStr)
	return key
}

func (p *Page) LeafNodeSetKey(cellTh uint32, newKey []byte) {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE
	copy((*p.data)[offset:offset+LEAF_NODE_KEY_SIZE], newKey)
}

// 返回指定page(节点)中的指定cell值 （value）
func (p *Page) LeafNodeGetValue(cellTh uint32) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellTh*LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE
	return (*p.data)[offset : offset+LEAF_NODE_CELL_SIZE]
}

// 在同一个Node中，将第sourceTh个cell复制到第desTh个cell
func (p *Page) LeafNodeMoveCell(desTh uint32, sourceTh uint32) {
	desThStart := LEAF_NODE_HEADER_SIZE + desTh*LEAF_NODE_CELL_SIZE
	desThEnd := desThStart + LEAF_NODE_CELL_SIZE

	sourceThStart := LEAF_NODE_HEADER_SIZE + sourceTh*LEAF_NODE_CELL_SIZE
	sourceThEnd := sourceThStart + LEAF_NODE_CELL_SIZE

	copy((*p.data)[desThStart:desThEnd], (*p.data)[sourceThStart:sourceThEnd])
}

func (p *Page) LeafNodeSetNextLeaf(pageTh uint32) uint32 {
	offset := LEAF_NODE_NEXT_LEAF_OFFSET

	pageThByte := NumberToByte(pageTh)
	copy((*p.data)[offset:offset+LEAF_NODE_NEXT_LEAF_SIZE], pageThByte[:])
	return pageTh
}

func (p *Page) LeafNodeGetNextLeaf() uint32 {
	offset := LEAF_NODE_NEXT_LEAF_OFFSET
	pageThStr := (*p.data)[offset : offset+LEAF_NODE_NEXT_LEAF_SIZE]

	pageTh := ByteToNumber(pageThStr)
	return pageTh
}
