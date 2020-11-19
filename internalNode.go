package main

import (
	"fmt"
	"os"
)

/*
内部节点api
*/

const (
	/*
	   Internal Node Header Layout
	*/
	INTERNAL_NODE_NUM_KEYS_SIZE   = 4 // sizeof uint32
	INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE

	INTERNAL_NODE_RIGHT_CHILD_SIZE   = 4 // sizeof uint32
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE

	INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

	/*
		Internal Node Body Layout
	*/
	INTERNAL_NODE_KEY_SIZE   = 4 // size of uint32
	INTERNAL_NODE_CHILD_SIZE = 4
	INTERNAL_NODE_CELL_SIZE  = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

	INTERNAL_NODE_MAX_CELLS = 3 // debug用
)

// 获得
func (p *Page) InternalNodeGetKeyCount() uint32 {
	offset := INTERNAL_NODE_NUM_KEYS_OFFSET
	return ByteToNumber((*p.data)[offset : offset+INTERNAL_NODE_NUM_KEYS_SIZE])
}

func (p *Page) InternalNodeSetKeyCount(keyCount uint32) {
	keyByte := NumberToByte(keyCount)

	offset := INTERNAL_NODE_NUM_KEYS_OFFSET
	copy((*p.data)[offset:offset+INTERNAL_NODE_NUM_KEYS_SIZE], keyByte[:])
}

// 获得最右边子节点序号
func (p *Page) InternalNodeGetRightChild() uint32 {
	offset := INTERNAL_NODE_RIGHT_CHILD_OFFSET
	return ByteToNumber((*p.data)[offset : offset+INTERNAL_NODE_RIGHT_CHILD_SIZE])
}

//
func (p *Page) InternalNodeSetRightChild(childTh uint32) {
	offset := INTERNAL_NODE_RIGHT_CHILD_OFFSET
	childThByte := NumberToByte(childTh)
	copy((*p.data)[offset:offset+INTERNAL_NODE_RIGHT_CHILD_SIZE], childThByte[:])
}

// 返回指定page(节点)中的指定cell值 child序号
func (p *Page) InternalNodeGetCell(cellTh uint32) uint32 {
	offset := INTERNAL_NODE_HEADER_SIZE + cellTh*INTERNAL_NODE_CELL_SIZE
	return ByteToNumber((*p.data)[offset : offset+INTERNAL_NODE_CHILD_SIZE])
}

func (p *Page) InternalNodeGetKey(keyTh uint32) uint32 {
	offset := INTERNAL_NODE_HEADER_SIZE + keyTh*INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE
	return ByteToNumber((*p.data)[offset : offset+INTERNAL_NODE_KEY_SIZE])
}

func (p *Page) InternalNodeSetKey(keyTh uint32, key uint32) {
	offset := INTERNAL_NODE_HEADER_SIZE + keyTh*INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE

	keyByte := NumberToByte(key)
	copy((*p.data)[offset:offset+INTERNAL_NODE_KEY_SIZE], keyByte[:])
}

// 获得内部节点的第cellTh个儿子
func (p *Page) InternalNodeGetChild(childTh uint32) uint32 {
	keyCount := p.InternalNodeGetKeyCount()

	if childTh > keyCount {
		fmt.Println("Tried to access child_th > keyCount")
		os.Exit(0)
	} else if childTh == keyCount {
		return p.InternalNodeGetRightChild()
	} else {
		return p.InternalNodeGetCell(childTh)
	}
	return 0
}

func (p *Page) InternalNodeSetChild(childTh uint32, nodeTh uint32) {
	nodeThByte := NumberToByte(nodeTh)

	keyCount := p.InternalNodeGetKeyCount()
	if childTh > keyCount {
		fmt.Println("Tried to access child_th > keyCount")
		os.Exit(0)
	} else if childTh == keyCount {
		offset := INTERNAL_NODE_RIGHT_CHILD_OFFSET
		copy((*p.data)[offset:offset+INTERNAL_NODE_RIGHT_CHILD_SIZE], nodeThByte[:])
	} else {
		offset := INTERNAL_NODE_HEADER_SIZE + childTh*INTERNAL_NODE_CELL_SIZE
		copy((*p.data)[offset:offset+INTERNAL_NODE_CHILD_SIZE], nodeThByte[:])
	}
}

// 通过内部节点时，通过key找到想要的下一个节点子节点
func (p *Page) internalNodeFindChild(key uint32) uint32 {

	keyCount := p.InternalNodeGetKeyCount()

	l := uint32(0)
	r := keyCount

	for l < r {
		mid := (l + r) / 2
		rightKey := p.InternalNodeGetKey(mid) // 第mid个key
		if rightKey >= key {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if p.InternalNodeGetKey(l) < key { // 返回最右边的子节点
		l = p.InternalNodeGetRightChild()
	}

	return l
}

func InternalNodeFind(table *Table, pageTh uint32, key uint32) *Cursor {
	node, err := getPage(table.Pager, pageTh)
	if err != nil {
		os.Exit(0)
	}

	// 内部节点的子节点可能仍然是内部节点
	childIndex := node.internalNodeFindChild(key)
	childTh :=  node.InternalNodeGetChild(childIndex)
	child, err := getPage(table.Pager, childTh)
	if err != nil {
		PrintError("internalNodeFind get child page failed")
	}
	switch getNodeType(child) {
	case NODE_LEAF:
		return leafNodeFind(table, childTh, key)
	case NODE_INTERNAL:
		return InternalNodeFind(table, childTh, key)
	}
	return nil
}

func (p *Page) InternalNodeUpdateKey(oldMaxKey uint32, newMaxKey uint32) {
	oldChildIndex := p.internalNodeFindChild(oldMaxKey)
	p.InternalNodeSetKey(oldChildIndex, newMaxKey)
}

func (p *Page) InternalNodeMove(sourceTh, desTh uint32) {
	sourceOffset := INTERNAL_NODE_HEADER_SIZE + sourceTh*INTERNAL_NODE_CELL_SIZE
	destinationOffset := INTERNAL_NODE_HEADER_SIZE + desTh*INTERNAL_NODE_CELL_SIZE

	copy((*p.data)[destinationOffset:destinationOffset+INTERNAL_NODE_CELL_SIZE], (*p.data)[sourceOffset:sourceOffset+INTERNAL_NODE_CELL_SIZE])
}
