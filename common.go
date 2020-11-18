package main

import (
	"fmt"
	"math"
	"os"
)

// 磁盘文件偏移量
const (
	ID_SIZE       = uint32(4)
	USERNAME_SIZE = uint32(32)
	EMAIL_SIZE    = uint32(255)

	ID_OFFSET       = uint32(0)
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE

	ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

const (
	NODE_LEAF     = uint8(1) //  叶子节点
	NODE_INTERNAL = uint8(2) // 内部节点

	NODE_TYPE_SIZE   = uint32(1)
	NODE_TYPE_OFFSET = uint32(0)

	IS_ROOT_SIZE   = uint32(1)
	IS_ROOT_OFFSET = NODE_TYPE_SIZE

	PARENT_POINTER_SIZE   = uint32(4)
	PARENT_POINTER_OFFSET = NODE_TYPE_SIZE + IS_ROOT_SIZE

	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

// 分割叶子节点常量
const (
	LEAF_NODE_SPLIT_FLAG = (LEAF_NODE_MAX_CELLS + 1) / 2
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

// 复制两个节点内容
func pageCopy(destination *Page, source *Page) {
	copy((*destination.data)[:PAGE_SIZE], (*source.data)[:PAGE_SIZE])
}

func getNodeType(page *Page) byte {
	offset := NODE_TYPE_OFFSET
	return (*page.data)[offset]
}

func setNodeType(page *Page, nodeType byte) {
	(*page.data)[NODE_TYPE_OFFSET] = nodeType
}

func printConstants() {
	fmt.Println("ROW_SIZE: ", ROW_SIZE)
	fmt.Println("COMMON_NODE_HEADER_SIZE: ", COMMON_NODE_HEADER_SIZE)
	fmt.Println("LEAF_NODE_HEADER_SIZE: ", LEAF_NODE_HEADER_SIZE)
	fmt.Println("LEAF_NODE_CELL_SIZE: ", LEAF_NODE_CELL_SIZE)
	fmt.Println("LEAF_NODE_SPACE_FOR_CELLS: ", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Println("LEAF_NODE_MAX_CELLS: ", LEAF_NODE_MAX_CELLS)
}

func leafNodeInsert(cursor *Cursor, keyByte []byte, value *Row) {
	page, err := getPage(cursor.Table.Pager, cursor.PageTh)
	if err != nil {
		fmt.Println("leafNodeInsert getPage failed. err = ", err)
		os.Exit(0)
	}
	cellCount := page.LeafNodeGetCellsCount()
	if cellCount+1 > LEAF_NODE_MAX_CELLS {
		// 分割叶子节点
		leafNodeSplitAndInsert(cursor, keyByte, value)
		return
	}

	if cursor.CellTh < cellCount {
		// 从第i位开始将cells整体往右移动一个单位
		for i := cellCount; i > cursor.CellTh; i-- {
			// 将i-1 复制给i
			page.LeafNodeMoveCell(i, i-1)
		}
	}
	page.LeafNodeAddCellsCount()
	page.LeafNodeSetKey(cursor.CellTh, keyByte)

	serializeRow(value, page, cursor.CellTh)
}

// 初始化内部节点
func initializeInternalNode(node *Page) {
	tempData := make([]byte, PAGE_SIZE)
	*node.data = tempData
	//node.pageLength = PAGE_SIZE

	setNodeRoot(node, false)
	setNodeType(node, NODE_INTERNAL)
}

// 叶子节点分割
func leafNodeSplitAndInsert(cursor *Cursor, keyByte []byte, value *Row) {
	oldNode, err := getPage(cursor.Table.Pager, cursor.PageTh)
	if err != nil {
		fmt.Println("leafNodeSplitAndInsert failed")
		os.Exit(0)
	}

	oldMaxKey := oldNode.GetNodeMaxKey()
	newPageTh := getUnusedPageTh(cursor.Table.Pager)
	newNode, err := getPage(cursor.Table.Pager, newPageTh)
	if err != nil {
		fmt.Println("leafNodeSplitAndInsert get new Page failed")
		os.Exit(0)
	}
	newNode.initializeLeafNode()

	// 父节点更新
	newNode.LeafNodeSetParent(oldNode.LeafNodeGetParent())

	// 分割叶子节点时， old->old_next_leaf = old->new>old_next_leaf
	newNode.LeafNodeSetNextLeaf(oldNode.LeafNodeGetNextLeaf())
	oldNode.LeafNodeSetNextLeaf(newPageTh)

	// 分割oldNode: flag右边cell移动到newNode，左边的cell保留. flag = 原长度+新节点(1) / 2
	var desNode *Page
	i := LEAF_NODE_MAX_CELLS
	for { // 不是LEAF_NODE_MAX_CELLS是因为需要从最后一个开始移动
		if i > LEAF_NODE_SPLIT_FLAG {
			desNode = newNode
		} else {
			desNode = oldNode
		}
		desCellTh := i % (LEAF_NODE_SPLIT_FLAG + 1) // 分割后的cellTh

		if i == cursor.CellTh {
			// 新插入的cell应该写入第i个cell处
			serializeRow(value, desNode, desCellTh)
			desNode.LeafNodeSetKey(desCellTh, keyByte)
			desNode.LeafNodeAddCellsCount()
		} else if i > cursor.CellTh {
			// 在新节点index后面的节点需要往后移动一个cell距离, 即 原来的第cellTh是现在的第cellTh+1
			tempCell := oldNode.LeafNodeGetCell(i - 1)
			desNode.LeafNodeSetCell(desCellTh, tempCell)
			// 新节点count + 1
			desNode.LeafNodeAddCellsCount()
			oldNode.LeafNodeSubCellsCount()
		} else {
			//新节点前面的节点正常移动
			tempCell := oldNode.LeafNodeGetCell(i)
			desNode.LeafNodeSetCell(desCellTh, tempCell)
		}
		if i == 0 {
			break
		} else {
			i--
		}
	}

	// 如果oldNode是根节点，则还需要创建新的根节点. 否则更新父节点即可.
	if isNodeRoot(oldNode) {
		createNewRoot(cursor.Table, newPageTh)
	} else {
		// update parent
		parentPageTh := oldNode.LeafNodeGetParent()
		newMaxKey := oldNode.GetNodeMaxKey()

		parentNode, err := getPage(cursor.Table.Pager, parentPageTh)
		if err != nil {
			os.Exit(0)
		}
		parentNode.InternalNodeUpdateKey(oldMaxKey, newMaxKey)

		internalNodeInsert(cursor.Table, parentPageTh, newPageTh)
	}
}

// 引入删除之后需要做内存管理
func getUnusedPageTh(pager *Pager) uint32 {
	return pager.pagesCount
}

// 判断是否为root节点
func isNodeRoot(node *Page) bool {
	flag := (*node.data)[IS_ROOT_OFFSET : IS_ROOT_OFFSET+IS_ROOT_SIZE][0]
	if flag == uint8(1) {
		return true
	}
	return false
}

func setNodeRoot(node *Page, flag bool) {
	value := uint8(0)
	if flag {
		value = uint8(1)
	}
	(*node.data)[IS_ROOT_OFFSET] = value
}

func createNewRoot(table *Table, rightChildPageTh uint32) {
	/*
		处理分裂根:
		1 旧根复制到新的页面，成为左子节点。
		2 右子结点的地址传入。
		3 重新初始化根页面以包含新的根节点。
		4 新根节点指向两个子节点。
	*/
	root, err := getPage(table.Pager, table.rootPageCTh)
	if err != nil {
		PrintError(fmt.Sprintf("createNewRoot failed, err=%s", err.Error()))
	}

	_, err = getPage(table.Pager, rightChildPageTh)
	if err != nil {
		PrintError(fmt.Sprintf("create rightChildNode failed, err=%s", err.Error()))
	}
	leftChildPageTh := getUnusedPageTh(table.Pager)
	leftChildNode, err := getPage(table.Pager, leftChildPageTh)
	if err != nil {
		PrintError(fmt.Sprintf("create leftChildNode failed, err=%s", err.Error()))
		os.Exit(0)
	}
	pageCopy(leftChildNode, root)
	setNodeRoot(leftChildNode, false)

	// root 节点将变成一个新的root节点(1个key和两个子节点)
	initializeInternalNode(root)
	setNodeRoot(root, true)
	root.InternalNodeSetKeyCount(1)
	root.InternalNodeSetChild(0, leftChildPageTh)

	leftChildMaxKey := leftChildNode.GetNodeMaxKey()
	root.InternalNodeSetKey(0, leftChildMaxKey)
	root.InternalNodeSetRightChild(rightChildPageTh)

	leftChildNode.LeafNodeSetNextLeaf(rightChildPageTh)
}

//func printTree(){
//	for {
//		if getNodeType(page) == NODE_LEAF{
//			break
//		}
//		leftChildPageTh := page.InternalNodeGetChild(0)
//		page = cursor.Table.Pager.Pages[leftChildPageTh]
//	}
//	// page 已经是最左边的节点
//}

func internalNodeInsert(table *Table, parentPageTh, childPageTh uint32) {
	// 向父节点添加一个新的子节点指针和key
	parentNode, err := getPage(table.Pager, parentPageTh)
	if err != nil {
		os.Exit(0)
	}

	childNode, err := getPage(table.Pager, childPageTh)
	if err != nil {
		os.Exit(0)
	}
	childMaxKey := childNode.GetNodeMaxKey()

	// 新cell添加到第index个
	index := parentNode.internalNodeFindChild(childMaxKey)

	originalKeyCount := parentNode.InternalNodeGetKeyCount()
	parentNode.InternalNodeSetKeyCount(originalKeyCount + 1)
	if originalKeyCount >= INTERNAL_NODE_MAX_CELLS {
		// todo: 分割内部节点
		fmt.Println("Need to implement splitting internal node")
		os.Exit(0)
	}

	// 正常添加

	rightChildPageTh := parentNode.InternalNodeGetRightChild()
	rightChildNode, err := getPage(table.Pager, rightChildPageTh)
	if err != nil {
		os.Exit(0)
	}

	if childMaxKey > rightChildNode.GetNodeMaxKey() {
		// 如果需要放在最右边
		parentNode.InternalNodeSetChild(originalKeyCount, rightChildPageTh) // 将原来放在头部的最右子节点移下来
		parentNode.InternalNodeSetKey(originalKeyCount, rightChildNode.GetNodeMaxKey())
		parentNode.InternalNodeSetRightChild(childPageTh) // 将新的子节点移动至头部

	} else {
		// 需要创建一个新的cell(key+ptr)
		for i := originalKeyCount; i > index; i-- {
			// 向右平移
			destination := parentNode.InternalNodeGetCell(i)
			source := parentNode.InternalNodeGetCell(i - 1)
			parentNode.InternalNodeMove(destination, source)
			// 防止uint32溢出
			if i == 0 {
				break
			}
		}
		// 在第index位置插入新的cell
		parentNode.InternalNodeSetChild(index, childPageTh)
		parentNode.InternalNodeSetKey(index, childMaxKey)

	}
}
