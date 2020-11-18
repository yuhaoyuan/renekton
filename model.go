package main

import "os"

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	pagesCount     uint32
	Pages          [TABLE_MAX_PAGES]*Page
}

type Table struct {
	rootPageCTh uint32
	Pager       *Pager
}

type Cursor struct {
	Table      *Table
	PageTh     uint32
	CellTh     uint32
	EndOfTable bool
}

func (c *Cursor) advance() {
	page, _ := getPage(c.Table.Pager, c.PageTh)

	// 先往下一行移动
	c.CellTh += 1
	if c.CellTh >= page.LeafNodeGetCellsCount() {
		// 往下一个节点移动
		nextLeafNodeTh := page.LeafNodeGetNextLeaf()
		if nextLeafNodeTh == 0 {
			c.EndOfTable = true
			return
		} else {
			c.PageTh = nextLeafNodeTh
			c.CellTh = 0
		}
	}
}

type Page struct {
	pageLength uint32 // 维护当前切片最大长度
	data *[]byte // 包括meta信息+cells信息
}

func (p *Page) GetNodeMaxKey() uint32 {
	switch getNodeType(p) {
	case NODE_INTERNAL:
		return p.InternalNodeGetKey(p.InternalNodeGetKeyCount() - 1)
	case NODE_LEAF:
		return p.LeafNodeGetKey(p.LeafNodeGetCellsCount() - 1)
	}
	return 0
}

// get父节点
func (p *Page) LeafNodeGetParent() uint32 {
	offset := PARENT_POINTER_OFFSET
	parentByte := (*p.data)[offset : offset+PARENT_POINTER_SIZE]

	return ByteToNumber(parentByte)
}

// 更新父节点
func (p *Page) LeafNodeSetParent(parentPageTh uint32) {
	offset := PARENT_POINTER_OFFSET

	parentByte := NumberToByte(parentPageTh)

	copy((*p.data)[offset:offset+PARENT_POINTER_SIZE], parentByte[:])
}
