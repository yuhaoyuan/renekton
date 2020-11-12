package main

import "os"

type Page struct {
	pageLength int     // 维护当前切片最大长度
	data       *[]byte // 包括meta信息+cells信息
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     int
	pagesCount     int
	Pages          [TABLE_MAX_PAGES]*Page
}

type Table struct {
	rootPageCTh int
	Pager       *Pager
}

type Cursor struct {
	Table      *Table
	PageTh     int
	CellTh     int
	EndOfTable bool
}

func (c *Cursor) advance() {
	page, _ := getPage(c.Table.Pager, c.PageTh)

	c.CellTh += 1
	if c.CellTh == int(leafNodeGetCellsCount(page)) {
		c.EndOfTable = true
	}
}
