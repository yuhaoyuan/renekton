package main

import "os"

type Page struct {
	pageLength int // 维护当前切片最大长度
	data       *[]byte
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     int
	Pages          [TABLE_MAX_PAGES]*Page
}

type Table struct {
	NumRows int
	Pager   *Pager
}

type Cursor struct {
	Table      *Table
	RowTh      int
	EndOfTable bool
}

func (c *Cursor) advance() {
	c.RowTh += 1
	if c.RowTh == c.Table.NumRows {
		c.EndOfTable = true
	}
}
