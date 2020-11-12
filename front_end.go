package main

import "C"
import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

type MetaCommandResult int
type PrepareResult int
type StatementType int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_EXIT
	META_COMMAND_UNRECOGNIZED_COMMAND
)
const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_NEGATIVE_ID
	PREPARE_STRING_TOO_LONG
	PREPARE_UNRECOGNIZED_STATEMENT
	PREPARE_SYNTAX_ERROR
)
const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Row struct {
	Id       uint32
	UserName []byte
	Email    []byte
}

func printRow(row *Row) {
	fmt.Println(row.Id)
	fmt.Println(row.UserName)
	fmt.Println(row.Email)
}

type Statement struct {
	SType       StatementType
	RowToInsert Row // 仅适用于insert语句
}

type InputBuffer struct {
	buffer       string
	bufferLength int
	inputLength  int
}

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		"",
		0,
		0,
	}
}
func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.Contains(inputBuffer.buffer, "insert") {
		statement.SType = STATEMENT_INSERT
		args := strings.Split(inputBuffer.buffer, " ")
		if len(args) < 4 {
			return PREPARE_SYNTAX_ERROR
		}
		defer func() {
			if err := recover(); err != nil {
			}
		}()
		idInt32, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("strconv id error ", err)
			return PREPARE_SYNTAX_ERROR
		}
		if idInt32 < 0 {
			return PREPARE_NEGATIVE_ID
		}
		if len(args[2]) > COLUMN_USERNAME_SIZE {
			return PREPARE_STRING_TOO_LONG
		}
		if len(args[3]) > COLUMN_EMAIL_SIZE {
			return PREPARE_STRING_TOO_LONG
		}

		statement.RowToInsert.Id = uint32(idInt32)
		statement.RowToInsert.UserName = []byte(args[2])
		statement.RowToInsert.Email = []byte(args[3])
		return PREPARE_SUCCESS
	} else if strings.Contains(inputBuffer.buffer, "select") {
		statement.SType = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func printPrompt() {
	fmt.Println("这是一段提示语")
}

func readInput(inputBuffer *InputBuffer) error {
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	fmt.Println("收到的指令为： ", input)
	if len(input) == 0 {
		fmt.Println("未读到您输入的指令")
		return errors.New("no input")
	}
	inputBuffer.inputLength = len(input) - 1
	inputBuffer.buffer = input
	return nil
}
