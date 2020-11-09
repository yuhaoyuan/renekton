package main

/*
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Println("Must supply a database filename")
		os.Exit(0)
	}
	filename := args[1]
	table := dbOpen(filename)
	inputBuffer := newInputBuffer()

	for {
		printPrompt()
		err := readInput(inputBuffer)
		if err != nil {
			fmt.Println("readInput failed, err = ", err)
			os.Exit(0)
		}
		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer, table) {
			case META_COMMAND_EXIT:
				fmt.Println("exit command!")
				return // 直接退出main
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Println(fmt.Sprintf("Unrecognized command %s", inputBuffer.buffer))
				continue
			}
		}

		statement := Statement{}
		switch prepareStatement(inputBuffer, &statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_NEGATIVE_ID:
			fmt.Println("Syntax error. Could not parse negative id")
		case PREPARE_STRING_TOO_LONG:
			fmt.Println("Syntax error. userName or email is too long")
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Println(fmt.Sprintf("Unrecognized keyword at start of %s.", inputBuffer.buffer))
			continue
		}

		switch executeStatement(&statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed.")
			break
		case EXECUTE_TABLE_FULL:
			fmt.Println("table full.")
			break
		}
	}
}
