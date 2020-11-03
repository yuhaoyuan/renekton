package main

/*
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
)
func main() {
	table := newTable()
	inputBuffer := newInputBuffer()

	for {
		printPrompt()
		err := readInput(inputBuffer)
		if err != nil {

		}
		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer) {
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
