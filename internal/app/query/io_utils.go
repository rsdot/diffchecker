/*
Copyright © 2023 Rick Sun

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

// Package query is a tool to generate sql statment for diff crud results
package query

import (
	"bufio"
	"io"
	"log"
	"os"
)

func readFromStdinOrFile(argRowlevelFile string) (inputLineBytes [][]byte) { // {{{
	// detect if stdin is empty
	// https://stackoverflow.com/a/26567513/10
	// https://stackoverflow.com/questions/8757389/reading-a-file-line-by-line-in-go

	var reader *bufio.Reader

	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		// data is being piped to stdin
		reader = bufio.NewReader(os.Stdin)
	} else if argRowlevelFile != "" {
		// data is being read from a file
		file, e := os.Open(argRowlevelFile)
		if e != nil {
			errorCheck(e)
		}
		defer func() {
			e := file.Close()
			errorCheck(e)
		}()

		reader = bufio.NewReader(file)
	} else {
		// stdin is from a terminal
		log.Fatal("require either -f <file> or pipe")
	}

	for {
		linebytes, e := reader.ReadBytes('\n')
		if e != nil {
			if e == io.EOF {
				break
			}
			log.Fatalf("read file line error: %v", e)
		}
		inputLineBytes = append(inputLineBytes, linebytes)
	}

	if len(inputLineBytes) == 0 {
		log.Fatal("either -f <file> or pipe is empty")
	}

	return
} // }}}

// vim: fdm=marker fdc=2
