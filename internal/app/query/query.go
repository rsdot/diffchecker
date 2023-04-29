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

// Importing fmt package for the sake of printing
import (
	"bufio"
	"diffchecker/internal/app/diff"
	"diffchecker/internal/pkg/common"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type envarg struct { // {{{
	ArgInsert       bool
	ArgUpdate       bool
	ArgDelete       bool
	ArgRowlevelFile string
} // }}}

// envArg is the package variable that holds the arg variables
var envArg = envarg{}

// envVar is the package variable that holds the environment variables
var envVar = common.GetEnvVar()

// ConsolidateTableRows : A struct to store multiple diff chunk json lines output
type ConsolidateTableRows struct { // {{{
	MapPKColumnValuesRows *map[string][]string // formated PK Column Values, 1 string per row
	MapPKColumnValues     *map[string][][]any  // actual PK Column Values
	TableSrc              string
	TableTgt              string
	AllPKColumnNames      []string
	AllPKColumnQuotes     []string
	FieldColumnNames      []string
} // }}}

func errorCheck(err error) { // {{{
	if err != nil {
		panic(err.Error())
	}
} // }}}

// SetArgs : assign CLI arguments
func SetArgs(
	argInsert bool,
	argUpdate bool,
	argDelete bool,
	argRowlevelFile string,
) { // {{{
	envArg.ArgInsert = argInsert
	envArg.ArgUpdate = argUpdate
	envArg.ArgDelete = argDelete
	envArg.ArgRowlevelFile = argRowlevelFile
} // }}}

func readFromStdinOrFile(argRowlevelFile string) (textlinebytes [][]byte) { // {{{
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
		textlinebytes = append(textlinebytes, linebytes)
	}

	if len(textlinebytes) == 0 {
		log.Fatal("either -f <file> or pipe is empty")
	}

	return
} // }}}

// PopulateConsolidateTableRows : populate ConsolidateTableRows struct to store multiple diff chunk json lines output
func PopulateConsolidateTableRows(
	argRowlevelFile string,
) (consolidateTableRows *ConsolidateTableRows) { // {{{

	textlinebytes := readFromStdinOrFile(argRowlevelFile)

	dbSrc := diff.InitializeDBSettings(
		envVar.DfcSrcHost,
		envVar.DfcSrcPort,
		envVar.DfcSrcUsername,
		envVar.DfcSrcPassword,
		envVar.DfcSrcDbname,
	)
	defer func() {
		e := dbSrc.Close()
		errorCheck(e)
	}()

	// stores PK column value rows string 'value1',value2,'value3'...
	mapPKColumnValuesRows := &map[string][]string{
		"insert": {},
		"update": {},
		"delete": {},
	}

	// stores PK column value rows original data
	mapPKColumnValues := &map[string][][]any{
		"insert": {},
		"update": {},
		"delete": {},
	}

	consolidateTableRows = &ConsolidateTableRows{
		MapPKColumnValuesRows: mapPKColumnValuesRows,
		MapPKColumnValues:     mapPKColumnValues,
	}

	// deduplication of table rows, for the case where the boundary row is different, which would result in multiple diff chunk json lines
	mapTableRowExists := &map[string](map[string]bool){
		"insert": {},
		"update": {},
		"delete": {},
	}

	populate := func(crudtype string, tablerows []diff.TableRow) { // {{{
		for _, tr := range tablerows {
			var fields []string

			for i := 0; i < len(tr.AllPKColumnValues); i++ {
				fields = append(
					fields,
					fmt.Sprintf(
						"%s%v%s",
						consolidateTableRows.AllPKColumnQuotes[i],
						tr.AllPKColumnValues[i],
						consolidateTableRows.AllPKColumnQuotes[i],
					),
				)
			}
			stringPKColumnValuesRow := strings.Join(fields, ",")

			// fmt.Printf(
			// 	"crudtype: %v, tablerow.PKColumnValues: %v, pkColumnValuesRow: %v\n",
			// 	crudtype,
			// 	tr.PKColumnValues,
			// 	(*mapTableRowExists)[crudtype][pkColumnValuesRow],
			// )

			if !(*mapTableRowExists)[crudtype][stringPKColumnValuesRow] {
				(*mapTableRowExists)[crudtype][stringPKColumnValuesRow] = true
				(*mapPKColumnValuesRows)[crudtype] = append(
					(*mapPKColumnValuesRows)[crudtype],
					stringPKColumnValuesRow,
				)
				(*mapPKColumnValues)[crudtype] = append(
					(*mapPKColumnValues)[crudtype],
					tr.AllPKColumnValues,
				)
			}
		}
	} // }}}

	for _, input := range textlinebytes {
		var tcri diff.TableChunkRowsInfo
		// fmt.Printf(
		// 	"textline: %v\n",
		// 	string(input),
		// )
		e := json.Unmarshal(input, &tcri)
		errorCheck(e)

		if consolidateTableRows.TableSrc == "" {
			consolidateTableRows.TableSrc = tcri.TableSrc
		}

		if consolidateTableRows.TableTgt == "" {
			consolidateTableRows.TableTgt = tcri.TableTgt
		}

		if consolidateTableRows.FieldColumnNames == nil {
			consolidateTableRows.FieldColumnNames = diff.GetTableColumns(dbSrc, tcri.TableSrc)
		}

		if consolidateTableRows.AllPKColumnNames == nil {
			consolidateTableRows.AllPKColumnNames = diff.FindAllPKColumnNames(dbSrc, tcri.TableSrc)
		}

		if consolidateTableRows.AllPKColumnQuotes == nil {
			consolidateTableRows.AllPKColumnQuotes = diff.FindAllPKColumnQuotes(
				dbSrc,
				tcri.TableSrc,
			)
		}

		populate("insert", tcri.Diff.Insert)
		populate("update", tcri.Diff.Update)
		populate("delete", tcri.Diff.Delete)
	}

	// fmt.Printf("insert: %v\n", (*consolidateTableRows.MapTableRows)["insert"])
	// fmt.Printf("update: %v\n", (*consolidateTableRows.MapTableRows)["update"])
	// fmt.Printf("delete: %v\n", (*consolidateTableRows.MapTableRows)["delete"])

	return
} // }}}

// prepareSQLStatement : prepares sql statement based on diff crud results
func prepareSQLStatement(
	crudtype string,
	consolidateTableRows *ConsolidateTableRows,
) string { // {{{

	tableSrc := consolidateTableRows.TableSrc
	tableTgt := consolidateTableRows.TableTgt
	fieldnames := consolidateTableRows.FieldColumnNames
	stringAllPKColumnNames := strings.Join(consolidateTableRows.AllPKColumnNames, ",")

	mapAllPKColumnValues := map[string]bool{}
	for _, pkColumnName := range consolidateTableRows.AllPKColumnNames {
		mapAllPKColumnValues[pkColumnName] = true
	}

	var pkColumnValuesRows []string
	//  format to slice of ROW(value1, value2)
	//  ┌──────────────────────────────────────────────────────────────────────────────┐
	for _, value := range (*consolidateTableRows.MapPKColumnValuesRows)[crudtype] {
		pkColumnValuesRows = append(pkColumnValuesRows, fmt.Sprintf("ROW(%s)", value))
	}
	//  └──────────────────────────────────────────────────────────────────────────────┘

	dbSrc := diff.InitializeDBSettings(
		envVar.DfcSrcHost,
		envVar.DfcSrcPort,
		envVar.DfcSrcUsername,
		envVar.DfcSrcPassword,
		envVar.DfcSrcDbname,
	)
	defer func() {
		e := dbSrc.Close()
		errorCheck(e)
	}()

	mapDiffTable := &map[string]string{
		"insert": tableSrc + "_diff_insert",
		"update": tableSrc + "_diff_update",
		"delete": tableSrc + "_diff_delete",
	}

	//  source
	//  ┌──────────────────────────────────────────────────────────────────────────────┐
	query := fmt.Sprintf(`
  -- source
  DROP TABLE IF EXISTS %s;
  CREATE TABLE %s AS SELECT * FROM /*source*/ %s WHERE 1=2;
  ALTER TABLE %s ADD PRIMARY KEY (%s);
  `,
		(*mapDiffTable)[crudtype],
		(*mapDiffTable)[crudtype],
		tableSrc,
		(*mapDiffTable)[crudtype],
		stringAllPKColumnNames,
	)

	query = query + fmt.Sprintf(`
  INSERT INTO %s(
    %s)
  SELECT
    s.%s
  FROM /*source*/ %s AS s
  INNER JOIN (
    SELECT *
    FROM (VALUES
      %s
      ) AS d(%s)
    ) AS dif
  USING (%s);
  `,
		(*mapDiffTable)[crudtype],
		strings.Join(fieldnames, ",\n    "),
		strings.Join(fieldnames, ",\n    s."),
		tableSrc,
		strings.Join(
			pkColumnValuesRows,
			",\n      ",
		), // format to ROW('value1',value2,'value3'...), ...
		stringAllPKColumnNames,
		stringAllPKColumnNames,
	)
	//  └──────────────────────────────────────────────────────────────────────────────┘

	//  target
	//  ┌──────────────────────────────────────────────────────────────────────────────┐
	switch crudtype {
	case "insert":
		//  ┌                                                                              ┐
		//  │ insert                                                                       │
		//  └                                                                              ┘
		query = query + fmt.Sprintf(`

    -- target
    INSERT INTO /*target*/ %s(
      %s)
    SELECT
      s.%s
    FROM /*target*/ %s AS s`,
			tableTgt,
			strings.Join(fieldnames, ",\n      "),
			strings.Join(fieldnames, ",\n      s."),
			(*mapDiffTable)[crudtype],
		)

	case "update":
		//  ┌                                                                              ┐
		//  │ update                                                                       │
		//  └                                                                              ┘
		updatefieldnames := []string{}
		for _, fieldname := range fieldnames {
			var v string
			if mapAllPKColumnValues[fieldname] {
				v = fmt.Sprintf("  -- /*PK*/ t.%s = s.%s", fieldname, fieldname)
			} else {
				v = fmt.Sprintf("  t.%s = s.%s", fieldname, fieldname)
			}
			updatefieldnames = append(updatefieldnames, v)
		}

		query = query + fmt.Sprintf(`

    -- target
    UPDATE /*target*/ %s AS t
    INNER JOIN /*target*/ %s AS s
    USING (%s)
    SET
    %s;`,
			tableTgt,
			(*mapDiffTable)[crudtype],
			stringAllPKColumnNames,
			strings.Join(updatefieldnames, ",\n    "),
		)

	case "delete":
		//  ┌                                                                              ┐
		//  │ delete                                                                       │
		//  └                                                                              ┘
		query = fmt.Sprintf(`

    -- target
    DELETE t
    FROM /*target*/ %s AS t
    INNER JOIN (
      SELECT *
      FROM (VALUES
        %s
        ) AS d(%s)
      ) AS dif
    USING (%s);`,
			tableTgt,
			strings.Join(
				pkColumnValuesRows,
				",\n        ",
			), // format to ROW(value1, value2), ROW(value3, value4)
			stringAllPKColumnNames,
			stringAllPKColumnNames,
		)
	}
	//  └──────────────────────────────────────────────────────────────────────────────┘

	return query
} // }}}

// GenerateSQL : Generate SQL statements
func GenerateSQL() { // {{{

	consolidateTableRows := PopulateConsolidateTableRows(envArg.ArgRowlevelFile)

	formatcrudresult := func(crudtype string) { // {{{
		fmt.Printf(
			"\n-- ┌[" + crudtype + "]──────────────────────────────────────────────────────────────────────────────┐\n",
		)

		if len((*consolidateTableRows.MapPKColumnValuesRows)[crudtype]) == 0 {
			fmt.Printf("//  <empty>")
		} else {
			preparedSQL := prepareSQLStatement(crudtype, consolidateTableRows)
			fmt.Println(preparedSQL)
		}

		fmt.Printf(
			"\n-- └──────────────────────────────────────────────────────────────────────────────────────┘\n",
		)
	} // }}}

	if envArg.ArgDelete {
		formatcrudresult("delete")
	}
	if envArg.ArgInsert {
		formatcrudresult("insert")
	}
	if envArg.ArgUpdate {
		formatcrudresult("update")
	}
} // }}}

// vim: fdm=marker fdc=2
