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

package diff

// Importing fmt package for the sake of printing
import (
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// TableRow : json marshalable struct on table row level
type TableRow struct { // {{{
	Hash              int   `json:"rowhash"`
	AllPKColumnValues []any `json:"allpkcolumnvalues"`
} // }}}

// tableRowCrud : json marshalable struct on table row level for crud types
type tableRowCrud struct { // {{{
	Insert []TableRow `json:"insert"`
	Update []TableRow `json:"update"`
	Delete []TableRow `json:"delete"`
} // }}}

// TableChunkRowsInfo : json marshalaable struct for table chunk rows
type TableChunkRowsInfo struct { // {{{
	tableChunkInfo
	Diff            tableRowCrud   `json:"diff"`
	TableRowsSrc    []TableRow     `json:"-"` // raw source table rows
	TableRowsTgt    []TableRow     `json:"-"` // raw target table rows
	MapTableRowsSrc map[string]int `json:"-"` // map of source table pk column values to row hash
	MapTableRowsTgt map[string]int `json:"-"` // map of target table pk column values to row hash
} // }}}

/*
TableHashQueryRowLevel : construct hash query statement for each row in the range

	SELECT COUNT(1) AS rowcnt, CRC32(CONCAT_WS('#', field1, field2, ..., fieldn))
	FROM table
	WHERE pkfield1 = ? AND pkfield2 = ? AND ... AND pkfieldn BETWEEN ? AND ?
	ORDER BY pkfield1, pkfield2, ..., pkfieldn
*/
func (t *pkTable) TableHashQueryRowLevel(
	db *sql.DB,
	table string,
) (query string) { // {{{

	columnNames, pkColumnsWhere := t.TableQueryColumnNames(db, table)

	allPKColumnNames := t.GetAllPKColumnNames()
	var additionalfilterstmt string
	if envArg.ArgAdditionalFilter != "" {
		additionalfilterstmt = " AND " + envArg.ArgAdditionalFilter
	}

	query = `
    SELECT SQL_NO_CACHE
      CAST(CRC32(
        CONCAT_WS('#',` + strings.Join(columnNames, ",") + `)
        ) AS UNSIGNED) AS crc32,` +
		strings.Join(allPKColumnNames, ",") + `
    FROM ` + table + `
    WHERE ` + strings.Join(pkColumnsWhere, " AND ") + additionalfilterstmt + `
    ORDER BY ` + strings.Join(allPKColumnNames, ",")

	log.Traceln(query)

	return
} // }}}

// TableResultRowLevel : execute hash row level query statement and stores result in struct
// tablehashresult
func (t *pkTable) TableResultRowLevel(
	db *sql.DB,
	issrc bool,
	tcri *TableChunkRowsInfo,
) (result tableHashResult) { // {{{

	stmt, inputs := t.TableHashStmt(
		db,
		issrc,
		&tcri.HashQuerySrc,
		&tcri.HashQueryTgt,
		tcri.LowerBoundary,
		tcri.LastPKFieldUpperBoundary,
	)
	if stmt != nil {
		defer func() {
			e := stmt.Close()
			errorCheck(e)
		}()
	}

	ts := time.Now()
	rowresult, e := stmt.Query(inputs...)
	errorCheck(e)
	elapsedms := time.Since(ts).Milliseconds()

	result.issrc = issrc
	result.ts = ts
	result.elapsedms = elapsedms

	// loop through the result set and store the result in struct
	for rowresult.Next() {
		vals := make([]any, len(t.GetAllPKColumns())+1)

		// 1st field: hash
		vals[0] = new(int)

		// 2nd - last fields: pkcolumn values
		for i := 1; i < len(vals); i++ {
			vals[i] = new(any)
		}

		e = rowresult.Scan(vals...)
		errorCheck(e)

		allPKColumnValues := make([]any, len(t.GetAllPKColumns()))

		for i := 1; i < len(vals); i++ {
			v := *vals[i].(*any)
			tv := t.GetAllPKColumns()[i-1].FieldType.transformDBResultType(v)
			allPKColumnValues[i-1] = tv
		}

		tr := TableRow{
			Hash:              *vals[0].(*int),
			AllPKColumnValues: allPKColumnValues,
		}

		allPKColumnValuesBytes, _ := json.Marshal(tr.AllPKColumnValues)
		stringAllPKColumnValues := string(allPKColumnValuesBytes)

		if issrc {
			tcri.TableRowsSrc = append(tcri.TableRowsSrc, tr)
			tcri.MapTableRowsSrc[stringAllPKColumnValues] = tr.Hash
		} else {
			tcri.TableRowsTgt = append(tcri.TableRowsTgt, tr)
			tcri.MapTableRowsTgt[stringAllPKColumnValues] = tr.Hash
		}
	}

	return
} // }}}

// TableRoutineRowLevel : co-routine executing hash query against both source and target DB on
// row level
func (t *pkTable) TableRoutineRowLevel(
	dbSrc *sql.DB,
	dbTgt *sql.DB,
	tcri *TableChunkRowsInfo,
) { // {{{
	var waitgroup sync.WaitGroup
	hashchan := make(chan tableHashResult)

	waitgroup.Add(2)

	go func() {
		waitgroup.Wait()
		close(hashchan)
	}()

	go func() {
		defer waitgroup.Done()
		hashchan <- t.TableResultRowLevel(dbSrc, true, tcri)
	}()

	go func() {
		defer waitgroup.Done()
		hashchan <- t.TableResultRowLevel(dbTgt, false, tcri)
	}()

	for result := range hashchan {
		if result.issrc {
			tcri.ElapsedMsSrc, tcri.TimestampSrc = result.elapsedms, result.ts
		} else {
			tcri.ElapsedMsTgt, tcri.TimestampTgt = result.elapsedms, result.ts
		}
	}

	// figure out crud on row level
	//  ┌──────────────────────────────────────────────────────────────────────────────┐
	for _, tr := range tcri.TableRowsSrc {
		allPKColumnValuesBytes, _ := json.Marshal(tr.AllPKColumnValues)
		stringAllPKColumnValues := string(allPKColumnValuesBytes)

		if _, exists := tcri.MapTableRowsTgt[stringAllPKColumnValues]; !exists {
			tcri.Diff.Insert = append(tcri.Diff.Insert, tr)
		} else if tcri.MapTableRowsSrc[stringAllPKColumnValues] != tcri.MapTableRowsTgt[stringAllPKColumnValues] {
			tcri.Diff.Update = append(tcri.Diff.Update, tr)
		}
	}

	for _, tr := range tcri.TableRowsTgt {
		allPKColumnValuesBytes, _ := json.Marshal(tr.AllPKColumnValues)
		if _, exists := tcri.MapTableRowsSrc[string(allPKColumnValuesBytes)]; !exists {
			tcri.Diff.Delete = append(tcri.Diff.Delete, tr)
		}
	}
	//  └──────────────────────────────────────────────────────────────────────────────┘
} // }}}

func (t *pkTable) RunTableRoutineRowLevel(
	dbSrc *sql.DB,
	dbTgt *sql.DB,
	tci *tableChunkInfo,
) { // {{{
	tcri := new(TableChunkRowsInfo)
	tcri.Match = tci.Match
	tcri.ChunkIdx = tci.ChunkIdx
	tcri.TableSrc = envArg.ArgSrcTable
	tcri.TableTgt = envArg.ArgTgtTable
	tcri.PKColumnNames = t.GetPKColumnNames()
	tcri.RowcntSrc = tci.RowcntSrc
	tcri.RowcntTgt = tci.RowcntTgt
	tcri.HashSrc = tci.HashSrc
	tcri.HashTgt = tci.HashTgt
	tcri.LastPKFieldUpperBoundary = tci.LastPKFieldUpperBoundary
	tcri.LowerBoundary = tci.LowerBoundary
	tcri.UpperBoundaryQuery = tci.UpperBoundaryQuery
	tcri.PKColumnSequence = tci.PKColumnSequence
	tcri.IgnoreFields = tci.IgnoreFields
	tcri.AdditionalFilter = tci.AdditionalFilter
	tcri.HashQuerySrc = t.TableHashQueryRowLevel(dbSrc, envArg.ArgSrcTable)
	tcri.HashQueryTgt = t.TableHashQueryRowLevel(dbTgt, envArg.ArgTgtTable)

	tcri.MapTableRowsSrc = make(map[string]int, tcri.RowcntSrc)
	tcri.MapTableRowsTgt = make(map[string]int, tcri.RowcntTgt)

	t.TableRoutineRowLevel(dbSrc, dbTgt, tcri)

	// if !envArg.ArgDebug {
	// 	tcri.UpperBoundaryQuery = ""
	// 	tcri.HashQuerySrc = ""
	// 	tcri.HashQueryTgt = ""
	// }
	t.TableLog(envArg.ArgOutputRowLevelfile, tcri)
} // }}}

// vim: fdm=marker fdc=2
