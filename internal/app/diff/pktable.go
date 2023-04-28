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
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type ipkTable interface { // {{{
	RunTableRoutine(*sql.DB, *sql.DB, ipkTable)
	GetPkColumns() []pkColumn
	GetPkColumnNames() []string
	PKColumnMaxGroupCount(*sql.DB) int
	UpperBoundaryQuery([]string, []string, int) string
	ResetLowerboundaryUpperboundary([]any, []any) (bool, any)
	TransformUpperBoundaryResult(*sql.DB, *tableUpperBoundary) (resultset [][]any)
} // }}}

// pkColumn return table's primary key column info
type pkColumn struct { // {{{
	ColumnName  string
	DataType    string
	FieldType   iFieldType
	IsLastField bool
} // }}}

// tableHashResult : stores CRC query run result
type tableHashResult struct { // {{{
	issrc     bool
	rowcnt    int
	hash      int
	ts        time.Time
	elapsedms int64
} // }}}

// tableUpperBoundary : json marshalaable struct for UpperBoundary query
type tableUpperBoundary struct { // {{{
	LowerBoundary      []any  `json:"lowerboundary"`
	UpperBoundaryQuery string `json:"upperboundaryquery"`
} // }}}

type pkTable struct {
	_pkColumns     []pkColumn
	_pkColumnNames []string
}

func (t *pkTable) init(pkColumns []pkColumn) { // {{{
	var columnsequence []int

	if len(envArg.ArgPkColumnSequence) == 1 && envArg.ArgPkColumnSequence[0] == "" {
		columnsequence = make([]int, len(pkColumns))
		t._pkColumns = make([]pkColumn, len(pkColumns))
		t._pkColumnNames = make([]string, len(pkColumns))
		for i := 0; i < len(pkColumns); i++ {
			columnsequence[i] = i
		}
	} else {
		newcolumns := envArg.ArgPkColumnSequence
		columnsequence = make([]int, len(newcolumns))
		t._pkColumns = make([]pkColumn, len(newcolumns))
		t._pkColumnNames = make([]string, len(newcolumns))
		for i, seq := range newcolumns {
			v, _ := strconv.Atoi(seq)
			columnsequence[i] = v - 1
		}
	}

	for i, v := range columnsequence {
		t._pkColumns[i] = pkColumns[v]
		t._pkColumnNames[i] = pkColumns[v].ColumnName
	}
} // }}}

func (t *pkTable) GetPkColumns() []pkColumn { // {{{
	return t._pkColumns
} // }}}

func (t *pkTable) GetPkColumnNames() []string { // {{{
	return t._pkColumnNames
} // }}}

// TableLog : table info JSON marshal and output to file, don't encode '>' char in hex
func (t *pkTable) TableLog(outputfile *os.File, ti any) { // {{{
	// b, e := json.Marshal(ti) // encode '>' char in hex,
	// errorCheck(e)
	// fmt.Fprintln(outputfile,string(b))

	// create a buffer to hold JSON data
	buf := new(bytes.Buffer)
	// create JSON encoder for `buf`
	bufEncoder := json.NewEncoder(buf)

	bufEncoder.SetEscapeHTML(false) // don't encode '>' char in hex
	e := bufEncoder.Encode(ti)
	errorCheck(e)
	fmt.Fprint(outputfile, buf) // calls `buf.String()` method
} // }}}

func (t *pkTable) TableQueryColumnNames(
	db *sql.DB,
	table string,
) (columnNames []string, pkColumnsWhere []string) { // {{{
	//  ┌                                                                              ┐
	//  │ figure out columnNames                                                       │
	//  └                                                                              ┘
	columnNames = GetTableColumns(db, table)

	Exclude := func(xs *[]string, excluded map[string]bool) {
		w := 0
		for _, x := range *xs {
			if !excluded[x] {
				(*xs)[w] = x
				w++
			}
		}
		*xs = (*xs)[:w]
	}

	mapFromSlice := func(ex []string) map[string]bool {
		r := map[string]bool{}
		for _, e := range ex {
			r[e] = true
		}
		return r
	}

	Exclude(&columnNames, mapFromSlice(envArg.ArgIgnoreFields))

	//  ┌                                                                              ┐
	//  │   figure out pkColumnsWhere                                                  │
	//  └                                                                              ┘
	pkColumnNames := t.GetPkColumnNames()

	for i := 0; i < len(pkColumnNames); i++ {
		if i == (len(pkColumnNames) - 1) { // last field
			pkColumnsWhere = append(pkColumnsWhere, pkColumnNames[i]+" BETWEEN ? AND ?")
		} else {
			pkColumnsWhere = append(pkColumnsWhere, pkColumnNames[i]+" = ?")
		}
	}

	return
} // }}}

func (t *pkTable) TableHashStmt(
	db *sql.DB,
	issrc bool,
	ptrHashQuerySrc *string,
	ptrHashQueryTgt *string,
	LowerBoundary []any,
	LastPKFieldUpperBoundary any,
) (stmt *sql.Stmt, inputs []any) { // {{{
	var e error

	if issrc {
		log.Debugf("----*ptrHashQuerySrc----\n%v\n", *ptrHashQuerySrc)
		stmt, e = db.Prepare(*ptrHashQuerySrc)
		errorCheck(e)
	} else {
		log.Debugf("----*ptrHashQueryTgt----\n%v\n", *ptrHashQueryTgt)
		stmt, e = db.Prepare(*ptrHashQueryTgt)
		errorCheck(e)
	}

	// lowerboundary for pkcolumns and lastpkfieldUpperboundary
	// field1 = ? AND field2 = ? AND lastpkfield BETWEEN ? AND ?
	for i := 0; i < len(LowerBoundary); i++ {
		inputs = append(inputs, LowerBoundary[i])
	}
	inputs = append(inputs, LastPKFieldUpperBoundary)

	// plugin input value to the normalized query, for logging purpose
	//  ┌──────────────────────────────────────────────────────────────────────────────┐
	for i := 0; i < len(inputs); i++ {
		var quote string
		if i == len(t.GetPkColumns()) {
			if t.GetPkColumns()[len(t.GetPkColumns())-1].FieldType.withQuote() {
				quote = "'"
			}
		} else if t.GetPkColumns()[i].FieldType.withQuote() {
			quote = "'"
		}

		if issrc {
			*ptrHashQuerySrc = strings.Replace(
				*ptrHashQuerySrc,
				"?",
				quote+fmt.Sprint(inputs[i])+quote,
				1,
			)
		} else {
			*ptrHashQueryTgt = strings.Replace(
				*ptrHashQueryTgt,
				"?",
				quote+fmt.Sprint(inputs[i])+quote,
				1,
			)
		}
	}
	//  └──────────────────────────────────────────────────────────────────────────────┘

	log.Tracef("&&&& go routine running on source host: %v, inputs: %v &&&&\n", issrc, inputs)

	return
} // }}}

func (t *pkTable) PKColumnMaxGroupCount(
	dbSrc *sql.DB,
) int { // {{{
	pkColumnNames := t.GetPkColumnNames()
	table := envArg.ArgSrcTable

	query := fmt.Sprintf(`
    SELECT COUNT(1) AS count
    FROM %s
    GROUP BY %s
    HAVING COUNT(1) > 1
    ORDER BY COUNT(1) DESC
    LIMIT 1;
    `,
		table,
		strings.Join(pkColumnNames, ", "),
	)

	stmt, e := dbSrc.Prepare(query)
	errorCheck(e)
	defer func() {
		e := stmt.Close()
		errorCheck(e)
	}()

	var count int
	e = stmt.QueryRow().Scan(&count)
	errorCheck(e)

	return count
} // }}}

/*
UpperBoundaryResult :	Return resultset of DB from queries, for example:

	SELECT dept_no,emp_no
	FROM dept_emp
	WHERE dept_no='d003' AND emp_no>=426762
	ORDER BY dept_no,emp_no
	LIMIT 1000

	SELECT dept_no,emp_no
	FROM dept_emp
	WHERE dept_no>'d003'
	ORDER BY dept_no,emp_no
	LIMIT 211
*/
func (t *pkTable) UpperBoundaryResult(
	dbSrc *sql.DB,
	columnNames []string,
	tub *tableUpperBoundary,
) (resultset [][]any) { // {{{
	stmt, e := dbSrc.Prepare(tub.UpperBoundaryQuery)
	errorCheck(e)
	defer func() {
		e := stmt.Close()
		errorCheck(e)
	}()

	result, e := stmt.Query(tub.LowerBoundary...)
	errorCheck(e)

	// plugin input value to the normalized query for logging purpose
	//  ┌──────────────────────────────────────────────────────────────────────────────┐
	for i := 0; i < len(tub.LowerBoundary); i++ {
		var quote string
		if t.GetPkColumns()[i].FieldType.withQuote() {
			quote = "'"
		}

		tub.UpperBoundaryQuery = strings.Replace(
			tub.UpperBoundaryQuery,
			"?",
			quote+fmt.Sprint(tub.LowerBoundary[i])+quote,
			1,
		)
	}
	//  └──────────────────────────────────────────────────────────────────────────────┘

	/*
		|-------------------+------------------------------------------------------------|
		| single pkTable    | COUNT(1) AS rowcnt, MAX(pkfield1)                          |
		|-------------------+------------------------------------------------------------|
		| composite pkTable | COUNT(1) AS rowcnt, pkfield1, pkfield2, ..., MAX(pkfieldn) |
		|-------------------+------------------------------------------------------------|
	*/
	for result.Next() {
		// len is calculated as: count field + pkcolumns(nolastpkfields) + lastpkfield
		vals := make([]any, 1+len(columnNames)-1+1)
		// COUNT(1) field
		vals[0] = new(int)
		// pkcolumns(nolastpkfields), single PK field table will skip
		for i := 1; i < len(vals)-1; i++ {
			vals[i] = new(any)
		}
		// lastpkfield
		vals[len(vals)-1] = new(any)

		e = result.Scan(vals...)
		errorCheck(e)

		resultset = append(resultset, vals)
	}

	return
} // }}}

// InitialPKFieldLowerboundaryFromTable : find 1st record in the table based on PK columns
func (t *pkTable) InitialPKFieldLowerboundaryFromTable(
	dbSrc *sql.DB,
) (resultset [][]any) { // {{{

	pkColumnNames := t.GetPkColumnNames()
	table := envArg.ArgSrcTable

	query := `
    SELECT SQL_NO_CACHE ` + strings.Join(pkColumnNames, ",") + `
    FROM ` + table + `
    ORDER BY ` + strings.Join(pkColumnNames, ",") + `
    LIMIT 1`

	log.Traceln(query)

	stmt, e := dbSrc.Prepare(query)
	errorCheck(e)
	defer func() {
		e := stmt.Close()
		errorCheck(e)
	}()

	result, e := stmt.Query()
	errorCheck(e)

	for result.Next() {
		vals := make([]any, len(pkColumnNames))

		for i := 0; i < len(vals); i++ {
			vals[i] = new(any)
		}

		e = result.Scan(vals...)
		errorCheck(e)

		resultset = append(resultset, vals)
	}

	return
} // }}}

// FindInitialPKFieldLowerboundary : find lowerboundary []any for the run
func (t *pkTable) FindInitialPKFieldLowerboundary(
	dbSrc *sql.DB,
) (lowerboundary []any) { // {{{

	pkColumnNames := t.GetPkColumnNames()
	lowerboundary = make([]any, len(pkColumnNames))

	if envArg.ArgLowerBoundary[0] != "" {
		for i := 0; i < len(pkColumnNames); i++ {
			if envArg.ArgLowerBoundary[i] != "" {
				ft := t.GetPkColumns()[i].FieldType
				lowerboundary[i] = ft.transformFieldType(envArg.ArgLowerBoundary[i])
			}
		}

		log.Debugf(
			"envArg.ArgLowerBoundary: %v, pkcolumns: %v, lowerboundary: %v\n",
			envArg.ArgLowerBoundary,
			pkColumnNames,
			lowerboundary,
		)
		return
	}

	rub := t.InitialPKFieldLowerboundaryFromTable(dbSrc)
	if len(rub) == 0 {
		log.Debugf("table is empty")
		return
	}

	originalrow := rub[0]
	for i := 0; i < len(originalrow); i++ {
		v := *originalrow[i].(*any)
		ft := t.GetPkColumns()[i].FieldType
		lowerboundary[i] = ft.transformDBResultType(v)
	}

	log.Debugf(
		"envArg.ArgLowerBoundary: %v, pkcolumns: %v, lowerboundary: %v\n",
		envArg.ArgLowerBoundary,
		pkColumnNames,
		lowerboundary,
	)
	return
} // }}}

func (t *pkTable) RunTableChunk(
	dbSrc *sql.DB,
	dbTgt *sql.DB,
	pkTab ipkTable,
	ptrChunkidx *int,
	lowerboundary []any,
	tci *tableChunkInfo,
	resultset [][]any,
) (stoprun bool) { // {{{
	log.Debugf("====resultset: %v====\n", resultset)

	lastPKfieldtype := pkTab.GetPkColumns()[len(pkTab.GetPkColumns())-1].FieldType
	hashQuerySrc := t.TableHashQueryChunkLevel(dbSrc, envArg.ArgSrcTable)
	hashQueryTgt := t.TableHashQueryChunkLevel(dbTgt, envArg.ArgTgtTable)
	rowcntSrc := 0

	// singlePKTable: resultset has only 1 row
	// multiPKTable: resultset could have multiple rows
	for r := 0; r < len(resultset); r++ { // row level
		row := resultset[r]

		// sum of all rowcnt of all rows ( 1st column )
		rowcntSrc += row[0].(int)

		log.Debugf("----inside loop rowcntSrc: %d, row: %v----\n", rowcntSrc, row)

		var stopAfterRun bool
		var lastpkfieldUpperboundary any

		stopAfterRun, lastpkfieldUpperboundary = pkTab.ResetLowerboundaryUpperboundary(
			row[:len(row)-1],
			lowerboundary,
		)

		// if this is the table last record, stop run
		// r == (len(resultset)-1):
		//   singlePKTable: always true
		//   multiPKTable: true when last row in the loop
		if r == (len(resultset)-1) &&
			lastPKfieldtype.equals(
				lowerboundary[len(lowerboundary)-1],
				lastpkfieldUpperboundary,
			) {
			fmt.Printf("END [last record]: %v\n", lowerboundary)
			stoprun = true
			break
		}

		// more smaller chunks if more than 1 row
		*ptrChunkidx++

		tci.ChunkIdx = *ptrChunkidx
		tci.LastPKFieldUpperBoundary = lastpkfieldUpperboundary
		tci.LowerBoundary = lowerboundary
		tci.UpperBoundaryQuery = row[len(row)-1].(string)
		tci.HashQuerySrc = hashQuerySrc // normalized
		tci.HashQueryTgt = hashQueryTgt // normalized

		// tci.HashQuerySrc and tci.HashQueryTgt are normalized, will be changed/filled for logging purpose
		t.RunTableRoutineChunkLevel(dbSrc, dbTgt, tci)

		lb, _ := json.Marshal(tci.LowerBoundary)
		ub, _ := json.Marshal(
			append(
				append(
					[]any{},
					tci.LowerBoundary[:len(tci.LowerBoundary)-1]...,
				),
				tci.LastPKFieldUpperBoundary),
		)

		logmsg := fmt.Sprintf(
			"[%-5v] [%5d] -l %v -u %v [RowcntSrc: %d, RowcntTgt: %d]",
			tci.Match,
			tci.ChunkIdx,
			strings.Trim(string(lb), "[]"),
			strings.Trim(string(ub), "[]"),
			tci.RowcntSrc,
			tci.RowcntTgt,
		)
		log.SetReportCaller(false) // hide line number
		log.Infoln(logmsg)
		log.SetReportCaller(true) // show line number

		if stopAfterRun {
			stoprun = true
			break
		}

		lowerboundary[len(lowerboundary)-1] = tci.LastPKFieldUpperBoundary

		log.Debugf(
			"----after loop rowcntSrc: %d, lowerboundary: %v----\n\n%s",
			rowcntSrc,
			lowerboundary,
			"================================================================================",
		)
	}

	return
} // }}}

// RunTableRoutine : loop through ranges between lowerboundary and upperboundary
func (t *pkTable) RunTableRoutine(
	dbSrc *sql.DB,
	dbTgt *sql.DB,
	pkTab ipkTable,
) { // {{{
	stoprun := false
	lowerboundary := t.FindInitialPKFieldLowerboundary(dbSrc)

	var pkColumnValuesQuote []string
	for _, pkcolumn := range pkTab.GetPkColumns() {
		if pkcolumn.FieldType.withQuote() {
			pkColumnValuesQuote = append(pkColumnValuesQuote, "'")
		} else {
			pkColumnValuesQuote = append(pkColumnValuesQuote, "")
		}
	}

	for chunkidx := 0; !stoprun; {
		var tci tableChunkInfo
		tci.TableSrc = envArg.ArgSrcTable
		tci.TableTgt = envArg.ArgTgtTable
		tci.PKColumnNames = t.GetPkColumnNames()
		tci.PKColumnValuesQuote = pkColumnValuesQuote
		tci.PkColumnSequence = envArg.ArgPkColumnSequence
		tci.IgnoreFields = envArg.ArgIgnoreFields
		tci.AdditionalFilter = envArg.ArgAdditionalFilter

		var tub tableUpperBoundary
		// make a copy of lowerboundary
		tub.LowerBoundary = append([]any(nil), lowerboundary...)
		resultset := pkTab.TransformUpperBoundaryResult(dbSrc, &tub)
		//  ┌                                                                              ┐
		//  │ single PK tables resultset has rows with following fields                    │
		//  └                                                                              ┘
		// 1. COUNT(1)
		// 2. PK field lowerboundary
		// 3. PK field upperboundary
		// 4. UpperBoundaryQuery
		//  ┌                                                                              ┐
		//  │ composite PK tables resultset has rows with following fields                 │
		//  └                                                                              ┘
		// 1. COUNT(1)
		// 2. PK fields (not include last field)
		// 3. last PK field lowerboundary
		// 4. last PK field upperboundary
		// 5. UpperBoundaryQuery
		stoprun = t.RunTableChunk(dbSrc, dbTgt, pkTab, &chunkidx, lowerboundary, &tci, resultset)
	}
} // }}}

// vim: fdm=marker fdc=2
