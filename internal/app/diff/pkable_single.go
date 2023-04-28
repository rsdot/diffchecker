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
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type pkTableSingle struct { // {{{
	pkTable
} // }}}

func (t *pkTableSingle) Init(pkColumns []pkColumn) *pkTableSingle { // {{{
	t.init(pkColumns)
	return t
} // }}}

/*
UpperBoundaryQuery for single pkTable
query statement returns 2 fields

	SELECT SQL_NO_CACHE
		COUNT(1) AS rowcnt, MAX(pkfield1)
	FROM (
		SELECT pkfield1
		FROM table
		WHERE pkfield1 >= ?
		ORDER BY pkfield1
		LIMIT 1000) AS A
*/
func (t *pkTableSingle) UpperBoundaryQuery(
	columnNames []string,
	columnOperators []string,
	chunksize int,
) (query string) { // {{{
	table := envArg.ArgSrcTable

	var lastpkfield string
	var pkcolumnsWhere []string

	lastpkfield = columnNames[0]
	pkcolumnsWhere = append(pkcolumnsWhere, columnNames[0]+columnOperators[0]+"?")
	var additionalfilterstmt string
	if envArg.ArgAdditionalFilter != "" {
		additionalfilterstmt = " AND " + envArg.ArgAdditionalFilter
	}

	query = `
    SELECT SQL_NO_CACHE
			COUNT(1) AS rowcnt, MAX(` + lastpkfield + `)
    FROM (
      SELECT ` + strings.Join(columnNames, ",") + `
      FROM ` + table + `
      WHERE ` + strings.Join(pkcolumnsWhere, " AND ") + additionalfilterstmt + `
      ORDER BY ` + strings.Join(columnNames, ",") + `
      LIMIT ` + strconv.Itoa(chunksize) + `) AS A
    `

	log.Traceln(query)

	return
} // }}}

/*
TransformUpperBoundaryResult for single pkTable
returns 3 fields

 1. COUNT(1)
 2. PK field lowerboundary
 3. PK field upperboundary
 4. UpperBoundaryQuery
*/
func (t *pkTableSingle) TransformUpperBoundaryResult(
	dbSrc *sql.DB,
	tub *tableUpperBoundary,
) (resultset [][]any) { // {{{
	chunksize := envArg.ArgChunksize
	pkColumnNames := t.GetPkColumnNames()

	lastPKfieldtype := t.GetPkColumns()[len(t.GetPkColumns())-1].FieldType
	var pkColumnOperators []string

	// lambda function code for reuse, transform column type *any-> any, return
	// 1. COUNT(1)
	// 2. PK field lowerboundary
	// 3. PK field upperboundary
	// 4. UpperBoundaryQuery
	populateResultset := func(rub [][]any) int { // {{{
		rowcntSrc := 0
		originalrow := rub[0]
		var rowinresultset []any

		rowcntSrc += *originalrow[0].(*int)

		// 1. COUNT(1)
		rowinresultset = append(rowinresultset, *originalrow[0].(*int))

		// 2. PK field lowerboundary
		rowinresultset = append(rowinresultset, tub.LowerBoundary[0])

		// 3. PK field upperboundary
		v := *originalrow[len(originalrow)-1].(*any)
		log.Debugf(
			"====return lowerboundary: %v, column type: %T, value: %v====\n",
			tub.LowerBoundary[0],
			v,
			lastPKfieldtype.transformDBResultType(v),
		)
		rowinresultset = append(rowinresultset, lastPKfieldtype.transformDBResultType(v))

		// 4. UpperBoundaryQuery
		rowinresultset = append(rowinresultset, tub.UpperBoundaryQuery)

		resultset = append(resultset, rowinresultset)

		return rowcntSrc
	} // }}}

	runUpperBoundary := func(runidx int) int { // {{{
		tub.UpperBoundaryQuery = t.UpperBoundaryQuery(pkColumnNames, pkColumnOperators, chunksize)
		log.Debugf("----[%d] lowerboundary: %v----\n", runidx, tub.LowerBoundary)
		log.Debugf("----[%d] UpperBoundaryQuery: %v----\n", runidx, tub.UpperBoundaryQuery)

		rub := t.UpperBoundaryResult(dbSrc, pkColumnNames, tub)
		log.Debugf("----[%d] UpperBoundaryQuery formated: %v----\n", runidx, tub.UpperBoundaryQuery)
		log.Debugf("----[%d] chunksize: %d----\n", runidx, chunksize)

		rowcntSrc := populateResultset(rub)

		log.Debugf("====[%d] rowcntSrc: %d, resultset: %v====\n", runidx, rowcntSrc, resultset)
		log.Tracef("====[%d] query: %v====\n", runidx, tub.UpperBoundaryQuery)

		return rowcntSrc
	} // }}}

	pkColumnOperators = []string{">="}
	runUpperBoundary(1)
	return
} // }}}

func (t *pkTableSingle) ResetLowerboundaryUpperboundary(
	row []any,
	lowerboundary []any,
) (
	stopAfterRun bool,
	lastpkfieldUpperboundary any,
) { // {{{
	log.Debugf("----before lowerboundary: %v----\n", lowerboundary)

	stopAfterRun = false

	// reset lowerboundary and lastpkfieldUpperboundary
	for c := 1; c < len(row); c++ { // column level
		if c < len(row)-1 {
			lowerboundary[c-1] = row[c]
		} else { // lastpkfield
			lastpkfieldUpperboundary = row[c]
		}
	}

	if envArg.ArgUpperBoundary[len(envArg.ArgUpperBoundary)-1] == "" {
		return
	}

	lastPKfieldtype := t.GetPkColumns()[len(t.GetPkColumns())-1].FieldType
	userUpperboundary := lastPKfieldtype.transformFieldType(
		envArg.ArgUpperBoundary[len(envArg.ArgUpperBoundary)-1],
	)

	if lastPKfieldtype.equals(lastpkfieldUpperboundary, userUpperboundary) {
		stopAfterRun = true
	} else if lastPKfieldtype.greaterThan(lastpkfieldUpperboundary, userUpperboundary) { // overwrite resultset value
		stopAfterRun = true
		lastpkfieldUpperboundary = userUpperboundary
	}

	log.Debugf(
		"----after stopAfterRun: %v, lowerboundary: %v, lastpkfieldUpperboundary: %v, userUpperboundary: %v----\n",
		stopAfterRun,
		lowerboundary,
		lastpkfieldUpperboundary,
		userUpperboundary,
	)

	return
} // }}}

// vim: fdm=marker fdc=2
