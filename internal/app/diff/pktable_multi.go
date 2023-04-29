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

type pkTableMulti struct { // {{{
	pkTable
} // }}}

func (t *pkTableMulti) Init(pkColumns []pkColumn) *pkTableMulti { // {{{
	t.init(pkColumns)
	return t
} // }}}

/*
UpperBoundaryQuery for composite pkTable
query statement returns 3 or more fields

	SELECT SQL_NO_CACHE
		 COUNT(1) AS rowcnt, pkfield1, pkfield2, ..., MAX(pkfieldn)
	FROM (
		SELECT pkfield1, pkfield2, ..., pkfieldn
		FROM table
		WHERE pkfield1 = ? AND pkfield2 = ? AND ... AND pkfieldn >= ? -- with pkfieldn filter
		ORDER BY pkfield1, pkfield2, ..., pkfieldn
		LIMIT 1000) AS A
	GROUP BY pkfield1, pkfield2, ..., pkfield(n-1)
	ORDER BY pkfield1, pkfield2, ..., pkfield(n-1)

or

	SELECT SQL_NO_CACHE
		 COUNT(1) AS rowcnt, pkfield1, pkfield2, ..., MAX(pkfieldn)
	FROM (
		SELECT pkfield1, pkfield2, ..., pkfieldn
		FROM table
		WHERE pkfield1 = ? AND pkfield2 = ? AND ... AND pkfield(n-1) > ? -- no pkfieldn filter
		ORDER BY pkfield1, pkfield2, ..., pkfieldn
		LIMIT 1000) AS A
	GROUP BY pkfield1, pkfield2, ..., pkfield(n-1)
	ORDER BY pkfield1, pkfield2, ..., pkfield(n-1)
*/
func (t *pkTableMulti) UpperBoundaryQuery(
	columnNames []string,
	columnOperators []string,
	chunksize int,
) (query string) { // {{{
	table := envArg.ArgSrcTable

	var lastpkfield string
	var pkcolumnsWhere []string
	var pkcolumnsNolastpkfields []string

	for i := 0; i < len(columnNames); i++ {
		if len(columnOperators) == len(columnNames) {
			pkcolumnsWhere = append(pkcolumnsWhere, columnNames[i]+columnOperators[i]+"?")
		} else if i < len(columnOperators) { // skip pkcolumns that do NOT have corresponding operator
			// for 2 PK case, 1st PK field has new group value, so no need to have 2nd PK field filter in WHERE statement
			// thus len(pkcolumnsOperator) = 1, when i = 1, no 2nd PK field filter in WHERE statement
			pkcolumnsWhere = append(pkcolumnsWhere, columnNames[i]+columnOperators[i]+"?")
		}

		if i < (len(columnNames) - 1) {
			pkcolumnsNolastpkfields = append(pkcolumnsNolastpkfields, columnNames[i])
		} else { // lastpkfield
			lastpkfield = columnNames[i]
		}
	}

	var additionalfilterstmt string
	if envArg.ArgAdditionalFilter != "" {
		additionalfilterstmt = " AND " + envArg.ArgAdditionalFilter
	}

	query = `
    SELECT SQL_NO_CACHE
			COUNT(1) AS rowcnt,` + strings.Join(pkcolumnsNolastpkfields, ",") + `,MAX(` + lastpkfield + `)
    FROM (
      SELECT ` + strings.Join(columnNames, ",") + `
      FROM ` + table + `
      WHERE ` + strings.Join(pkcolumnsWhere, " AND ") + additionalfilterstmt + `
      ORDER BY ` + strings.Join(columnNames, ",") + `
      LIMIT ` + strconv.Itoa(chunksize) + `) AS A
    GROUP BY ` + strings.Join(pkcolumnsNolastpkfields, ",") + `
    ORDER BY ` + strings.Join(pkcolumnsNolastpkfields, ",") + `
    `

	log.Traceln(query)

	return
} // }}}

/*
TransformUpperBoundaryResult for composite pkTable
returns 4 or more fields

 1. COUNT(1)
 2. PK fields (not include last field)
 3. last PK field lowerboundary
 4. last PK field upperboundary
 5. UpperBoundaryQuery
*/
func (t *pkTableMulti) TransformUpperBoundaryResult(
	dbSrc *sql.DB,
	tub *tableUpperBoundary,
) (resultset [][]any) { // {{{
	chunksize := envArg.ArgChunksize
	pkColumnNames := t.GetPKColumnNames()

	lastPKfieldtype := t.GetPKColumns()[len(t.GetPKColumns())-1].FieldType
	var pkColumnOperators []string

	// lambda function code for reuse, return
	// 1. COUNT(1)
	// 2. PK fields (not include last field)
	// 3. last PK field lowerboundary
	// 4. last PK field upperboundary
	// 5. UpperBoundaryQuery
	populateResultset := func(rub [][]any) int { // {{{
		rowcntSrc := 0
		for r := 0; r < len(rub); r++ { // row level
			originalrow := rub[r]
			var rowinresultset []any

			rowcntSrc += *originalrow[0].(*int)

			// 1. COUNT(1)
			rowinresultset = append(rowinresultset, *originalrow[0].(*int))

			// 2. PK fields (not include last field)
			for c := 1; c < len(originalrow)-1; c++ {
				v := *originalrow[c].(*any)
				ft := t.GetPKColumns()[c-1].FieldType
				rowinresultset = append(rowinresultset, ft.transformDBResultType(v))
			}

			// 3. last PK field lowerboundary
			var lb any
			// fmt.Printf("tub.LowerBoundary: %v\n", tub.LowerBoundary)
			if len(tub.LowerBoundary) < len(pkColumnNames) {
				lb = lastPKfieldtype.lowestFieldData()
			} else {
				lb = tub.LowerBoundary[len(pkColumnNames)-1]
			}

			rowinresultset = append(rowinresultset, lb)

			// 4. last PK field upperboundary
			v := *originalrow[len(originalrow)-1].(*any)
			log.Debugf(
				"====return lowerboundary: %v, column type: %T, value: %v====\n",
				lb,
				v,
				lastPKfieldtype.transformDBResultType(v),
			)
			rowinresultset = append(rowinresultset, lastPKfieldtype.transformDBResultType(v))

			// 5. UpperBoundaryQuery
			rowinresultset = append(rowinresultset, tub.UpperBoundaryQuery)

			resultset = append(resultset, rowinresultset)
		}

		return rowcntSrc
	} // }}}

	runUpperBoundary := func(runidx int) int { // {{{
		// if runidx > 1 {
		// 	log.SetLevel(log.DebugLevel)
		// }

		tub.UpperBoundaryQuery = t.UpperBoundaryQuery(pkColumnNames, pkColumnOperators, chunksize)
		log.Debugf("----[%d] lowerboundary: %v----\n", runidx, tub.LowerBoundary)
		log.Debugf("----[%d] UpperBoundaryQuery: %v----\n", runidx, tub.UpperBoundaryQuery)

		rub := t.UpperBoundaryResult(dbSrc, pkColumnNames, tub)
		log.Debugf("----[%d] UpperBoundaryQuery formated: %v----\n", runidx, tub.UpperBoundaryQuery)
		log.Debugf("----[%d] chunksize: %d----\n", runidx, chunksize)

		// possible result loop size of more than 1
		rowcntSrc := populateResultset(rub)

		log.Debugf("====[%d] rowcntSrc: %d, resultset: %v====\n", runidx, rowcntSrc, resultset)
		log.Tracef("====[%d] query: %v====\n", runidx, tub.UpperBoundaryQuery)

		// if runidx > 1 {
		// 	log.SetLevel(log.InfoLevel)
		// }

		return rowcntSrc
	} // }}}

	runUpperBoundaryFor2PKFields := func() { // {{{
		rowcntSrc := runUpperBoundary(2)

		if rowcntSrc == chunksize {
			return
		}

		chunksize -= rowcntSrc

		// at this point, 1st PK field has new group value, so no 2nd and beyond PK field filter in WHERE statement
		tub.LowerBoundary = tub.LowerBoundary[:len(tub.LowerBoundary)-1]

		pkColumnOperators = []string{">"}
		runUpperBoundary(1)
	} // }}}

	runUpperBoundaryFor3PKFields := func() { // {{{
		rowcntSrc := runUpperBoundary(3)

		if rowcntSrc == chunksize {
			return
		}

		chunksize -= rowcntSrc

		// at this point, 2nd PK field has new group value, so no 3rd and beyond PK field filter in WHERE statement
		tub.LowerBoundary = tub.LowerBoundary[:len(tub.LowerBoundary)-1]

		pkColumnOperators = []string{"=", ">"}
		runUpperBoundaryFor2PKFields()
	} // }}}

	runUpperBoundaryFor4PKFields := func() { // {{{
		rowcntSrc := runUpperBoundary(4)

		if rowcntSrc == chunksize {
			return
		}

		chunksize -= rowcntSrc

		// at this point, 3rd PK field has new group value, so no 4nd and beyond PK field filter in WHERE statement
		tub.LowerBoundary = tub.LowerBoundary[:len(tub.LowerBoundary)-1]

		pkColumnOperators = []string{"=", "=", ">"}
		runUpperBoundaryFor3PKFields()
	} // }}}

	switch len(pkColumnNames) {
	case 2:
		pkColumnOperators = []string{"=", ">="}
		runUpperBoundaryFor2PKFields()
	case 3:
		pkColumnOperators = []string{"=", "=", ">="}
		runUpperBoundaryFor3PKFields()
	case 4:
		pkColumnOperators = []string{"=", "=", "=", ">="}
		runUpperBoundaryFor4PKFields()
	default:
		// WARN:
		log.Fatalln("5 or more composite pk table is not supported")
	}

	return
} // }}}

func (t *pkTableMulti) ResetLowerboundaryUpperboundary(
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

	// -u is empty, return
	if len(envArg.ArgUpperBoundary) == 1 && envArg.ArgUpperBoundary[0] == "" {
		return
	}

	// compare envArg.ArgUpperBoundary excluding last PK field with resultset rows
	// exclude 1st field[COUNT(1)]
	// exclude (2nd to the last) and last fields, respective lowerboundary and upperboundary of last PK field
	// single PK skip the loop
	matchedfieldcnt := 0
	for c := 1; c < len(row)-2; c++ { // column level
		ft := t.GetPKColumns()[c-1].FieldType
		userUB := envArg.ArgUpperBoundary[c-1]
		if ft.equals(row[c], userUB) {
			matchedfieldcnt++
			log.Debugf("----match matchedfieldcnt: %d----\n", matchedfieldcnt)
		} else if !ft.greaterThan(row[c], userUB) {
			log.Debugf("----user UB value is higher: %d----\n", matchedfieldcnt)
			return
		} else {
			// meet following criteria:
			// 1. exclude last PK field, already taken care of by the loop
			// 2. 1st ... (n-1)th PK field value from user upperboundary = 1st ... (n-1)th PK field value from resultset
			// 3. nth PK field value from user upperboundary < nth PK field value from resultset
			if matchedfieldcnt == c-1 && ft.greaterThan(row[c], userUB) {
				log.Debugf("----matchedfieldcnt: %d----\n", matchedfieldcnt)
				return
			}
		}
	}

	lastPKfieldtype := t.GetPKColumns()[len(t.GetPKColumns())-1].FieldType
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
