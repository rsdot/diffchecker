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
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// tableChunkInfo : json marshalable struct for table chunks
type tableChunkInfo struct { // {{{
	Match                    bool      `json:"match"`
	ChunkIdx                 int       `json:"chunkidx"`
	TimestampSrc             time.Time `json:"timestampsrc"`
	TimestampTgt             time.Time `json:"timestamptgt"`
	ElapsedMsSrc             int64     `json:"elapsedmssrc"`
	ElapsedMsTgt             int64     `json:"elapsedmstgt"`
	TableSrc                 string    `json:"tablesrc"`
	TableTgt                 string    `json:"tabletgt"`
	PKColumnNames            []string  `json:"pkcolumnnames"`
	PKColumnSequence         []string  `json:"pkcolumnsequence"`
	RowcntSrc                int       `json:"rowcntsrc"`
	RowcntTgt                int       `json:"rowcnttgt"`
	HashSrc                  int       `json:"hashsrc"`
	HashTgt                  int       `json:"hashtgt"`
	IgnoreFields             []string  `json:"ignorefields"`
	AdditionalFilter         string    `json:"additionalfilter"`
	LastPKFieldUpperBoundary any       `json:"lastpkfieldupperboundary"`
	tableUpperBoundary
	HashQuerySrc string `json:"hashquerysrc"`
	HashQueryTgt string `json:"hashquerytgt"`
} // }}}

/*
TableHashQueryChunkLevel : construct hash query statement like

	SELECT COUNT(1) AS rowcnt, CRC32(GROUP_CONCAT(CONCAT_WS('#', field1, field2, ..., fieldn)))
	FROM table
	WHERE pkfield1 = ? AND pkfield2 = ? AND ... AND pkfieldn BETWEEN ? AND ?
*/
func (t *pkTable) TableHashQueryChunkLevel(
	db *sql.DB,
	table string,
) (query string) { // {{{

	columnNames, pkColumnsWhere := t.TableQueryColumnNames(db, table)
	var additionalfilterstmt string
	if envArg.ArgAdditionalFilter != "" {
		additionalfilterstmt = " AND " + envArg.ArgAdditionalFilter
	}

	query = `
    SELECT SQL_NO_CACHE
      COUNT(1) AS rowcnt,
      COALESCE(
        CAST(CRC32(
          GROUP_CONCAT(
            CAST(CRC32(
              CONCAT_WS('#',` + strings.Join(columnNames, ",") + `)
              ) AS UNSIGNED)
            )
          ) AS UNSIGNED),
        0) AS crc32
    FROM ` + table + `
    WHERE ` + strings.Join(pkColumnsWhere, " AND ") + additionalfilterstmt

	log.Traceln(query)

	return
} // }}}

// TableResultChunkLevel : execute hash query statement and stores result in struct tablehashresult
func (t *pkTable) TableResultChunkLevel(
	db *sql.DB,
	issrc bool,
	tci *tableChunkInfo,
) (result tableHashResult) { // {{{

	stmt, inputs := t.TableHashStmt(
		db,
		issrc,
		&tci.HashQuerySrc,
		&tci.HashQueryTgt,
		tci.LowerBoundary,
		tci.LastPKFieldUpperBoundary,
	)
	if stmt != nil {
		defer func() {
			e := stmt.Close()
			errorCheck(e)
		}()
	}

	var rowcnt int
	var hash int

	ts := time.Now()
	e := stmt.QueryRow(inputs...).Scan(&rowcnt, &hash)
	errorCheck(e)
	elapsedms := time.Since(ts).Milliseconds()

	result.issrc = issrc
	result.ts = ts
	result.elapsedms = elapsedms

	result.rowcnt = rowcnt
	result.hash = hash

	return
} // }}}

// TableRoutineChunkLevel : co-routine executing hash query against both source and target DB
func (t *pkTable) TableRoutineChunkLevel(
	dbSrc *sql.DB,
	dbTgt *sql.DB,
	tci *tableChunkInfo,
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
		hashchan <- t.TableResultChunkLevel(dbSrc, true, tci)
	}()

	go func() {
		defer waitgroup.Done()
		hashchan <- t.TableResultChunkLevel(dbTgt, false, tci)
	}()

	for result := range hashchan {
		if result.issrc {
			tci.RowcntSrc, tci.HashSrc, tci.ElapsedMsSrc, tci.TimestampSrc = result.rowcnt, result.hash, result.elapsedms, result.ts
		} else {
			tci.RowcntTgt, tci.HashTgt, tci.ElapsedMsTgt, tci.TimestampTgt = result.rowcnt, result.hash, result.elapsedms, result.ts
		}
	}

	tci.Match = (tci.RowcntSrc == tci.RowcntTgt) && (tci.HashSrc == tci.HashTgt)

	if !tci.Match {
		t.RunTableRoutineRowLevel(dbSrc, dbTgt, tci)
	}
} // }}}

func (t *pkTable) RunTableRoutineChunkLevel(
	dbSrc *sql.DB,
	dbTgt *sql.DB,
	tci *tableChunkInfo,
) { // {{{
	t.TableRoutineChunkLevel(dbSrc, dbTgt, tci)

	if !envArg.ArgDebug {
		tci.UpperBoundaryQuery = ""
		tci.HashQuerySrc = ""
		tci.HashQueryTgt = ""
	}
	t.TableLog(envArg.ArgOutputfile, tci)
} // }}}

// vim: fdm=marker fdc=2
