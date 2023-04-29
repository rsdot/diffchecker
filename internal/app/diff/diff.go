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

// Package diff is a tool to compare hashes between two MySQL compatible database tables
package diff

// Importing fmt package for the sake of printing
import (
	"database/sql"
	"diffchecker/internal/pkg/common"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type envarg struct { // {{{
	ArgDebug              bool
	ArgTrace              bool
	ArgLowerBoundary      []string
	ArgUpperBoundary      []string
	ArgSrcTable           string
	ArgTgtTable           string
	ArgChunksize          int
	ArgPKColumnSequence   []string
	ArgIgnoreFields       []string
	ArgAdditionalFilter   string
	ArgOutputfile         *os.File
	ArgOutputRowLevelfile *os.File
} // }}}

// envArg is the package variable that holds the arg variables
var envArg = envarg{}

// envVar is the package variable that holds the environment variables
var envVar = common.GetEnvVar()

func errorCheck(err error) { // {{{
	if err != nil {
		panic(err.Error())
	}
} // }}}

// InitializeDBSettings is to initialize the database connection
func InitializeDBSettings(host, port, username, password, dbname string) *sql.DB { // {{{
	conn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&group_concat_max_len=1000000",
		username,
		password,
		host,
		port,
		dbname,
	)
	db, e := sql.Open("mysql", conn)
	errorCheck(e)

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	e = db.Ping()
	errorCheck(e)

	return db
} // }}}

func singleTableColumnResult(
	db *sql.DB,
	table string,
	query string,
) (fieldcolumns []string) { // {{{
	stmt, e := db.Prepare(query)
	errorCheck(e)
	defer func() {
		e := stmt.Close()
		errorCheck(e)
	}()

	result, e := stmt.Query(table)
	errorCheck(e)

	for result.Next() {
		var columnname string
		e = result.Scan(&columnname)
		errorCheck(e)
		fieldcolumns = append(fieldcolumns, columnname)
	}

	log.Debugln(fieldcolumns)

	return
} // }}}

// GetTableColumns returns table column names for a given table
func GetTableColumns(db *sql.DB, table string) (fieldcolumns []string) { // {{{
	query := `
    SELECT SQL_NO_CACHE COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = database()
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    `
	fieldcolumns = singleTableColumnResult(db, table, query)

	return
} // }}}

// allPKColumns populate pkcolumn struct
func allPKColumns(db *sql.DB, table string) (allpkcolumns []pkColumn) { // {{{
	query := `
    SELECT SQL_NO_CACHE
      col.column_name,
      col.data_type
    FROM information_schema.tables as tab
    INNER JOIN information_schema.statistics as sta
    ON sta.table_schema = tab.table_schema
      and sta.table_name = tab.table_name
      and sta.index_name = 'primary'
    INNER JOIN information_schema.columns as col
    ON sta.table_schema = col.table_schema
      and sta.table_name = col.table_name
      and sta.column_name = col.column_name
    WHERE tab.table_schema = database()
      and tab.table_type = 'BASE TABLE'
      and tab.table_name = ?
    ORDER BY
      tab.table_name,
      sta.seq_in_index;
    `

	stmt, e := db.Prepare(query)
	errorCheck(e)
	defer func() {
		e := stmt.Close()
		errorCheck(e)
	}()

	result, e := stmt.Query(table)
	errorCheck(e)

	for result.Next() {
		var columnname string
		var datatype string
		e = result.Scan(&columnname, &datatype)
		errorCheck(e)

		var ft iFieldType

		if strings.Contains(datatype, "char") {
			ft = new(fieldtypeChar)
		} else if strings.Contains(datatype, "int") {
			ft = new(fieldtypeInt)
		} else if strings.Contains(datatype, "time") {
			ft = new(fieldtypeTime)
		} else if strings.Contains(datatype, "date") {
			ft = new(fieldtypeDate)
		} else {
			// TODO: add support for other data types
			log.Fatalf("Unsupported data type: %s\n", datatype)
		}

		allpkcolumns = append(
			allpkcolumns,
			pkColumn{
				ColumnName:  columnname,
				DataType:    datatype,
				FieldType:   ft,
				IsLastField: false,
			},
		)
	}
	// flag last field
	allpkcolumns[len(allpkcolumns)-1].IsLastField = true

	log.Debugln(allpkcolumns)

	return
} // }}}

// FindAllPKColumnNames find table's all PK column names
func FindAllPKColumnNames(db *sql.DB, table string) []string { // {{{
	allpkcolumns := allPKColumns(db, table)

	pkColumnNames := make([]string, len(allpkcolumns))

	for i, pkcolumn := range allpkcolumns {
		pkColumnNames[i] = pkcolumn.ColumnName
	}

	return pkColumnNames
} // }}}

// FindAllPKColumnQuotes find table's all PK column quotes
func FindAllPKColumnQuotes(db *sql.DB, table string) []string { // {{{
	allpkcolumns := allPKColumns(db, table)

	pkColumnValuesQuotes := make([]string, len(allpkcolumns))

	for i, pkcolumn := range allpkcolumns {
		if pkcolumn.FieldType.withQuote() {
			pkColumnValuesQuotes[i] = "'"
		} else {
			pkColumnValuesQuotes[i] = ""
		}
	}

	return pkColumnValuesQuotes
} // }}}

// SetArgs : assign CLI arguments
func SetArgs(
	argDebug bool,
	argTrace bool,
	argLowerboundary string,
	argUpperboundary string,
	argTable string,
	argSrcTable string,
	argTgtTable string,
	argChunksize int,
	argPKColumnSequence string,
	argIgnoreFields string,
	argAdditionalFilter string,
) { // {{{

	envArg.ArgDebug = argDebug
	envArg.ArgTrace = argTrace
	envArg.ArgLowerBoundary = strings.Split(argLowerboundary, ",")
	envArg.ArgUpperBoundary = strings.Split(argUpperboundary, ",")
	if argChunksize <= 1 {
		envArg.ArgChunksize = 2
	} else {
		envArg.ArgChunksize = argChunksize
	}
	envArg.ArgPKColumnSequence = strings.Split(argPKColumnSequence, ",")
	envArg.ArgIgnoreFields = strings.Split(argIgnoreFields, ",")
	envArg.ArgAdditionalFilter = argAdditionalFilter

	if argLowerboundary != "" && argUpperboundary != "" &&
		len(envArg.ArgLowerBoundary) != len(envArg.ArgUpperBoundary) {
		log.Fatalln("-l and -u should have same number of elements")
	}

	if argPKColumnSequence != "" {
		if argLowerboundary != "" &&
			len(envArg.ArgLowerBoundary) != len(envArg.ArgPKColumnSequence) {
			log.Fatalln("-l and -S should have same number of elements")
		}
		if argUpperboundary != "" &&
			len(envArg.ArgUpperBoundary) != len(envArg.ArgPKColumnSequence) {
			log.Fatalln("-u and -S should have same number of elements")
		}
	}

	if argTable == "" && argSrcTable == "" && argTgtTable == "" {
		log.Fatalln("--table or -s/-t is required")
	}

	if !((argTable != "" && argSrcTable == "" && argTgtTable == "") || (argTable == "" && argSrcTable != "" && argTgtTable != "")) {
		log.Fatalln("--table and -s/-t are mutual exclusive")
	}

	if argTable != "" {
		envArg.ArgSrcTable = argTable
		envArg.ArgTgtTable = argTable
	} else {
		envArg.ArgSrcTable = argSrcTable
		envArg.ArgTgtTable = argTgtTable
	}
} // }}}

func setLogSettings() { // {{{
	// https://www.golinuxcloud.com/golang-logrus/

	// log.SetFormatter(&log.JSONFormatter{
	//   TimestampFormat: "2006-01-02T15:04:05.9999999Z07:00",
	//   DisableHTMLEscape: true,
	// })
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:     true,
		DisableColors:   false,
	})

	log.WithFields(log.Fields{
		"F": envArg.ArgAdditionalFilter,
		"I": strings.Join(envArg.ArgIgnoreFields, ","),
		"S": strings.Join(envArg.ArgPKColumnSequence, ","),
		"c": envArg.ArgChunksize,
		"l": strings.Join(envArg.ArgLowerBoundary, ","),
		"o": envArg.ArgOutputfile.Name() + ", " + envArg.ArgOutputRowLevelfile.Name(),
		"s": envArg.ArgSrcTable,
		"t": envArg.ArgTgtTable,
		"u": strings.Join(envArg.ArgUpperBoundary, ","),
	},
	).Infoln("[match]=[index]=[lowerboundary]=[upperboundary]===[rowstats]===")

	log.SetReportCaller(true) // show line number

	if envArg.ArgTrace {
		log.SetLevel(log.TraceLevel)
	} else if envArg.ArgDebug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
} // }}}

// RunTable : calculate hash for a table
func RunTable(outputfile string) { // {{{
	removeFile := func(filename string) { // {{{
		var e error
		_, e = os.Stat(filename)

		// if file exists, remove it
		if e == nil {
			e = os.Remove(filename)
			errorCheck(e)
		}
	} // }}}

	re := regexp.MustCompile(`\.json`)
	rowlevelfile := re.ReplaceAllString(outputfile, ".rowlevel.json")

	removeFile(outputfile)
	removeFile(rowlevelfile)

	var e error
	envArg.ArgOutputfile, e = os.OpenFile(
		outputfile,
		os.O_CREATE|os.O_WRONLY|os.O_SYNC,
		0o666,
	)
	errorCheck(e)
	defer func() {
		e := envArg.ArgOutputfile.Close()
		errorCheck(e)
	}()

	envArg.ArgOutputRowLevelfile, e = os.OpenFile(
		rowlevelfile,
		os.O_CREATE|os.O_WRONLY|os.O_SYNC,
		0o666,
	)
	errorCheck(e)
	defer func() {
		e := envArg.ArgOutputRowLevelfile.Close()
		errorCheck(e)
	}()

	setLogSettings()

	dbSrc := InitializeDBSettings(
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

	dbTgt := InitializeDBSettings(
		envVar.DfcTgtHost,
		envVar.DfcTgtPort,
		envVar.DfcTgtUsername,
		envVar.DfcTgtPassword,
		envVar.DfcTgtDbname,
	)
	defer func() {
		e := dbTgt.Close()
		errorCheck(e)
	}()

	allpkcolumns := allPKColumns(dbSrc, envArg.ArgSrcTable)

	var t ipkTable
	switch len(allpkcolumns) {
	case 1:
		t = new(pkTableSingle).Init(allpkcolumns)
	default:
		t = new(pkTableMulti).Init(allpkcolumns)
	}

	// abort if:
	// 1. argPKColumnSequence is less than actual pk columns
	// 2. chunksize < top 1 count of group by argPKColumnSequence columns
	if len(t.GetPKColumnNames()) < len(allpkcolumns) {
		maxgroupcount := t.PKColumnMaxGroupCount(dbSrc)
		if envArg.ArgChunksize <= maxgroupcount {
			log.Fatalf(
				"chunksize should be greater than max count(%d) of group by (%s) columns\n",
				maxgroupcount,
				strings.Join(t.GetPKColumnNames(), ", "),
			)
		}
	}

	t.RunTableRoutine(dbSrc, dbTgt, t)
} // }}}

// vim: fdm=marker fdc=2
