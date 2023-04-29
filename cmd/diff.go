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

package cmd

import (
	"diffchecker/internal/app/diff"
	"diffchecker/internal/pkg/common"

	"github.com/spf13/cobra"
)

// diffCmd represents the diff command
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Diff two MySQL compatible database tables data",
	Long: `Diff two MySQL compatible database tables data

  database tables are compared and the differences results are stored in json output files.
  `,
	Run: func(cmd *cobra.Command, args []string) {
		common.ParseEnvVar()

		// get all flag values
		argDebug, _ := cmd.Flags().GetBool("debug")
		argTrace, _ := cmd.Flags().GetBool("trace")
		argLowerboundary, _ := cmd.Flags().GetString("lower-boundary")
		argUpperboundary, _ := cmd.Flags().GetString("upper-boundary")
		argTable, _ := cmd.Flags().GetString("table")
		argSrcTable, _ := cmd.Flags().GetString("source-table")
		argTgtTable, _ := cmd.Flags().GetString("target-table")
		argChunksize, _ := cmd.Flags().GetInt("chunk-size")
		argPKColumnSequence, _ := cmd.Flags().GetString("pkcolumn-sequence")
		argIgnoreFields, _ := cmd.Flags().GetString("ignore-fields")
		argAdditionalFilter, _ := cmd.Flags().GetString("additional-filter")
		argOutputfile, _ := cmd.Flags().GetString("output")

		// print all flag values
		// fmt.Printf("argDebug: %v\n", argDebug)
		// fmt.Printf("argTrace: %v\n", argTrace)
		// fmt.Printf("argLowerboundary: %v\n", argLowerboundary)
		// fmt.Printf("argUpperboundary: %v\n", argUpperboundary)
		// fmt.Printf("argTable: %v\n", argTable)
		// fmt.Printf("argSrcTable: %v\n", argSrcTable)
		// fmt.Printf("argTgtTable: %v\n", argTgtTable)
		// fmt.Printf("argChunksize: %v\n", argChunksize)
		// fmt.Printf("argPKColumnSequence: %v\n", argPKColumnSequence)
		// fmt.Printf("argIgnoreFields: %v\n", argIgnoreFields)
		// fmt.Printf("argAdditionalFilter: %v\n", argAdditionalFilter)
		// fmt.Printf("argOutputfile: %v\n", argOutputfile)
		//
		// fmt.Printf("EnvVar: %v\n", common.GetEnvVar())

		// assign flag values to diff struct
		diff.SetArgs(
			argDebug,
			argTrace,
			argLowerboundary,
			argUpperboundary,
			argTable,
			argSrcTable,
			argTgtTable,
			argChunksize,
			argPKColumnSequence,
			argIgnoreFields,
			argAdditionalFilter,
		)

		diff.RunTable(argOutputfile)
	},
}

func init() {
	rootCmd.AddCommand(diffCmd)

	diffCmd.Flags().BoolP("debug", "v", false, "verbose for DebugLevel")
	diffCmd.Flags().Lookup("debug").NoOptDefVal = "true" // set to true with -v, --debug flag explicitly

	diffCmd.Flags().Bool("trace", false, "verbose for TraceLevel")
	diffCmd.Flags().Lookup("trace").NoOptDefVal = "true" // set to true with --trace flag explicitly

	diffCmd.Flags().
		StringP("lower-boundary", "l", "", "primary key fields start with lower boundary values, seperated by commas")
	diffCmd.Flags().
		StringP("upper-boundary", "u", "", "primary key fields end at upper boundary values, seperated by commas")
	diffCmd.Flags().String("table", "", "tablename (same for source/target) for data diff")
	diffCmd.Flags().StringP("source-table", "s", "", "source tablename for data diff")
	diffCmd.Flags().StringP("target-table", "t", "", "target tablename for data diff")
	diffCmd.Flags().IntP("chunk-size", "c", 1000, "chunk size for tablename")
	diffCmd.Flags().
		StringP("pkcolumn-sequence", "S", "", "primary key fields sequence used for the chunk query, seperated by commas")
	diffCmd.Flags().
		StringP("ignore-fields", "I", "", "ignore fields in the chunk query, seperated by commas")
	diffCmd.Flags().
		StringP("additional-filter", "F", "", "additional cutomized filter statement used in chunk query")
	diffCmd.Flags().StringP("output", "o", "log.json", "output log file")
}

// vim: fdm=marker fdc=2
