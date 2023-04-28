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
	"diffchecker/internal/app/query"
	"diffchecker/internal/pkg/common"
	"log"

	"github.com/spf13/cobra"
)

// queryCmd represents the sync command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Generate sql query for different CRUD type",
	Long: `Generate sql query for different CRUD type

  based on diff output rowlevel data
  `,
	Run: func(cmd *cobra.Command, args []string) {
		common.ParseEnvVar()

		argInsert, _ := cmd.Flags().GetBool("insert")
		argUpdate, _ := cmd.Flags().GetBool("update")
		argDelete, _ := cmd.Flags().GetBool("delete")
		argRowlevelFile, _ := cmd.Flags().GetString("rowlevel-file")

		// print all flag values
		// fmt.Printf("argInsert: %v\n", argInsert)
		// fmt.Printf("argUpdate: %v\n", argUpdate)
		// fmt.Printf("argDelete: %v\n", argDelete)
		// fmt.Printf("argRowlevelFile: %v\n", argRowlevelFile)

		// assign flag values to query struct
		query.SetArgs(
			argInsert,
			argUpdate,
			argDelete,
			argRowlevelFile,
		)

		if !(argInsert || argUpdate || argDelete) {
			log.Fatalln(
				"-i/-u/-d cannot be empty at the same time, any combination of 3 would be allowed",
			)
		}

		query.GenerateSQL()
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)

	queryCmd.Flags().StringP("rowlevel-file", "f", "", "rowlevel file")

	queryCmd.Flags().BoolP("insert", "i", false, "INSERT ONLY sql to target table")
	queryCmd.Flags().Lookup("insert").NoOptDefVal = "true" // set to true with -i, --insert flag explicitly

	queryCmd.Flags().BoolP("update", "u", false, "UPDATE ONLY sql to target table")
	queryCmd.Flags().Lookup("update").NoOptDefVal = "true" // set to true with -u, --update flag explicitly

	queryCmd.Flags().BoolP("delete", "d", false, "DELETE ONLY sql to target table")
	queryCmd.Flags().Lookup("delete").NoOptDefVal = "true" // set to true with -d, --delete flag explicitly
}

// vim: fdm=marker fdc=2
