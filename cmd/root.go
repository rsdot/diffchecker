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
	"diffchecker/internal/pkg/common"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "diffchecker",
	Short: "A CLI tool to compare database table data",
	Long: `A CLI tool to compare database table data.

prerequisite is to set the following environment variables for source and target DBs:

export DFC_SRC_USERNAME=...
export DFC_SRC_PASSWORD=...
export DFC_SRC_HOST=...
export DFC_SRC_PORT=...
export DFC_SRC_DBNAME=...
export DFC_TGT_USERNAME=...
export DFC_TGT_PASSWORD=...
export DFC_TGT_HOST=...
export DFC_TGT_PORT=...
export DFC_TGT_DBNAME=...

  `,
	Run: func(cmd *cobra.Command, args []string) {
		common.ParseEnvVar()
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {}

// vim: fdm=marker fdc=2
