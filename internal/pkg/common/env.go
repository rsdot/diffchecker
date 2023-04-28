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

package common

import (
	"log"
	"os"
)

type EnvVar struct { // {{{
	DfcSrcUsername string
	DfcSrcPassword string
	DfcSrcHost     string
	DfcSrcPort     string
	DfcSrcDbname   string
	DfcTgtUsername string
	DfcTgtPassword string
	DfcTgtHost     string
	DfcTgtPort     string
	DfcTgtDbname   string
} // }}}

// envVar is the global variable that holds the environment and arg variables
var envVar = new(EnvVar)

// GetEnvVar return struct with environment variables
func GetEnvVar() *EnvVar { // {{{
	return envVar
} // }}}

func envVarCheck() { // {{{
	log.Fatalf(`
    env should be set:
      %s
      %s
      %s
      %s
      %s
      %s
      %s
      %s
      %s
      %s
    `,
		"export DFC_SRC_USERNAME=",
		"export DFC_SRC_PASSWORD=",
		"export DFC_SRC_HOST=",
		"export DFC_SRC_PORT=",
		"export DFC_SRC_DBNAME=",
		"export DFC_TGT_USERNAME=",
		"export DFC_TGT_PASSWORD=",
		"export DFC_TGT_HOST=",
		"export DFC_TGT_PORT=",
		"export DFC_TGT_DBNAME=")
} // }}}

// ParseEnvVar fetch the environment variables
func ParseEnvVar() { // {{{
	var isset bool
	envVar.DfcSrcUsername, isset = os.LookupEnv("DFC_SRC_USERNAME")
	if !isset {
		envVarCheck()
	}
	envVar.DfcSrcPassword, isset = os.LookupEnv("DFC_SRC_PASSWORD")
	if !isset {
		envVarCheck()
	}
	envVar.DfcSrcHost, isset = os.LookupEnv("DFC_SRC_HOST")
	if !isset {
		envVarCheck()
	}
	envVar.DfcSrcPort, isset = os.LookupEnv("DFC_SRC_PORT")
	if !isset {
		envVarCheck()
	}
	envVar.DfcSrcDbname, isset = os.LookupEnv("DFC_SRC_DBNAME")
	if !isset {
		envVarCheck()
	}
	envVar.DfcTgtUsername, isset = os.LookupEnv("DFC_TGT_USERNAME")
	if !isset {
		envVarCheck()
	}
	envVar.DfcTgtPassword, isset = os.LookupEnv("DFC_TGT_PASSWORD")
	if !isset {
		envVarCheck()
	}
	envVar.DfcTgtHost, isset = os.LookupEnv("DFC_TGT_HOST")
	if !isset {
		envVarCheck()
	}
	envVar.DfcTgtPort, isset = os.LookupEnv("DFC_TGT_PORT")
	if !isset {
		envVarCheck()
	}
	envVar.DfcTgtDbname, isset = os.LookupEnv("DFC_TGT_DBNAME")
	if !isset {
		envVarCheck()
	}
} // }}}

// vim: fdm=marker fdc=2
