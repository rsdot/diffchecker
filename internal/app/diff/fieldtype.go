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

import (
	"fmt"
	"math"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//  ╔══════════════════════════════════════════════════════════════════════════════╗
//  ║ interfaces                                                                   ║
//  ╚══════════════════════════════════════════════════════════════════════════════╝

// iFieldType : field type attribute interface
type iFieldType interface {
	transformDBResultType(v any) any
	transformFieldType(v any) any
	lowestFieldData() any
	greaterThan(v1 any, v2 any) bool
	equals(v1 any, v2 any) bool
	withQuote() bool
}

// ┌──────────────────────────────────────────────────────────────────────────────┐
//	int data type

type fieldtypeInt struct{}

// implement interface {{{

func (t *fieldtypeInt) transformDBResultType(v any) any { // {{{
	return v
} // }}}

func (t *fieldtypeInt) transformFieldType(v any) any { // {{{
	v2, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
	return v2
} // }}}

func (t *fieldtypeInt) lowestFieldData() any { // {{{
	return math.MinInt64
} // }}}

func (t *fieldtypeInt) greaterThan(v1 any, v2 any) bool { // {{{
	return t.transformFieldType(v1).(int64) > t.transformFieldType(v2).(int64)
} // }}}

func (t *fieldtypeInt) equals(v1 any, v2 any) bool { // {{{
	return t.transformFieldType(v1).(int64) == t.transformFieldType(v2).(int64)
} // }}}

func (t *fieldtypeInt) withQuote() bool { // {{{
	return false
} // }}}

// }}}

//  └──────────────────────────────────────────────────────────────────────────────┘

// ┌──────────────────────────────────────────────────────────────────────────────┐
//	char data type

type fieldtypeChar struct{}

// implement interface {{{

func (t *fieldtypeChar) transformDBResultType(v any) any { // {{{
	return string(v.([]uint8))
} // }}}

func (t *fieldtypeChar) transformFieldType(v any) any { // {{{
	return v.(string)
} // }}}

func (t *fieldtypeChar) lowestFieldData() any { // {{{
	return ""
} // }}}

func (t *fieldtypeChar) greaterThan(v1 any, v2 any) bool { // {{{
	return v1.(string) > v2.(string)
} // }}}

func (t *fieldtypeChar) equals(v1 any, v2 any) bool { // {{{
	return v1.(string) == v2.(string)
} // }}}

func (t *fieldtypeChar) withQuote() bool { // {{{
	return true
} // }}}

// }}}

//  └──────────────────────────────────────────────────────────────────────────────┘

// ┌──────────────────────────────────────────────────────────────────────────────┐
//	time data type

type fieldtypeTime struct{}

// implement interface {{{

func (t *fieldtypeTime) transformDBResultType(v any) any { // {{{
	return t.transformFieldType(v).(time.Time).Format("2006-01-02T15:04:05-07:00")
} // }}}

func (t *fieldtypeTime) transformFieldType(v any) any { // {{{
	r, e := time.Parse("2006-01-02", fmt.Sprint(v))
	if e != nil {
		r, e = time.Parse("2006-01-02 15:04:05", fmt.Sprint(v))
		if e != nil {
			r, e = time.Parse("2006-01-02T15:04:05Z", fmt.Sprint(v))
			if e != nil {
				r, e = time.Parse("2006-01-02T15:04:05-07:00", fmt.Sprint(v))
				if e != nil {
					r, e = time.Parse("2006-01-02 15:04:05 -0700 MST", fmt.Sprint(v))
					if e != nil {
						r, e = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", fmt.Sprint(v))
						errorCheck(e)
					}
				}
			}
		}
	}
	return r
} // }}}

func (t *fieldtypeTime) lowestFieldData() any { // {{{
	// https://stackoverflow.com/questions/25065055/what-is-the-maximum-time-time-in-go
	return t.transformDBResultType(time.Unix(0, 0))
} // }}}

func (t *fieldtypeTime) greaterThan(v1 any, v2 any) bool { // {{{
	return t.transformFieldType(v1).(time.Time).After(t.transformFieldType(v2).(time.Time))
} // }}}

func (t *fieldtypeTime) equals(v1 any, v2 any) bool { // {{{
	return t.transformFieldType(v1).(time.Time).Equal(t.transformFieldType(v2).(time.Time))
} // }}}

func (t *fieldtypeTime) withQuote() bool { // {{{
	return true
} // }}}

// }}}

//  └──────────────────────────────────────────────────────────────────────────────┘

// ┌──────────────────────────────────────────────────────────────────────────────┐
//	date data type

type fieldtypeDate struct{}

// implement interface {{{

func (t *fieldtypeDate) transformDBResultType(v any) any { // {{{
	return t.transformFieldType(v).(time.Time).Format("2006-01-02")
} // }}}

func (t *fieldtypeDate) transformFieldType(v any) any { // {{{
	r, e := time.Parse("2006-01-02", fmt.Sprint(v))
	if e != nil {
		r, e = time.Parse("2006-01-02 15:04:05", fmt.Sprint(v))
		if e != nil {
			r, e = time.Parse("2006-01-02T15:04:05Z", fmt.Sprint(v))
			if e != nil {
				r, e = time.Parse("2006-01-02T15:04:05-07:00", fmt.Sprint(v))
				if e != nil {
					r, e = time.Parse("2006-01-02 15:04:05 -0700 MST", fmt.Sprint(v))
					if e != nil {
						r, e = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", fmt.Sprint(v))
						errorCheck(e)
					}
				}
			}
		}
	}
	return r
} // }}}

func (t *fieldtypeDate) lowestFieldData() any { // {{{
	// https://stackoverflow.com/questions/25065055/what-is-the-maximum-time-time-in-go
	return t.transformDBResultType(time.Unix(0, 0))
} // }}}

func (t *fieldtypeDate) greaterThan(v1 any, v2 any) bool { // {{{
	return t.transformFieldType(v1).(time.Time).After(t.transformFieldType(v2).(time.Time))
} // }}}

func (t *fieldtypeDate) equals(v1 any, v2 any) bool { // {{{
	return t.transformFieldType(v1).(time.Time).Equal(t.transformFieldType(v2).(time.Time))
} // }}}

func (t *fieldtypeDate) withQuote() bool { // {{{
	return true
} // }}}

// }}}

//  └──────────────────────────────────────────────────────────────────────────────┘

// vim: fdm=marker fdc=2
