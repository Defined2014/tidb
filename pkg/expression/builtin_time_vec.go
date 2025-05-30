// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/tikv/client-go/v2/oracle"
)

func (b *builtinMonthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(ds[i].Month())
	}
	return nil
}

func (b *builtinMonthSig) vectorized() bool {
	return true
}

func (b *builtinYearSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(ds[i].Year())
	}
	return nil
}

func (b *builtinYearSig) vectorized() bool {
	return true
}

func (b *builtinDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}
	times := result.Times()
	for i := range times {
		if result.IsNull(i) {
			continue
		}
		if times[i].IsZero() && sqlMode(ctx).HasNoZeroDateMode() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			times[i].SetCoreTime(types.FromDate(times[i].Year(), times[i].Month(), times[i].Day(), 0, 0, 0, 0))
			times[i].SetType(mysql.TypeDate)
		}
	}
	return nil
}

func (b *builtinDateSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime2ArgSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime2ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[0].VecEvalDecimal(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err = b.args[1].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	ds := buf1.Decimals()
	fsp := b.tp.GetDecimal()
	for i := range n {
		if buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		t, isNull, err := evalFromUnixTime(ctx, fsp, &ds[i])
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		res, err := t.DateFormat(buf2.GetString(i))
		if err != nil {
			return err
		}
		result.AppendString(res)
	}
	return nil
}

func (b *builtinSysDateWithoutFspSig) vectorized() bool {
	return true
}

func (b *builtinSysDateWithoutFspSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	loc := location(ctx)
	now := time.Now().In(loc)

	result.ResizeTime(n, false)
	times := result.Times()
	t, err := convertTimeToMysqlTime(now, 0, types.ModeHalfUp)
	if err != nil {
		return err
	}
	for i := range n {
		times[i] = t
	}
	return nil
}

func (b *builtinExtractDatetimeFromStringSig) vectorized() bool {
	// TODO: to fix https://github.com/pingcap/tidb/issues/9716 in vectorized evaluation.
	return false
}

func (b *builtinExtractDatetimeFromStringSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	ds := buf1.Times()
	result.MergeNulls(buf, buf1)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		res, err := types.ExtractDatetimeNum(&ds[i], buf.GetString(i))
		if err != nil {
			return err
		}
		i64s[i] = res
	}
	return nil
}

func (b *builtinDayNameSig) vectorized() bool {
	return true
}

func (b *builtinDayNameSig) vecEvalIndex(ctx EvalContext, input *chunk.Chunk, apply func(i int, res int), applyNull func(i int)) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	ds := buf.Times()
	for i := range n {
		if buf.IsNull(i) {
			applyNull(i)
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			applyNull(i)
			continue
		}
		// Monday is 0, ... Sunday = 6 in MySQL
		// but in go, Sunday is 0, ... Saturday is 6
		// we will do a conversion.
		res := (int(ds[i].Weekday()) + 6) % 7
		apply(i, res)
	}
	return nil
}

// vecEvalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)

	return b.vecEvalIndex(ctx, input, func(i, res int) {
		result.AppendString(types.WeekdayNames[res])
	}, func(i int) {
		result.AppendNull()
	})
}

func (b *builtinDayNameSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()

	return b.vecEvalIndex(ctx, input, func(i, res int) {
		f64s[i] = float64(res)
	}, func(i int) {
		result.SetNull(i, true)
	})
}

func (b *builtinDayNameSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	return b.vecEvalIndex(ctx, input, func(i, res int) {
		i64s[i] = int64(res)
	}, func(i int) {
		result.SetNull(i, true)
	})
}

func (b *builtinWeekDaySig) vectorized() bool {
	return true
}

func (b *builtinWeekDaySig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64((ds[i].Weekday() + 6) % 7)
	}
	return nil
}

func (b *builtinTimeFormatSig) vectorized() bool {
	return true
}

func (b *builtinTimeFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		// If err != nil, then dur is ZeroDuration, outputs 00:00:00
		// in this case which follows the behavior of mysql.
		// Use the non-vectorized method to handle this kind of error.
		return vecEvalStringByRows(b, ctx, input, result)
	}

	buf1, err1 := b.bufAllocator.get()
	if err1 != nil {
		return err1
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err := b.formatTime(buf.GetDuration(i, 0), buf1.GetString(i))
		if err != nil {
			return err
		}
		result.AppendString(res)
	}
	return nil
}

func (b *builtinUTCTimeWithArgSig) vectorized() bool {
	return true
}

// vecEvalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	utc := nowTs.UTC().Format(types.TimeFSPFormat)
	tc := typeCtx(ctx)
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
	i64s := buf.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		fsp := i64s[i]
		if fsp > int64(math.MaxInt32) || fsp < int64(types.MinFsp) {
			return types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
		}
		if fsp > int64(types.MaxFsp) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "utc_time", types.MaxFsp)
		}
		res, _, err := types.ParseDuration(tc, utc, int(fsp))
		if err != nil {
			return err
		}
		d64s[i] = res.Duration
	}
	return nil
}

func (b *builtinUnixTimestampCurrentSig) vectorized() bool {
	return true
}

func (b *builtinUnixTimestampCurrentSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	dec, err := goTimeToMysqlUnixTimestamp(nowTs, 1)
	if err != nil {
		return err
	}
	intVal, err := dec.ToInt()
	if !terror.ErrorEqual(err, types.ErrTruncated) {
		terror.Log(err)
	}
	n := input.NumRows()
	result.ResizeInt64(n, false)
	intRes := result.Int64s()
	for i := range n {
		intRes[i] = intVal
	}
	return nil
}

func (b *builtinYearWeekWithoutModeSig) vectorized() bool {
	return true
}

// vecEvalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		if date.InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		year, week := date.YearWeek(0)
		i64s[i] = int64(week + year*100)
		if i64s[i] < 0 {
			i64s[i] = int64(math.MaxUint32)
		}
	}
	return nil
}

func (b *builtinPeriodDiffSig) vectorized() bool {
	return true
}

// vecEvalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	i64s := result.Int64s()
	periods := buf.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if !validPeriod(i64s[i]) || !validPeriod(periods[i]) {
			return errIncorrectArgs.GenWithStackByArgs("period_diff")
		}
		i64s[i] = int64(period2Month(uint64(i64s[i])) - period2Month(uint64(periods[i])))
	}
	return nil
}

func (b *builtinNowWithArgSig) vectorized() bool {
	return true
}

func (b *builtinNowWithArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufFsp, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufFsp)
	if err = b.args[0].VecEvalInt(ctx, input, bufFsp); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	times := result.Times()
	fsps := bufFsp.Int64s()

	for i := range n {
		fsp := 0
		if !bufFsp.IsNull(i) {
			if fsps[i] > int64(math.MaxInt32) || fsps[i] < int64(types.MinFsp) {
				return types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
			}
			if fsps[i] > int64(types.MaxFsp) {
				return types.ErrTooBigPrecision.GenWithStackByArgs(fsps[i], "now", types.MaxFsp)
			}
			fsp = int(fsps[i])
		}

		t, isNull, err := evalNowWithFsp(ctx, fsp)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}

		times[i] = t
	}
	return nil
}

func (b *builtinGetFormatSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		format := buf0.GetString(i)
		location := buf1.GetString(i)
		res := b.getFormat(format, location)
		result.AppendString(res)
	}
	return nil
}

func (b *builtinGetFormatSig) getFormat(format, location string) string {
	res := ""
	switch format {
	case dateFormat:
		switch location {
		case usaLocation:
			res = "%m.%d.%Y"
		case jisLocation:
			res = "%Y-%m-%d"
		case isoLocation:
			res = "%Y-%m-%d"
		case eurLocation:
			res = "%d.%m.%Y"
		case internalLocation:
			res = "%Y%m%d"
		}
	case datetimeFormat, timestampFormat:
		switch location {
		case usaLocation:
			res = "%Y-%m-%d %H.%i.%s"
		case jisLocation:
			res = "%Y-%m-%d %H:%i:%s"
		case isoLocation:
			res = "%Y-%m-%d %H:%i:%s"
		case eurLocation:
			res = "%Y-%m-%d %H.%i.%s"
		case internalLocation:
			res = "%Y%m%d%H%i%s"
		}
	case timeFormat:
		switch location {
		case usaLocation:
			res = "%h:%i:%s %p"
		case jisLocation:
			res = "%H:%i:%s"
		case isoLocation:
			res = "%H:%i:%s"
		case eurLocation:
			res = "%H.%i.%s"
		case internalLocation:
			res = "%H%i%s"
		}
	}
	return res
}

func (b *builtinLastDaySig) vectorized() bool {
	return true
}

func (b *builtinLastDaySig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}
	times := result.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		tm := times[i]
		year, month := tm.Year(), tm.Month()
		if tm.Month() == 0 || (tm.Day() == 0 && sqlMode(ctx).HasNoZeroDateMode()) {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		lastDay := types.GetLastDay(year, month)
		times[i] = types.NewTime(types.FromDate(year, month, lastDay, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp)
	}
	return nil
}

func (b *builtinStrToDateDateSig) vectorized() bool {
	return true
}

func (b *builtinStrToDateDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufStrings, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufStrings)
	if err := b.args[0].VecEvalString(ctx, input, bufStrings); err != nil {
		return err
	}

	bufFormats, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufFormats)
	if err := b.args[1].VecEvalString(ctx, input, bufFormats); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(bufStrings, bufFormats)
	times := result.Times()
	tc := typeCtx(ctx)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		var t types.Time
		succ := t.StrToDate(tc, bufStrings.GetString(i), bufFormats.GetString(i))
		if !succ {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if sqlMode(ctx).HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		t.SetType(mysql.TypeDate)
		t.SetFsp(types.MinFsp)
		times[i] = t
	}
	return nil
}

func (b *builtinSysDateWithFspSig) vectorized() bool {
	return true
}

func (b *builtinSysDateWithFspSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	loc := location(ctx)
	now := time.Now().In(loc)

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	ds := buf.Int64s()

	for i := range n {
		if result.IsNull(i) {
			continue
		}
		t, err := convertTimeToMysqlTime(now, int(ds[i]), types.ModeHalfUp)
		if err != nil {
			return err
		}
		times[i] = t
	}
	return nil
}

func (b *builtinTidbParseTsoSig) vectorized() bool {
	return true
}

func (b *builtinTidbParseTsoSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	args := buf.Int64s()
	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if args[i] <= 0 {
			result.SetNull(i, true)
			continue
		}
		t := oracle.GetTimeFromTS(uint64(args[i]))
		r := types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.MaxFsp)
		if err := r.ConvertTimeZone(time.Local, location(ctx)); err != nil {
			return err
		}
		times[i] = r
	}
	return nil
}

func (b *builtinTiDBBoundedStalenessSig) vectorized() bool {
	return true
}

func (b *builtinTiDBBoundedStalenessSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	store, err := b.GetKVStore(ctx)
	if err != nil {
		return err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalTime(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}
	args0 := buf0.Times()
	args1 := buf1.Times()
	timeZone := getTimeZone(ctx)
	minSafeTime := GetStmtMinSafeTime(vars.StmtCtx, store, timeZone)
	result.ResizeTime(n, false)
	result.MergeNulls(buf0, buf1)
	times := result.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if invalidArg0, invalidArg1 := args0[i].InvalidZero(), args1[i].InvalidZero(); invalidArg0 || invalidArg1 {
			if invalidArg0 {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, args0[i].String()))
			}
			if invalidArg1 {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, args1[i].String()))
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		minTime, err := args0[i].GoTime(timeZone)
		if err != nil {
			return err
		}
		maxTime, err := args1[i].GoTime(timeZone)
		if err != nil {
			return err
		}
		if minTime.After(maxTime) {
			result.SetNull(i, true)
			continue
		}
		// Because the minimum unit of a TSO is millisecond, so we only need fsp to be 3.
		times[i] = types.NewTime(types.FromGoTime(calAppropriateTime(minTime, maxTime, minSafeTime)), mysql.TypeDatetime, 3)
	}
	return nil
}

func (b *builtinFromDaysSig) vectorized() bool {
	return true
}

func (b *builtinFromDaysSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	ts := result.Times()
	i64s := buf.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		ts[i] = types.TimeFromDays(i64s[i])
	}
	return nil
}

func (b *builtinMicroSecondSig) vectorized() bool {
	return true
}

func (b *builtinMicroSecondSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return vecEvalIntByRows(ctx, b, input, result)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, int(types.UnspecifiedFsp)).MicroSecond())
	}
	return nil
}

func (b *builtinQuarterSig) vectorized() bool {
	return true
}

// vecEvalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		i64s[i] = int64((date.Month() + 2) / 3)
	}
	return nil
}

func (b *builtinWeekWithModeSig) vectorized() bool {
	return true
}

func (b *builtinWeekWithModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	ds := buf1.Times()
	ms := buf2.Int64s()
	for i := range n {
		if buf1.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		date := ds[i]
		if date.IsZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if buf2.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		mode := int(ms[i])
		week := date.Week(mode)
		i64s[i] = int64(week)
	}
	return nil
}

func (b *builtinExtractDatetimeSig) vectorized() bool {
	return true
}

func (b *builtinExtractDatetimeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(unit)
	if err := b.args[0].VecEvalString(ctx, input, unit); err != nil {
		return err
	}
	dt, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dt)
	if err = b.args[1].VecEvalTime(ctx, input, dt); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(unit, dt)
	i64s := result.Int64s()
	tmIs := dt.Times()
	var t types.Time
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		unitI := unit.GetString(i)
		t = tmIs[i]
		i64s[i], err = types.ExtractDatetimeNum(&t, unitI)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinExtractDurationSig) vectorized() bool {
	return true
}

func (b *builtinExtractDurationSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(unit)
	if err := b.args[0].VecEvalString(ctx, input, unit); err != nil {
		return err
	}
	dur, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dur)
	if err = b.args[1].VecEvalDuration(ctx, input, dur); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(unit, dur)
	i64s := result.Int64s()
	durIs := dur.GoDurations()
	var duration types.Duration
	duration.Fsp = b.args[1].GetType(ctx).GetDecimal()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		unitI := unit.GetString(i)
		duration.Duration = durIs[i]
		i64s[i], err = types.ExtractDurationNum(&duration, unitI)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinStrToDateDurationSig) vectorized() bool {
	return true
}

func (b *builtinStrToDateDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufStrings, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufStrings)
	if err := b.args[0].VecEvalString(ctx, input, bufStrings); err != nil {
		return err
	}

	bufFormats, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufFormats)
	if err := b.args[1].VecEvalString(ctx, input, bufFormats); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	result.MergeNulls(bufStrings, bufFormats)
	d64s := result.GoDurations()
	tc := typeCtx(ctx)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		var t types.Time
		succ := t.StrToDate(tc, bufStrings.GetString(i), bufFormats.GetString(i))
		if !succ {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		t.SetFsp(b.tp.GetDecimal())
		dur, err := t.ConvertToDuration()
		if err != nil {
			return err
		}
		d64s[i] = dur.Duration
	}
	return nil
}

func (b *builtinToSecondsSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		arg := ds[i]
		ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
		if ret == 0 {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = ret
	}
	return nil
}

func (b *builtinMinuteSig) vectorized() bool {
	return true
}

func (b *builtinMinuteSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return vecEvalIntByRows(ctx, b, input, result)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, types.UnspecifiedFsp).Minute())
	}
	return nil
}

func (b *builtinSecondSig) vectorized() bool {
	return true
}

func (b *builtinSecondSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return vecEvalIntByRows(ctx, b, input, result)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, types.UnspecifiedFsp).Second())
	}
	return nil
}

func (b *builtinNowWithoutArgSig) vectorized() bool {
	return true
}

func (b *builtinNowWithoutArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, isNull, err := evalNowWithFsp(ctx, 0)
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range n {
		times[i] = nowTs
	}
	return nil
}

func (b *builtinTimestampLiteralSig) vectorized() bool {
	return true
}

func (b *builtinTimestampLiteralSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range times {
		times[i] = b.tm
	}
	return nil
}

func (b *builtinMakeDateSig) vectorized() bool {
	return true
}

func (b *builtinMakeDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[0].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf1, buf2)

	times := result.Times()
	years := buf1.Int64s()
	days := buf2.Int64s()

	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if days[i] <= 0 || years[i] < 0 || years[i] > 9999 {
			result.SetNull(i, true)
			continue
		}
		if years[i] < 70 {
			years[i] += 2000
		} else if years[i] < 100 {
			years[i] += 1900
		}
		startTime := types.NewTime(types.FromDate(int(years[i]), 1, 1, 0, 0, 0, 0), mysql.TypeDate, 0)
		retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
		if retTimestamp == 0 {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, startTime.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		ret := types.TimeFromDays(retTimestamp + days[i] - 1)
		if ret.IsZero() || ret.Year() > 9999 {
			result.SetNull(i, true)
			continue
		}
		times[i] = ret
	}
	return nil
}

func (b *builtinWeekOfYearSig) vectorized() bool {
	return true
}

func (b *builtinWeekOfYearSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		week := ds[i].Week(3)
		i64s[i] = int64(week)
	}
	return nil
}

func (b *builtinUTCTimestampWithArgSig) vectorized() bool {
	return true
}

// vecEvalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeTime(n, false)
	t64s := result.Times()
	i64s := buf.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		fsp := i64s[i]
		if fsp > int64(math.MaxInt32) || fsp < int64(types.MinFsp) {
			return types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
		}
		if fsp > int64(types.MaxFsp) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "utc_timestamp", types.MaxFsp)
		}
		res, isNull, err := evalUTCTimestampWithFsp(ctx, int(fsp))
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		t64s[i] = res
	}
	return nil
}

func (b *builtinTimeToSecSig) vectorized() bool {
	return true
}

// vecEvalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	fsp := b.args[0].GetType(ctx).GetDecimal()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		var sign int
		duration := buf.GetDuration(i, fsp)
		if duration.Duration >= 0 {
			sign = 1
		} else {
			sign = -1
		}
		i64s[i] = int64(sign * (duration.Hour()*3600 + duration.Minute()*60 + duration.Second()))
	}
	return nil
}

func (b *builtinStrToDateDatetimeSig) vectorized() bool {
	return true
}

func (b *builtinStrToDateDatetimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	dateBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err = b.args[0].VecEvalString(ctx, input, dateBuf); err != nil {
		return err
	}

	formatBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(formatBuf)
	if err = b.args[1].VecEvalString(ctx, input, formatBuf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(dateBuf, formatBuf)
	times := result.Times()
	tc := typeCtx(ctx)
	hasNoZeroDateMode := sqlMode(ctx).HasNoZeroDateMode()
	fsp := b.tp.GetDecimal()

	for i := range n {
		if result.IsNull(i) {
			continue
		}
		var t types.Time
		succ := t.StrToDate(tc, dateBuf.GetString(i), formatBuf.GetString(i))
		if !succ {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if hasNoZeroDateMode && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		t.SetType(mysql.TypeDatetime)
		t.SetFsp(fsp)
		times[i] = t
	}
	return nil
}

func (b *builtinUTCDateSig) vectorized() bool {
	return true
}

func (b *builtinUTCDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	year, month, day := nowTs.UTC().Date()
	utcDate := types.NewTime(types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)), mysql.TypeDate, types.UnspecifiedFsp)

	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range n {
		times[i] = utcDate
	}
	return nil
}

func (b *builtinWeekWithoutModeSig) vectorized() bool {
	return true
}

func (b *builtinWeekWithoutModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()

	mode := 0
	if modeStr := ctx.GetDefaultWeekFormatMode(); modeStr != "" {
		mode, err = strconv.Atoi(modeStr)
		if err != nil {
			return handleInvalidTimeError(ctx, types.ErrInvalidWeekModeFormat.GenWithStackByArgs(modeStr))
		}
	}
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		if date.IsZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		week := date.Week(mode)
		i64s[i] = int64(week)
	}
	return nil
}

func (b *builtinUnixTimestampDecSig) vectorized() bool {
	return true
}

func (b *builtinUnixTimestampDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeDecimal(n, false)
	ts := result.Decimals()
	timeBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(timeBuf)
	if err := b.args[0].VecEvalTime(ctx, input, timeBuf); err != nil {
		for i := range n {
			temp, isNull, err := b.evalDecimal(ctx, input.GetRow(i))
			if err != nil {
				return err
			}
			ts[i] = *temp
			if isNull {
				result.SetNull(i, true)
			}
		}
	} else {
		result.MergeNulls(timeBuf)
		for i := range n {
			if result.IsNull(i) {
				continue
			}
			t, err := timeBuf.GetTime(i).GoTime(getTimeZone(ctx))
			if err != nil {
				ts[i] = *new(types.MyDecimal)
				continue
			}
			tmp, err := goTimeToMysqlUnixTimestamp(t, b.tp.GetDecimal())
			if err != nil {
				return err
			}
			ts[i] = *tmp
		}
	}
	return nil
}

func (b *builtinPeriodAddSig) vectorized() bool {
	return true
}

// vecEvalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	i64s := result.Int64s()
	result.MergeNulls(buf)
	ns := buf.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		// in MySQL, if p is invalid but n is NULL, the result is NULL, so we have to check if n is NULL first.
		if !validPeriod(i64s[i]) {
			return errIncorrectArgs.GenWithStackByArgs("period_add")
		}
		sumMonth := int64(period2Month(uint64(i64s[i]))) + ns[i]
		i64s[i] = int64(month2Period(uint64(sumMonth)))
	}
	return nil
}

func (b *builtinTimestampAddSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalReal(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalTime(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf1.Float64s()
	ds := buf2.Times()
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		unit := buf.GetString(i)
		v := nums[i]
		arg := ds[i]

		tm1, err := arg.GoTime(time.Local)
		if err != nil {
			tc := typeCtx(ctx)
			tc.AppendWarning(err)
			result.AppendNull()
			continue
		}
		tb, overflow, err := addUnitToTime(unit, tm1, v)
		if err != nil {
			return err
		}
		if overflow {
			if err = handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime")); err != nil {
				return err
			}
			result.AppendNull()
			continue
		}
		fsp := types.DefaultFsp
		// use MaxFsp when microsecond is not zero
		if tb.Nanosecond()/1000 != 0 {
			fsp = types.MaxFsp
		}
		r := types.NewTime(types.FromGoTime(tb), b.resolveType(arg.Type(), unit), fsp)
		if err = r.Check(typeCtx(ctx)); err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.AppendNull()
			continue
		}
		result.AppendString(r.String())
	}
	return nil
}

func (b *builtinToDaysSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		arg := ds[i]
		ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
		if ret == 0 {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = ret
	}
	return nil
}

func (b *builtinDateFormatSig) vectorized() bool {
	return true
}

func (b *builtinDateFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	dateBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err := b.args[0].VecEvalTime(ctx, input, dateBuf); err != nil {
		return err
	}
	times := dateBuf.Times()

	formatBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(formatBuf)
	if err := b.args[1].VecEvalString(ctx, input, formatBuf); err != nil {
		return err
	}

	result.ReserveString(n)

	for i := range times {
		t := times[i]
		if dateBuf.IsNull(i) || formatBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		formatMask := formatBuf.GetString(i)
		// MySQL compatibility, #11203
		// If format mask is 0 then return 0 without warnings
		if formatMask == "0" {
			result.AppendString("0")
			continue
		}

		if t.InvalidZero() {
			// MySQL compatibility, #11203
			// 0 | 0.0 should be converted to null without warnings
			n, err := t.ToNumber().ToInt()
			isOriginalIntOrDecimalZero := err == nil && n == 0
			// Args like "0000-00-00", "0000-00-00 00:00:00" set Fsp to 6
			isOriginalStringZero := t.Fsp() > 0

			result.AppendNull()
			if isOriginalIntOrDecimalZero && !isOriginalStringZero {
				continue
			}
			if errHandled := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); errHandled != nil {
				return errHandled
			}
			continue
		}
		res, err := t.DateFormat(formatMask)
		if err != nil {
			return err
		}
		result.AppendString(res)
	}
	return nil
}

func (b *builtinHourSig) vectorized() bool {
	return true
}

func (b *builtinHourSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return vecEvalIntByRows(ctx, b, input, result)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, int(types.UnspecifiedFsp)).Hour())
	}
	return nil
}

func (b *builtinSecToTimeSig) vectorized() bool {
	return true
}

func (b *builtinSecToTimeSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}
	args := buf.Float64s()
	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	durations := result.GoDurations()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		secondsFloat := args[i]
		negative := ""
		if secondsFloat < 0 {
			negative = "-"
			secondsFloat = math.Abs(secondsFloat)
		}
		seconds := uint64(secondsFloat)
		demical := secondsFloat - float64(seconds)
		var minute, second uint64
		hour := seconds / 3600
		if hour > 838 {
			hour = 838
			minute = 59
			second = 59
			demical = 0
			tc := typeCtx(ctx)
			err = tc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("time", strconv.FormatFloat(secondsFloat, 'f', -1, 64)))
			if err != nil {
				return err
			}
		} else {
			minute = seconds % 3600 / 60
			second = seconds % 60
		}
		secondDemical := float64(second) + demical
		duration, _, err := types.ParseDuration(typeCtx(ctx), fmt.Sprintf("%s%02d:%02d:%s", negative, hour, minute, strconv.FormatFloat(secondDemical, 'f', -1, 64)), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		durations[i] = duration.Duration
	}
	return nil
}

func (b *builtinUTCTimeWithoutArgSig) vectorized() bool {
	return true
}

// vecEvalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	res, _, err := types.ParseDuration(typeCtx(ctx), nowTs.UTC().Format(types.TimeFormat), types.DefaultFsp)
	if err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
	for i := range n {
		d64s[i] = res.Duration
	}
	return nil
}

func (b *builtinDateDiffSig) vectorized() bool {
	return true
}

func (b *builtinDateDiffSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalTime(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf0, buf1)
	args0 := buf0.Times()
	args1 := buf1.Times()
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if invalidArg0, invalidArg1 := args0[i].InvalidZero(), args1[i].InvalidZero(); invalidArg0 || invalidArg1 {
			if invalidArg0 {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, args0[i].String()))
			}
			if invalidArg1 {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, args1[i].String()))
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(types.DateDiff(args0[i].CoreTime(), args1[i].CoreTime()))
	}
	return nil
}

func (b *builtinCurrentDateSig) vectorized() bool {
	return true
}

func (b *builtinCurrentDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}

	tz := location(ctx)
	year, month, day := nowTs.In(tz).Date()
	timeValue := types.NewTime(types.FromDate(year, int(month), day, 0, 0, 0, 0), mysql.TypeDate, 0)

	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range n {
		times[i] = timeValue
	}
	return nil
}

func (b *builtinMakeTimeSig) vectorized() bool {
	return true
}

func (b *builtinMakeTimeSig) getVecIntParam(ctx EvalContext, arg Expression, input *chunk.Chunk, col *chunk.Column) (err error) {
	if arg.GetType(ctx).EvalType() == types.ETReal {
		err = arg.VecEvalReal(ctx, input, col)
		if err != nil {
			return err
		}
		f64s := col.Float64s()
		i64s := col.Int64s()
		n := input.NumRows()
		for i := range n {
			i64s[i] = int64(f64s[i])
		}
		return nil
	}
	err = arg.VecEvalInt(ctx, input, col)
	return err
}

func (b *builtinMakeTimeSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	hoursBuf := result
	var err error
	if err = b.getVecIntParam(ctx, b.args[0], input, hoursBuf); err != nil {
		return err
	}
	minutesBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(minutesBuf)
	if err = b.getVecIntParam(ctx, b.args[1], input, minutesBuf); err != nil {
		return err
	}
	secondsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(secondsBuf)
	if err = b.args[2].VecEvalReal(ctx, input, secondsBuf); err != nil {
		return err
	}
	hours := hoursBuf.Int64s()
	minutes := minutesBuf.Int64s()
	seconds := secondsBuf.Float64s()
	durs := result.GoDurations()
	result.MergeNulls(minutesBuf, secondsBuf)
	hourUnsignedFlag := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if minutes[i] < 0 || minutes[i] >= 60 || seconds[i] < 0 || seconds[i] >= 60 {
			result.SetNull(i, true)
			continue
		}
		dur, err := b.makeTime(typeCtx(ctx), hours[i], minutes[i], seconds[i], hourUnsignedFlag)
		if err != nil {
			return err
		}
		durs[i] = dur.Duration
	}
	return nil
}

func (b *builtinDayOfYearSig) vectorized() bool {
	return true
}

func (b *builtinDayOfYearSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(ds[i].YearDay())
	}
	return nil
}

func (b *builtinFromUnixTime1ArgSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime1ArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	ts := result.Times()
	ds := buf.Decimals()
	fsp := b.tp.GetDecimal()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		t, isNull, err := evalFromUnixTime(ctx, fsp, &ds[i])
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		ts[i] = t
	}
	return nil
}

func (b *builtinYearWeekWithModeSig) vectorized() bool {
	return true
}

// vecEvalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf1)
	i64s := result.Int64s()
	ds := buf1.Times()
	ms := buf2.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		if date.IsZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		mode := int(ms[i])
		if buf2.IsNull(i) {
			mode = 0
		}
		year, week := date.YearWeek(mode)
		i64s[i] = int64(week + year*100)
		if i64s[i] < 0 {
			i64s[i] = int64(math.MaxUint32)
		}
	}
	return nil
}

func (b *builtinTimestampDiffSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unitBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(unitBuf)
	if err := b.args[0].VecEvalString(ctx, input, unitBuf); err != nil {
		return err
	}
	lhsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lhsBuf)
	if err := b.args[1].VecEvalTime(ctx, input, lhsBuf); err != nil {
		return err
	}
	rhsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rhsBuf)
	if err := b.args[2].VecEvalTime(ctx, input, rhsBuf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(unitBuf, lhsBuf, rhsBuf)
	i64s := result.Int64s()
	lhs := lhsBuf.Times()
	rhs := rhsBuf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if invalidLHS, invalidRHS := lhs[i].InvalidZero(), rhs[i].InvalidZero(); invalidLHS || invalidRHS {
			if invalidLHS {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, lhs[i].String()))
			}
			if invalidRHS {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rhs[i].String()))
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = types.TimestampDiff(unitBuf.GetString(i), lhs[i], rhs[i])
	}
	return nil
}

func (b *builtinUnixTimestampIntSig) vectorized() bool {
	return true
}

// vecEvalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		var isNull bool
		for i := range n {
			i64s[i], isNull, err = b.evalInt(ctx, input.GetRow(i))
			if err != nil {
				return err
			}
			if isNull {
				result.SetNull(i, true)
			}
		}
	} else {
		result.MergeNulls(buf)
		for i := range n {
			if result.IsNull(i) {
				continue
			}

			t, err := buf.GetTime(i).AdjustedGoTime(getTimeZone(ctx))
			if err != nil {
				i64s[i] = 0
				continue
			}
			dec, err := goTimeToMysqlUnixTimestamp(t, 1)
			if err != nil {
				return err
			}
			intVal, err := dec.ToInt()
			if !terror.ErrorEqual(err, types.ErrTruncated) {
				terror.Log(err)
			}
			i64s[i] = intVal
		}
	}

	return nil
}

func (b *builtinCurrentTime0ArgSig) vectorized() bool {
	return true
}

func (b *builtinCurrentTime0ArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	tz := location(ctx)
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, _, err := types.ParseDuration(typeCtx(ctx), dur, types.MinFsp)
	if err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	durations := result.GoDurations()
	for i := range n {
		durations[i] = res.Duration
	}
	return nil
}

func (b *builtinTimeSig) vectorized() bool {
	return true
}

func (b *builtinTimeSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	ds := result.GoDurations()
	tc := typeCtx(ctx)
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		fsp := 0
		expr := buf.GetString(i)
		if idx := strings.Index(expr, "."); idx != -1 {
			fsp = len(expr) - idx - 1
		}

		var tmpFsp int
		if tmpFsp, err = types.CheckFsp(fsp); err != nil {
			return err
		}
		fsp = tmpFsp

		res, _, err := types.ParseDuration(tc, expr, fsp)
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = tc.HandleTruncate(err)
		}
		if err != nil {
			return err
		}
		ds[i] = res.Duration
	}
	return nil
}

func (b *builtinDateLiteralSig) vectorized() bool {
	return true
}

func (b *builtinDateLiteralSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	mode := sqlMode(ctx)
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		return types.ErrWrongValue.GenWithStackByArgs(types.DateStr, b.literal.String())
	}
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		return types.ErrWrongValue.GenWithStackByArgs(types.DateStr, b.literal.String())
	}

	result.ResizeTime(n, false)
	times := result.Times()
	for i := range times {
		times[i] = b.literal
	}
	return nil
}

func (b *builtinTimeLiteralSig) vectorized() bool {
	return true
}

func (b *builtinTimeLiteralSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
	for i := range n {
		d64s[i] = b.duration.Duration
	}
	return nil
}

func (b *builtinMonthNameSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	ds := buf.Times()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		mon := ds[i].Month()
		if (ds[i].IsZero() && sqlMode(ctx).HasNoZeroDateMode()) || mon < 0 || mon > len(types.MonthNames) {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.AppendNull()
			continue
		} else if mon == 0 || ds[i].IsZero() {
			result.AppendNull()
			continue
		}
		result.AppendString(types.MonthNames[mon-1])
	}
	return nil
}

func (b *builtinMonthNameSig) vectorized() bool {
	return true
}

func (b *builtinDayOfWeekSig) vectorized() bool {
	return true
}

func (b *builtinDayOfWeekSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(ds[i].Weekday() + 1)
	}
	return nil
}

func (b *builtinCurrentTime1ArgSig) vectorized() bool {
	return true
}

func (b *builtinCurrentTime1ArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	tz := location(ctx)
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	tc := typeCtx(ctx)
	i64s := buf.Int64s()
	result.ResizeGoDuration(n, false)
	durations := result.GoDurations()
	for i := range n {
		res, _, err := types.ParseDuration(tc, dur, int(i64s[i]))
		if err != nil {
			return err
		}
		durations[i] = res.Duration
	}
	return nil
}

func (b *builtinUTCTimestampWithoutArgSig) vectorized() bool {
	return true
}

// vecEvalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	res, isNull, err := evalUTCTimestampWithFsp(ctx, types.DefaultFsp)
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}
	result.ResizeTime(n, false)
	t64s := result.Times()
	for i := range n {
		t64s[i] = res
	}
	return nil
}

func (b *builtinConvertTzSig) vectorized() bool {
	return true
}

func (b *builtinConvertTzSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}

	fromTzBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(fromTzBuf)
	if err := b.args[1].VecEvalString(ctx, input, fromTzBuf); err != nil {
		return err
	}

	toTzBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(toTzBuf)
	if err := b.args[2].VecEvalString(ctx, input, toTzBuf); err != nil {
		return err
	}

	result.MergeNulls(fromTzBuf, toTzBuf)
	ts := result.Times()
	var isNull bool
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		ts[i], isNull, err = b.convertTz(ts[i], fromTzBuf.GetString(i), toTzBuf.GetString(i))
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		}
	}
	return nil
}

func (b *builtinTimestamp1ArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	tc := typeCtx(ctx)
	var tm types.Time
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		s := buf.GetString(i)

		if b.isFloat {
			tm, err = types.ParseTimeFromFloatString(tc, s, mysql.TypeDatetime, types.GetFsp(s))
		} else {
			tm, err = types.ParseTime(tc, s, mysql.TypeDatetime, types.GetFsp(s))
		}
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
	}
	return nil
}

func (b *builtinTimestamp1ArgSig) vectorized() bool {
	return true
}

func (b *builtinTimestamp2ArgsSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf0, buf1)
	times := result.Times()
	tc := typeCtx(ctx)
	var tm types.Time
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		arg0 := buf0.GetString(i)
		arg1 := buf1.GetString(i)

		if b.isFloat {
			tm, err = types.ParseTimeFromFloatString(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
		} else {
			tm, err = types.ParseTime(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
		}
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if tm.Year() == 0 {
			// MySQL won't evaluate add for date with zero year.
			// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc.cc#L2805
			result.SetNull(i, true)
			continue
		}

		if !isDuration(arg1) {
			result.SetNull(i, true)
			continue
		}

		duration, _, err := types.ParseDuration(tc, arg1, types.GetFsp(arg1))
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		tmp, err := tm.Add(tc, duration)
		if err != nil {
			return err
		}
		times[i] = tmp
	}
	return nil
}

func (b *builtinTimestamp2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinDayOfMonthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(ds[i].Day())
	}
	return nil
}

func (b *builtinDayOfMonthSig) vectorized() bool {
	return true
}

func (b *builtinAddSubDateAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ReserveString(n)
		result.SetNulls(0, n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	dateBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err := b.vecGetDate(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, dateBuf); err != nil {
		return err
	}

	result.ReserveString(n)

	dateBuf.MergeNulls(intervalBuf)
	for i := range n {
		if dateBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		resDate, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, dateBuf.Times()[i], intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
		} else {
			if resDate.Microsecond() == 0 {
				resDate.SetFsp(types.MinFsp)
			} else {
				resDate.SetFsp(types.MaxFsp)
			}
			result.AppendString(resDate.String())
		}
	}
	return nil
}

func (b *builtinAddSubDateAsStringSig) vectorized() bool {
	return true
}

func (b *builtinAddSubDateDatetimeAnySig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	if err := b.vecGetDateFromDatetime(&b.baseBuiltinFunc, ctx, input, unit, result); err != nil {
		return err
	}

	result.MergeNulls(intervalBuf)
	resDates := result.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		resDate, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, resDates[i], intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		} else {
			resDates[i] = resDate
		}
	}
	return nil
}

func (b *builtinAddSubDateDatetimeAnySig) vectorized() bool {
	return true
}

func (b *builtinAddSubDateDurationAnySig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	durBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(durBuf)
	if err := b.args[0].VecEvalDuration(ctx, input, durBuf); err != nil {
		return err
	}

	goDurations := durBuf.GoDurations()
	result.ResizeTime(n, false)
	result.MergeNulls(durBuf, intervalBuf)
	resDates := result.Times()
	iterDuration := types.Duration{Fsp: types.MaxFsp}
	tc := typeCtx(ctx)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		iterDuration.Duration = goDurations[i]
		t, err := iterDuration.ConvertToTime(tc, mysql.TypeDatetime)
		if err != nil {
			result.SetNull(i, true)
		}
		resDate, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, t, intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		} else {
			resDates[i] = resDate
		}
	}
	return nil
}

func (b *builtinAddSubDateDurationAnySig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeGoDuration(n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	if err := b.args[0].VecEvalDuration(ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(intervalBuf)
	resDurations := result.GoDurations()
	iterDuration := types.Duration{Fsp: types.MaxFsp}
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		iterDuration.Duration = resDurations[i]
		resDuration, isNull, err := b.durationOp(&b.baseDateArithmetical, ctx, iterDuration, intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		} else {
			resDurations[i] = resDuration.Duration
		}
	}
	return nil
}

func (b *builtinAddSubDateDurationAnySig) vectorized() bool {
	return true
}
