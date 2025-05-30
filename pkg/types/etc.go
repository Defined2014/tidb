// Copyright 2015 PingCAP, Inc.
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

// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package types

import (
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	ast "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// IsTypeBlob returns a boolean indicating whether the tp is a blob type.
var IsTypeBlob = ast.IsTypeBlob

// IsTypeChar returns a boolean indicating
// whether the tp is the char type like a string type or a varchar type.
var IsTypeChar = ast.IsTypeChar

// IsTypeVector returns whether tp is a vector type.
var IsTypeVector = ast.IsTypeVector

// IsTypeVarchar returns a boolean indicating
// whether the tp is the varchar type like a varstring type or a varchar type.
func IsTypeVarchar(tp byte) bool {
	return tp == mysql.TypeVarString || tp == mysql.TypeVarchar
}

// IsTypeUnspecified returns a boolean indicating whether the tp is the Unspecified type.
func IsTypeUnspecified(tp byte) bool {
	return tp == mysql.TypeUnspecified
}

// IsTypePrefixable returns a boolean indicating
// whether an index on a column with the tp can be defined with a prefix.
func IsTypePrefixable(tp byte) bool {
	return IsTypeBlob(tp) || IsTypeChar(tp)
}

// IsTypeFractionable returns a boolean indicating
// whether the tp can has time fraction.
func IsTypeFractionable(tp byte) bool {
	return tp == mysql.TypeDatetime || tp == mysql.TypeDuration || tp == mysql.TypeTimestamp
}

// IsTypeTime returns a boolean indicating
// whether the tp is time type like datetime, date or timestamp.
func IsTypeTime(tp byte) bool {
	return tp == mysql.TypeDatetime || tp == mysql.TypeDate || tp == mysql.TypeTimestamp
}

// IsTypeFloat indicates whether the type is TypeFloat
func IsTypeFloat(tp byte) bool {
	return tp == mysql.TypeFloat
}

// IsTypeInteger returns a boolean indicating whether the tp is integer type.
func IsTypeInteger(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		return true
	}
	return false
}

// IsTypeStoredAsInteger returns a boolean indicating whether the tp is stored as integer type.
func IsTypeStoredAsInteger(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return true
	case mysql.TypeYear:
		return true
	// Enum and Set are stored as integer type but they can not be pushed down to TiFlash
	// case mysql.TypeEnum, mysql.TypeSet:
	// 	return true
	case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeDuration:
		return true
	}
	return false
}

// IsTypeNumeric returns a boolean indicating whether the tp is numeric type.
func IsTypeNumeric(tp byte) bool {
	switch tp {
	case mysql.TypeBit, mysql.TypeTiny, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeNewDecimal,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeShort:
		return true
	}
	return false
}

// IsTypeBit returns a boolean indicating whether the tp is bit type.
func IsTypeBit(ft *FieldType) bool {
	return ft.GetType() == mysql.TypeBit
}

// IsTemporalWithDate returns a boolean indicating
// whether the tp is time type with date.
func IsTemporalWithDate(tp byte) bool {
	return IsTypeTime(tp)
}

// IsBinaryStr returns a boolean indicating
// whether the field type is a binary string type.
func IsBinaryStr(ft *FieldType) bool {
	return ft.GetCollate() == charset.CollationBin && IsString(ft.GetType())
}

// IsNonBinaryStr returns a boolean indicating
// whether the field type is a non-binary string type.
func IsNonBinaryStr(ft *FieldType) bool {
	if ft.GetCollate() != charset.CollationBin && IsString(ft.GetType()) {
		return true
	}
	return false
}

// NeedRestoredData returns if a type needs restored data.
// If the type is char and the collation is _bin, NeedRestoredData() returns false.
func NeedRestoredData(ft *FieldType) bool {
	if collate.NewCollationEnabled() &&
		IsNonBinaryStr(ft) &&
		(!collate.IsBinCollation(ft.GetCollate()) || IsTypeVarchar(ft.GetType())) &&
		ft.GetCollate() != "utf8mb4_0900_bin" {
		return true
	}
	return false
}

// IsString returns a boolean indicating
// whether the field type is a string type.
func IsString(tp byte) bool {
	return IsTypeChar(tp) || IsTypeBlob(tp) || IsTypeVarchar(tp) || IsTypeUnspecified(tp)
}

// IsStringKind returns a boolean indicating whether the tp is a string type.
func IsStringKind(kind byte) bool {
	return kind == KindString || kind == KindBytes
}

var kind2Str = map[byte]string{
	KindNull:          "null",
	KindInt64:         "bigint",
	KindUint64:        "unsigned bigint",
	KindFloat32:       "float",
	KindFloat64:       "double",
	KindString:        "char",
	KindBytes:         "bytes",
	KindBinaryLiteral: "bit/hex literal",
	KindMysqlDecimal:  "decimal",
	KindMysqlDuration: "time",
	KindMysqlEnum:     "enum",
	KindMysqlBit:      "bit",
	KindMysqlSet:      "set",
	KindMysqlTime:     "datetime",
	KindInterface:     "interface",
	KindMinNotNull:    "min_not_null",
	KindMaxValue:      "max_value",
	KindRaw:           "raw",
	KindMysqlJSON:     "json",
	KindVectorFloat32: "vector",
}

// TypeStr converts tp to a string.
var TypeStr = ast.TypeStr

// KindStr converts kind to a string.
func KindStr(kind byte) (r string) {
	return kind2Str[kind]
}

// TypeToStr converts a field to a string.
// It is used for converting Text to Blob,
// or converting Char to Binary.
// Args:
//
//	tp: type enum
//	cs: charset
var TypeToStr = ast.TypeToStr

// EOFAsNil filtrates errors,
// If err is equal to io.EOF returns nil.
func EOFAsNil(err error) error {
	if terror.ErrorEqual(err, io.EOF) {
		return nil
	}
	return errors.Trace(err)
}

// InvOp2 returns an invalid operation error.
func InvOp2(x, y any, o opcode.Op) (any, error) {
	return nil, errors.Errorf("Invalid operation: %v %v %v (mismatched types %T and %T)", x, o, y, x, y)
}

// overflow returns an overflowed error.
func overflow(v any, tp byte) error {
	return ErrOverflow.GenWithStack("constant %v overflows %s", v, TypeStr(tp))
}

// IsTypeTemporal checks if a type is a temporal type.
func IsTypeTemporal(tp byte) bool {
	switch tp {
	case mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeTimestamp,
		mysql.TypeDate, mysql.TypeNewDate:
		return true
	}
	return false
}
