// Package archive stores every committed change of an app in one Iceberg
// table per Postgres table, committed through an Iceberg catalog.
package archive

import "github.com/apache/iceberg-go"

// Postgres type OIDs with an Iceberg type of their own. Every other type,
// such as numeric, uuid, json or an array, is archived in its text format.
const (
	oidBool        = 16
	oidBytea       = 17
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidOID         = 26
	oidFloat4      = 700
	oidFloat8      = 701
	oidDate        = 1082
	oidTime        = 1083
	oidTimestamp   = 1114
	oidTimestampTz = 1184
)

// columnType is the Iceberg type a Postgres column's values are archived as.
func columnType(oid uint32) iceberg.Type {
	switch oid {
	case oidBool:
		return iceberg.PrimitiveTypes.Bool
	case oidBytea:
		return iceberg.PrimitiveTypes.Binary
	case oidInt2, oidInt4:
		return iceberg.PrimitiveTypes.Int32
	case oidInt8, oidOID:
		return iceberg.PrimitiveTypes.Int64
	case oidFloat4:
		return iceberg.PrimitiveTypes.Float32
	case oidFloat8:
		return iceberg.PrimitiveTypes.Float64
	case oidDate:
		return iceberg.PrimitiveTypes.Date
	case oidTime:
		return iceberg.PrimitiveTypes.Time
	case oidTimestamp:
		return iceberg.PrimitiveTypes.Timestamp
	case oidTimestampTz:
		return iceberg.PrimitiveTypes.TimestampTz
	}
	return iceberg.PrimitiveTypes.String
}

// promotes reports whether Iceberg schema evolution may widen a field of
// type from to type to: int to long, and float to double.
func promotes(from, to iceberg.Type) bool {
	switch from.(type) {
	case iceberg.Int32Type:
		_, ok := to.(iceberg.Int64Type)
		return ok
	case iceberg.Float32Type:
		_, ok := to.(iceberg.Float64Type)
		return ok
	}
	return false
}

// holds reports whether a field of type field stores values of type value
// without change: the same type, or one the field was promoted from.
func holds(field, value iceberg.Type) bool {
	return field.Equals(value) || promotes(value, field)
}
