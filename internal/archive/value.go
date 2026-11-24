package archive

import (
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// Postgres stores infinite dates and timestamps as the extremes of their
// integer types; the archive keeps the same values.
const (
	dateInfinity      = arrow.Date32(math.MaxInt32)
	dateMinusInfinity = arrow.Date32(math.MinInt32)
	tsInfinity        = arrow.Timestamp(math.MaxInt64)
	tsMinusInfinity   = arrow.Timestamp(math.MinInt64)
)

// timestampText matches a Postgres date or timestamp in ISO format, with an
// optional UTC offset and era.
var timestampText = regexp.MustCompile(`^(\d{4,})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?)?(?:([+-])(\d{2})(?::(\d{2}))?(?::(\d{2}))?)?( BC)?$`)

// parseValue reads a value in Postgres' text format, as the reader's session
// settings shape it, as a value of type t:
// bool, int32, int64, float32, float64, string, []byte, arrow.Date32 or
// arrow.Timestamp in microseconds.
func parseValue(text string, t iceberg.Type) (any, error) {
	switch t.(type) {
	case iceberg.StringType:
		return text, nil
	case iceberg.BooleanType:
		switch text {
		case "t":
			return true, nil
		case "f":
			return false, nil
		}
		return nil, fmt.Errorf("boolean %q", text)
	case iceberg.Int32Type:
		n, err := strconv.ParseInt(text, 10, 32)
		return int32(n), err
	case iceberg.Int64Type:
		return strconv.ParseInt(text, 10, 64)
	case iceberg.Float32Type:
		f, err := strconv.ParseFloat(text, 32)
		return float32(f), err
	case iceberg.Float64Type:
		return strconv.ParseFloat(text, 64)
	case iceberg.BinaryType:
		hexText, ok := strings.CutPrefix(text, `\x`)
		if !ok {
			return nil, fmt.Errorf("bytea %q is not in hex format", text)
		}
		return hex.DecodeString(hexText)
	case iceberg.DateType:
		return parseDate(text)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return parseTimestamp(text)
	}
	return nil, fmt.Errorf("no parser for type %s", t)
}

func parseDate(text string) (arrow.Date32, error) {
	switch text {
	case "infinity":
		return dateInfinity, nil
	case "-infinity":
		return dateMinusInfinity, nil
	}
	secs, _, err := parseInstant(text)
	if err != nil {
		return 0, err
	}
	// Unix seconds hold every Postgres date, unlike microseconds, and a date
	// is midnight UTC, so the division is exact.
	days := secs / 86400
	if days <= int64(dateMinusInfinity) || days >= int64(dateInfinity) {
		return 0, fmt.Errorf("date %q is out of range", text)
	}
	return arrow.Date32(days), nil
}

// Unix seconds of the finite timestamps the archive holds: whole seconds
// whose microseconds fit an int64 without reaching the infinities.
const (
	minTimestampSeconds = math.MinInt64/1_000_000 + 1
	maxTimestampSeconds = math.MaxInt64/1_000_000 - 1
)

func parseTimestamp(text string) (arrow.Timestamp, error) {
	switch text {
	case "infinity":
		return tsInfinity, nil
	case "-infinity":
		return tsMinusInfinity, nil
	}
	secs, frac, err := parseInstant(text)
	if err != nil {
		return 0, err
	}
	// Postgres counts microseconds from 2000, so its last 30 years of
	// timestamps, after 294246, fit no microseconds from 1970: such a value
	// stops the archive rather than wrapping around.
	if secs < minTimestampSeconds || secs > maxTimestampSeconds {
		return 0, fmt.Errorf("timestamp %q is out of range", text)
	}
	return arrow.Timestamp(secs*1e6 + frac), nil
}

// parseInstant reads a date or timestamp as Unix seconds in UTC and the
// microseconds after them.
func parseInstant(text string) (secs, frac int64, err error) {
	m := timestampText.FindStringSubmatch(text)
	if m == nil {
		return 0, 0, fmt.Errorf("timestamp %q", text)
	}
	year := atoi(m[1])
	if m[12] != "" {
		// 1 BC is year 0, 2 BC year -1.
		year = 1 - year
	}
	offset := int64(atoi(m[9])*3600 + atoi(m[10])*60 + atoi(m[11]))
	if m[8] == "-" {
		offset = -offset
	}
	t := time.Date(year, time.Month(atoi(m[2])), atoi(m[3]), atoi(m[4]), atoi(m[5]), atoi(m[6]), 0, time.UTC)
	return t.Unix() - offset, fraction(m[7]), nil
}

// atoi reads digits the patterns above already matched; empty reads as 0.
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// fraction reads up to six digits after the decimal point as microseconds.
func fraction(digits string) int64 {
	if digits == "" {
		return 0
	}
	n, _ := strconv.ParseInt(digits+strings.Repeat("0", 6-len(digits)), 10, 64)
	return n
}
