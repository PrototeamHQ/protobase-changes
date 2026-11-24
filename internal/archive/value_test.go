package archive

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

func micros(t time.Time) arrow.Timestamp { return arrow.Timestamp(t.UnixMicro()) }

func TestParseValue(t *testing.T) {
	p := iceberg.PrimitiveTypes
	cases := []struct {
		text string
		typ  iceberg.Type
		want any
	}{
		{"hi", p.String, "hi"},
		{"t", p.Bool, true},
		{"f", p.Bool, false},
		{"-32768", p.Int32, int32(-32768)},
		{"9007199254740993", p.Int64, int64(9007199254740993)},
		{"1.5", p.Float32, float32(1.5)},
		{"-Infinity", p.Float64, math.Inf(-1)},
		{"2026-11-24", p.Date, arrow.Date32(20781)},
		{"1969-12-31", p.Date, arrow.Date32(-1)},
		{"infinity", p.Date, dateInfinity},
		{"300000-01-01", p.Date, arrow.Date32(time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{"5874897-12-31", p.Date, arrow.Date32(time.Date(5874897, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{"4714-11-24 BC", p.Date, arrow.Date32(time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{"0001-01-01 BC", p.Date, arrow.Date32(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{"2026-11-24 05:36:38.466", p.Timestamp, micros(time.Date(2026, 11, 24, 5, 36, 38, 466e6, time.UTC))},
		{"2026-11-24 05:36:38.466123+00", p.TimestampTz, micros(time.Date(2026, 11, 24, 5, 36, 38, 466123e3, time.UTC))},
		{"2026-11-24 11:06:38+05:30", p.TimestampTz, micros(time.Date(2026, 11, 24, 5, 36, 38, 0, time.UTC))},
		{"1960-01-01 00:00:00.25+00", p.TimestampTz, micros(time.Date(1960, 1, 1, 0, 0, 0, 25e7, time.UTC))},
		{"10000-01-01 00:00:00", p.Timestamp, micros(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{"294246-12-31 23:59:59.999999", p.Timestamp, arrow.Timestamp(time.Date(294246, 12, 31, 23, 59, 59, 0, time.UTC).Unix()*1e6 + 999999)},
		{"4714-11-24 00:00:00+00 BC", p.TimestampTz, arrow.Timestamp(time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC).Unix() * 1e6)},
		{"-infinity", p.TimestampTz, tsMinusInfinity},
	}
	for _, c := range cases {
		got, err := parseValue(c.text, c.typ)
		if err != nil {
			t.Errorf("%q as %s: %v", c.text, c.typ, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q as %s: got %v (%T), want %v (%T)", c.text, c.typ, got, got, c.want, c.want)
		}
	}
}

func TestParseBytea(t *testing.T) {
	got, err := parseValue(`\x00ff`, iceberg.PrimitiveTypes.Binary)
	if err != nil || !bytes.Equal(got.([]byte), []byte{0, 255}) {
		t.Fatalf("got %v, %v", got, err)
	}
}

// Postgres allows 24:00:00, which no Iceberg time holds, so a time stays text.
func TestTimeIsKeptAsText(t *testing.T) {
	typ := columnType(1083)
	for _, text := range []string{"13:45:06.5", "24:00:00"} {
		if got, err := parseValue(text, typ); err != nil || got != text {
			t.Errorf("%q: got %v, %v", text, got, err)
		}
	}
}

func TestParseValueRefuses(t *testing.T) {
	p := iceberg.PrimitiveTypes
	cases := []struct {
		text string
		typ  iceberg.Type
	}{
		{"yes", p.Bool},
		{"2147483648", p.Int32},
		{"1.5", p.Int64},
		{"0102", p.Binary},
		{"24.11.2026", p.Date},
		{"2026-11-24T05:36:38Z", p.TimestampTz},
		{"294276-01-01 00:00:00", p.Timestamp},
		{"294247-01-10 04:00:54.775807", p.Timestamp},
		{"294247-01-10 06:00:54+02", p.TimestampTz},
	}
	for _, c := range cases {
		if _, err := parseValue(c.text, c.typ); err == nil {
			t.Errorf("%q as %s parsed", c.text, c.typ)
		}
	}
}

func TestPromotes(t *testing.T) {
	p := iceberg.PrimitiveTypes
	if !promotes(p.Int32, p.Int64) || !promotes(p.Float32, p.Float64) {
		t.Fatal("int and float widenings are promotions")
	}
	if promotes(p.Int64, p.Int32) || promotes(p.Int32, p.String) || promotes(p.Timestamp, p.TimestampTz) {
		t.Fatal("a narrowing or a change of kind is no promotion")
	}
	if !holds(p.Int64, p.Int32) || holds(p.Int32, p.Int64) {
		t.Fatal("a long field holds ints, an int field no longs")
	}
}
