package archive

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// structValues are a row struct's values by field; nil is a null struct,
// such as old when the WAL sent no old row.
type structValues map[*field]change.Value

// archivedRow is one change as the archive stores it.
type archivedRow struct {
	ev        event
	unchanged []string
	// structs are key, old and new, in the order of rowStructs.
	structs [3]structValues
}

// apply evolves the shape for the events and returns the changes as rows.
// Renames apply in their place among the changes.
func (s *shape) apply(events []event) []archivedRow {
	var rows []archivedRow
	for _, ev := range events {
		if ev.rename != nil {
			s.rename(ev.rename.From, ev.rename.To)
			continue
		}
		c := ev.change
		r := archivedRow{ev: ev}
		r.structs[0] = s.values(structKey, c.Columns, c.Key)
		if c.OldRowKind != change.NoOldRow {
			r.structs[1] = s.values(structOld, c.Columns, c.OldRow)
		}
		r.structs[2] = s.values(structNew, c.Columns, c.NewRow)
		for _, col := range c.Columns {
			if v, ok := c.NewRow[col.Name]; ok && v.Kind == change.Unchanged {
				r.unchanged = append(r.unchanged, col.Name)
			}
		}
		rows = append(rows, r)
	}
	return rows
}

// values maps a WAL row to the fields of struct name. Columns the row does
// not state, such as the non-key columns of a key-only old row, stay null.
func (s *shape) values(name string, cols []change.Column, row change.Row) structValues {
	if row == nil {
		return nil
	}
	fields := s.columns(name, cols)
	if fields == nil {
		return nil
	}
	vals := structValues{}
	for col, f := range fields {
		if v, ok := row[col]; ok {
			vals[f] = v
		}
	}
	return vals
}

// record builds the rows as one Arrow record of the table's schema.
func record(schema *iceberg.Schema, rows []archivedRow) (arrow.RecordBatch, error) {
	as, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, err
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	at := func(name string) array.Builder { return b.Field(as.FieldIndices(name)[0]) }

	for _, r := range rows {
		tx := r.ev.tx
		at(fieldCommitLSN).(*array.Int64Builder).Append(int64(tx.CommitLSN))
		at(fieldXid).(*array.Int64Builder).Append(int64(tx.Xid))
		at(fieldCommitTime).(*array.TimestampBuilder).Append(arrow.Timestamp(tx.CommitTime.UnixMicro()))
		at(fieldSeq).(*array.Int32Builder).Append(int32(r.ev.seq))
		at(fieldOperation).(*array.StringBuilder).Append(string(r.ev.change.Operation))
		actor := at(fieldActor).(*array.StringBuilder)
		if r.ev.actor == nil {
			actor.AppendNull()
		} else {
			actor.Append(*r.ev.actor)
		}
		list := at(fieldUnchanged).(*array.ListBuilder)
		if len(r.unchanged) == 0 {
			list.AppendNull()
		} else {
			list.Append(true)
			list.ValueBuilder().(*array.StringBuilder).AppendValues(r.unchanged, nil)
		}
		for i, name := range rowStructs {
			f, ok := schema.FindFieldByName(name)
			if !ok {
				continue
			}
			err := appendStruct(at(name).(*array.StructBuilder), f.Type.(*iceberg.StructType), r.structs[i])
			if err != nil {
				return nil, fmt.Errorf("%s of change %d of transaction %d: %w", name, r.ev.seq, tx.Xid, err)
			}
		}
	}
	return b.NewRecordBatch(), nil
}

func appendStruct(b *array.StructBuilder, st *iceberg.StructType, vals structValues) error {
	if vals == nil {
		b.AppendNull()
		return nil
	}
	b.Append(true)
	byName := make(map[string]change.Value, len(vals))
	for f, v := range vals {
		byName[f.name] = v
	}
	for i, f := range st.FieldList {
		v, ok := byName[f.Name]
		if !ok || v.Kind != change.Text {
			b.FieldBuilder(i).AppendNull()
			continue
		}
		if err := appendValue(b.FieldBuilder(i), f.Type, v.Text); err != nil {
			return fmt.Errorf("column %s: %w", f.Name, err)
		}
	}
	return nil
}

func appendValue(b array.Builder, t iceberg.Type, text string) error {
	v, err := parseValue(text, t)
	if err != nil {
		return err
	}
	switch b := b.(type) {
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	case *array.Int32Builder:
		b.Append(v.(int32))
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.Float32Builder:
		b.Append(v.(float32))
	case *array.Float64Builder:
		b.Append(v.(float64))
	case *array.BinaryBuilder:
		b.Append(v.([]byte))
	case *array.Date32Builder:
		b.Append(v.(arrow.Date32))
	case *array.Time64Builder:
		b.Append(v.(arrow.Time64))
	case *array.TimestampBuilder:
		b.Append(v.(arrow.Timestamp))
	default:
		return fmt.Errorf("no Arrow builder for type %s", t)
	}
	return nil
}
