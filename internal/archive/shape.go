package archive

import (
	"fmt"

	"github.com/apache/iceberg-go"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// field is one column's field in a row struct. A rename changes its name in
// place, so the values a segment holds follow the field to its final name.
type field struct {
	name string
	typ  iceberg.Type
}

// rowStruct is one row struct of a table, such as new.
type rowStruct struct {
	name   string
	exists bool
	fields []*field
}

func (rs *rowStruct) find(name string) *field {
	for _, f := range rs.fields {
		if f.name == name {
			return f
		}
	}
	return nil
}

type opKind uint8

const (
	opAddStruct opKind = iota
	opAddField
	opRename
	opPromote
)

// schemaOp is one schema change, naming fields as they are at that point.
type schemaOp struct {
	kind opKind
	// path is the struct, then for a rename or promotion the field.
	path []string
	// name is the added field, or the new name of a renamed one.
	name string
	// typ is the added field's or struct's type, or the promoted type.
	typ iceberg.Type
}

// shape is a table's schema while a segment's changes are applied to it in
// order: each change first evolves it, recording schema operations whose
// field ids the catalog then keeps, and then maps its columns to fields.
type shape struct {
	structs map[string]*rowStruct
	ops     []schemaOp
}

// shapeOf reads the row structs of a table's current schema.
func shapeOf(schema *iceberg.Schema) *shape {
	s := &shape{structs: map[string]*rowStruct{}}
	for _, name := range rowStructs {
		rs := &rowStruct{name: name}
		if f, ok := schema.FindFieldByName(name); ok {
			rs.exists = true
			for _, sub := range f.Type.(*iceberg.StructType).FieldList {
				rs.fields = append(rs.fields, &field{name: sub.Name, typ: sub.Type})
			}
		}
		s.structs[name] = rs
	}
	return s
}

// columns evolves struct name to hold the columns cols that it stores and
// returns the field of each by column name, or nil when it stores none.
// A new column adds a field, a widening promotes one, and an incompatible
// type retires the old field under another name and adds a new one.
func (s *shape) columns(name string, cols []change.Column) map[string]*field {
	cols = structColumns(name, cols)
	if len(cols) == 0 {
		return nil
	}
	rs := s.structs[name]
	if !rs.exists {
		return s.addStruct(rs, cols)
	}
	byColumn := make(map[string]*field, len(cols))
	for _, c := range cols {
		t := columnType(c.Type)
		f := rs.find(c.Name)
		switch {
		case f == nil:
			f = s.addField(rs, c.Name, t)
		case holds(f.typ, t):
		case promotes(f.typ, t):
			s.ops = append(s.ops, schemaOp{kind: opPromote, path: []string{rs.name, f.name}, typ: t})
			f.typ = t
		default:
			s.retire(rs, f)
			f = s.addField(rs, c.Name, t)
		}
		byColumn[c.Name] = f
	}
	return byColumn
}

func (s *shape) addStruct(rs *rowStruct, cols []change.Column) map[string]*field {
	st := &iceberg.StructType{}
	byColumn := make(map[string]*field, len(cols))
	for _, c := range cols {
		f := &field{name: c.Name, typ: columnType(c.Type)}
		rs.fields = append(rs.fields, f)
		st.FieldList = append(st.FieldList, iceberg.NestedField{Name: f.name, Type: f.typ})
		byColumn[c.Name] = f
	}
	rs.exists = true
	s.ops = append(s.ops, schemaOp{kind: opAddStruct, path: []string{rs.name}, typ: st})
	return byColumn
}

func (s *shape) addField(rs *rowStruct, name string, t iceberg.Type) *field {
	f := &field{name: name, typ: t}
	rs.fields = append(rs.fields, f)
	s.ops = append(s.ops, schemaOp{kind: opAddField, path: []string{rs.name}, name: name, typ: t})
	return f
}

// rename renames column from to to in every struct that has it, keeping
// its field id. A field already named to, such as of a dropped column, is
// retired first.
func (s *shape) rename(from, to string) {
	for _, name := range rowStructs {
		rs := s.structs[name]
		f := rs.find(from)
		if f == nil || from == to {
			continue
		}
		if other := rs.find(to); other != nil {
			s.retire(rs, other)
		}
		s.renameField(rs, f, to)
	}
}

// retire moves f out of the way under the first free name <name>__<n>,
// where its values stay readable.
func (s *shape) retire(rs *rowStruct, f *field) {
	for n := 1; ; n++ {
		name := fmt.Sprintf("%s__%d", f.name, n)
		if rs.find(name) == nil {
			s.renameField(rs, f, name)
			return
		}
	}
}

func (s *shape) renameField(rs *rowStruct, f *field, to string) {
	s.ops = append(s.ops, schemaOp{kind: opRename, path: []string{rs.name, f.name}, name: to})
	f.name = to
}
