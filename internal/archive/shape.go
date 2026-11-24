package archive

import (
	"fmt"

	"github.com/apache/iceberg-go"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

// retiredDoc marks a retired field in the schema, so it stays retired when
// the table is loaded again.
const retiredDoc = "Retired: the values of an earlier column, before it changed type or another column was renamed to its name."

// field is one column's field in a row struct. A rename changes its name in
// place, so the values a segment holds follow the field to its final name.
type field struct {
	name string
	typ  iceberg.Type
	doc  string
	// retired is a field moved aside, which no column writes to again.
	retired bool
}

// rowStruct is one row struct of a table, such as new.
type rowStruct struct {
	name   string
	exists bool
	fields []*field
}

// find returns the live field of column name.
func (rs *rowStruct) find(name string) *field {
	f := rs.holder(name)
	if f == nil || f.retired {
		return nil
	}
	return f
}

// holder returns the field named name, live or retired.
func (rs *rowStruct) holder(name string) *field {
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
	opRetire
	opPromote
)

// schemaOp is one schema change, naming fields as they are at that point.
type schemaOp struct {
	kind opKind
	// path is the struct, then for a rename or promotion the field.
	path []string
	// name is the added field, or the new name of a renamed or retired one.
	name string
	// doc is a renamed field's doc.
	doc string
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
				rs.fields = append(rs.fields, &field{name: sub.Name, typ: sub.Type, doc: sub.Doc, retired: sub.Doc == retiredDoc})
			}
		}
		s.structs[name] = rs
	}
	return s
}

// columns evolves struct name to hold the columns cols that it stores and
// returns the field of each by column name, or nil when it stores none.
// A new column adds a field, a widening promotes one, and an incompatible
// type retires the old field under another name and adds a new one. A
// retired field never takes a column's values: one holding a new column's
// name moves aside again.
func (s *shape) columns(name string, cols []change.Column) map[string]*field {
	cols = structColumns(name, cols)
	if len(cols) == 0 {
		return nil
	}
	rs := s.structs[name]
	if !rs.exists {
		return s.addStruct(rs, cols)
	}
	taken := make(map[string]bool, len(cols))
	for _, c := range cols {
		taken[c.Name] = true
	}
	byColumn := make(map[string]*field, len(cols))
	for _, c := range cols {
		t := columnType(c.Type)
		f := rs.find(c.Name)
		switch {
		case f == nil:
			if h := rs.holder(c.Name); h != nil {
				s.retire(rs, h, taken)
			}
			f = s.addField(rs, c.Name, t)
		case holds(f.typ, t):
		case promotes(f.typ, t):
			s.ops = append(s.ops, schemaOp{kind: opPromote, path: []string{rs.name, f.name}, typ: t})
			f.typ = t
		default:
			s.retire(rs, f, taken)
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

// renamedDoc is the doc of a field the rename at position at named to, from
// column from, which tells it apart from a field of an earlier column of
// that name when the rename is applied again.
func renamedDoc(from string, at position) string {
	return fmt.Sprintf("Renamed from %s at WAL position %s.", from, at)
}

// rename renames column from to to in every struct that has it, keeping
// its field id. A field already named to, such as of a dropped column, is
// retired first, also in a struct without from: to names the renamed column
// from then on. A field this rename already named, as when the rename is
// applied again after its schema updates, stays.
func (s *shape) rename(from, to string, at position) {
	if from == to {
		return
	}
	doc := renamedDoc(from, at)
	for _, name := range rowStructs {
		rs := s.structs[name]
		f := rs.find(from)
		if f == nil {
			if other := rs.find(to); other != nil && other.doc != doc {
				s.retire(rs, other, map[string]bool{to: true})
			}
			continue
		}
		if other := rs.holder(to); other != nil {
			s.retire(rs, other, map[string]bool{to: true})
		}
		s.ops = append(s.ops, schemaOp{kind: opRename, path: []string{rs.name, f.name}, name: to, doc: doc})
		f.name = to
		f.doc = doc
	}
}

// retire moves f out of the way under the first name <name>__<n> that no
// field holds and that is not taken by a column, where its values stay
// readable, and marks it retired.
func (s *shape) retire(rs *rowStruct, f *field, taken map[string]bool) {
	for n := 1; ; n++ {
		name := fmt.Sprintf("%s__%d", f.name, n)
		if rs.holder(name) == nil && !taken[name] {
			s.ops = append(s.ops, schemaOp{kind: opRetire, path: []string{rs.name, f.name}, name: name})
			f.name = name
			f.doc = retiredDoc
			f.retired = true
			return
		}
	}
}
