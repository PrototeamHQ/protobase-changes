package archive

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
)

const (
	oidText = 25
	oidJSON = 114
)

func col(name string, oid uint32, key bool) change.Column {
	return change.Column{Name: name, Type: oid, Modifier: -1, Key: key}
}

// staged commits ops to a table of schema in the development catalog and
// returns the resulting schema.
func staged(t *testing.T, schema *iceberg.Schema, ops []schemaOp) *iceberg.Schema {
	t.Helper()
	ctx := context.Background()
	cat := devCatalog(t)
	ident := identifier("shop", messages)
	if err := ensureNamespace(ctx, cat, ident[:1]); err != nil {
		t.Fatal(err)
	}
	if err := ensureNamespace(ctx, cat, ident[:2]); err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.CreateTable(ctx, ident, schema)
	if err != nil {
		t.Fatal(err)
	}
	if tbl, err = evolve(ctx, tbl, ops); err != nil {
		t.Fatal(err)
	}
	return tbl.Schema()
}

func fieldID(t *testing.T, schema *iceberg.Schema, name string) (int, iceberg.Type) {
	t.Helper()
	f, ok := schema.FindFieldByName(name)
	if !ok {
		t.Fatalf("no field %s in %s", name, schema)
	}
	return f.ID, f.Type
}

func TestEvolveKeepsFieldIDs(t *testing.T) {
	first := []change.Column{col("id", oidInt4, true), col("text", oidText, false), col("n", oidInt4, false)}
	s := shapeOf(baseSchema())
	s.columns(structKey, first)
	s.columns(structNew, first)
	v1 := staged(t, baseSchema(), s.ops)
	idID, _ := fieldID(t, v1, "new.id")
	textID, _ := fieldID(t, v1, "new.text")
	nID, _ := fieldID(t, v1, "new.n")
	keyID, _ := fieldID(t, v1, "key.id")
	if _, ok := v1.FindFieldByName("old"); ok {
		t.Fatal("old struct added without columns")
	}

	// Rename text to body, widen id and n, and turn n into json: a new field.
	s = shapeOf(v1)
	s.rename("text", "body", position{lsn: 100})
	second := []change.Column{col("id", oidInt8, true), col("body", oidText, false), col("n", oidJSON, false), col("extra", oidBool, false)}
	s.columns(structKey, second)
	s.columns(structNew, second)
	s.columns(structOld, second)
	v2 := staged(t, v1, s.ops)

	if id, typ := fieldID(t, v2, "new.id"); id != idID || !typ.Equals(iceberg.PrimitiveTypes.Int64) {
		t.Errorf("new.id: id %d type %s, want id %d long", id, typ, idID)
	}
	if id, typ := fieldID(t, v2, "key.id"); id != keyID || !typ.Equals(iceberg.PrimitiveTypes.Int64) {
		t.Errorf("key.id: id %d type %s, want id %d long", id, typ, keyID)
	}
	if id, _ := fieldID(t, v2, "new.body"); id != textID {
		t.Errorf("new.body has id %d, want the id of text %d", id, textID)
	}
	if id, typ := fieldID(t, v2, "new.n__1"); id != nID || !typ.Equals(iceberg.PrimitiveTypes.Int32) {
		t.Errorf("new.n__1: id %d type %s, want the retired int field %d", id, typ, nID)
	}
	if id, typ := fieldID(t, v2, "new.n"); id == nID || !typ.Equals(iceberg.PrimitiveTypes.String) {
		t.Errorf("new.n: id %d type %s, want a new string field", id, typ)
	}
	fieldID(t, v2, "new.extra")
	fieldID(t, v2, "old.body")
	if _, ok := v2.FindFieldByName("new.text"); ok {
		t.Error("new.text survived the rename")
	}
}

func TestRenameOntoAnExistingName(t *testing.T) {
	cols := []change.Column{col("a", oidText, false), col("b", oidText, false)}
	s := shapeOf(baseSchema())
	s.columns(structNew, cols)
	v1 := staged(t, baseSchema(), s.ops)
	aID, _ := fieldID(t, v1, "new.a")
	bID, _ := fieldID(t, v1, "new.b")

	// a was dropped, and b renamed to a.
	s = shapeOf(v1)
	s.rename("b", "a", position{lsn: 100})
	got := s.columns(structNew, []change.Column{col("a", oidText, false)})
	v2 := staged(t, v1, s.ops)
	if id, _ := fieldID(t, v2, "new.a"); id != bID {
		t.Errorf("new.a has id %d, want b's %d", id, bID)
	}
	if id, _ := fieldID(t, v2, "new.a__1"); id != aID {
		t.Errorf("new.a__1 has id %d, want the old a's %d", id, aID)
	}
	if got["a"].name != "a" {
		t.Errorf("column a maps to field %s", got["a"].name)
	}
}

// b was the key and was dropped, and a is renamed to b and made the key: the
// key's b belongs to the dropped column, though the key never held a.
func TestRenameRetiresAnEarlierColumnOfTheNewName(t *testing.T) {
	s := shapeOf(baseSchema())
	s.columns(structKey, []change.Column{col("b", oidInt4, true)})
	s.columns(structNew, []change.Column{col("a", oidText, false), col("b", oidInt4, true)})
	v1 := staged(t, baseSchema(), s.ops)
	keyB, _ := fieldID(t, v1, "key.b")
	aID, _ := fieldID(t, v1, "new.a")

	at := position{lsn: 200, step: 1}
	s = shapeOf(v1)
	s.rename("a", "b", at)
	ops := s.ops

	// Applied again after all of its schema updates, or only some, as after
	// a crash, the rename finishes without retiring the field it renamed.
	for n := range len(ops) + 1 {
		partial := staged(t, v1, ops[:n])
		s = shapeOf(partial)
		s.rename("a", "b", at)
		v2 := staged(t, partial, s.ops)
		if _, ok := v2.FindFieldByName("key.b"); ok {
			t.Errorf("after %d of %d updates: the dropped column's key.b stayed live", n, len(ops))
		}
		if id, _ := fieldID(t, v2, "key.b__1"); id != keyB {
			t.Errorf("after %d of %d updates: key.b__1 has id %d, want the dropped b's %d", n, len(ops), id, keyB)
		}
		if id, _ := fieldID(t, v2, "new.b"); id != aID {
			t.Errorf("after %d of %d updates: new.b has id %d, want a's %d", n, len(ops), id, aID)
		}
	}
}

func TestRenameOfAFieldAddedInTheSameSegment(t *testing.T) {
	s := shapeOf(baseSchema())
	before := s.columns(structNew, []change.Column{col("a", oidText, false)})
	s.rename("a", "b", position{lsn: 100})
	after := s.columns(structNew, []change.Column{col("b", oidText, false), col("a", oidInt4, false)})
	v := staged(t, baseSchema(), s.ops)
	if before["a"] != after["b"] || before["a"].name != "b" {
		t.Fatal("the renamed column's values do not follow its field")
	}
	if _, typ := fieldID(t, v, "new.a"); !typ.Equals(iceberg.PrimitiveTypes.Int32) {
		t.Errorf("new.a is %s, want the new int column", typ)
	}
	fieldID(t, v, "new.b")
}

func TestRetiredFieldsTakeNoColumn(t *testing.T) {
	s := shapeOf(baseSchema())
	s.columns(structNew, []change.Column{col("a", oidInt4, false)})
	v1 := staged(t, baseSchema(), s.ops)
	aID, _ := fieldID(t, v1, "new.a")

	// a turns into text while a new int column takes the name a__1.
	s = shapeOf(v1)
	got := s.columns(structNew, []change.Column{col("a", oidText, false), col("a__1", oidInt4, false)})
	v2 := staged(t, v1, s.ops)
	if id, _ := fieldID(t, v2, "new.a__2"); id != aID {
		t.Errorf("new.a__2 has id %d, want the old a's %d", id, aID)
	}
	if id, _ := fieldID(t, v2, "new.a__1"); id == aID || got["a__1"].retired {
		t.Errorf("column a__1 writes to the retired field %d", id)
	}

	// Loaded again, the retired field stays apart from a later column of its name.
	s = shapeOf(v2)
	got = s.columns(structNew, []change.Column{col("a__2", oidInt4, false)})
	v3 := staged(t, v2, s.ops)
	if id, _ := fieldID(t, v3, "new.a__2"); id == aID || got["a__2"].retired {
		t.Errorf("column a__2 writes to the retired field %d", id)
	}
	if id, _ := fieldID(t, v3, "new.a__2__1"); id != aID {
		t.Errorf("new.a__2__1 has id %d, want the old a's %d", id, aID)
	}
}
