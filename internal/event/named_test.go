package event

import (
	"testing"

	"github.com/PrototeamHQ/protobase-changes/internal/change"
	"github.com/PrototeamHQ/protobase-changes/internal/token"
)

func deletes(resource string, c *change.Change) []string {
	if resource == "messages" && c.Operation == change.Delete {
		return []string{"messages.removed"}
	}
	return nil
}

func TestNamedRaisesOneEventPerChangeAndType(t *testing.T) {
	got := Named("https://shop.example", entry(), token.Raw, deletes)
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	want := `{"specversion":"1.0","id":"` + got[0].ID + `","source":"https://shop.example","type":"messages.removed",` +
		`"time":"2026-11-24T10:00:00Z","datacontenttype":"application/json","cursor":"c1","data":{"changes":[` +
		`{"resource":"messages","operation":"delete","key":{"id":"2"},"old":{"id":"2"}}]}}`
	if e := encode(t, got[0]); e != want {
		t.Fatalf("got  %s\nwant %s", e, want)
	}
	if again := Named("https://shop.example", entry(), token.Thin, deletes); again[0].ID != got[0].ID || again[0].Data.Changes[0].OldRow != nil {
		t.Fatalf("thin replay %+v, want the same id without rows", again[0])
	}
}
