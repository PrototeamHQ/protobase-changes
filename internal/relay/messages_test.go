package relay

import (
	"errors"
	"testing"
)

func TestParse(t *testing.T) {
	m, err := parse([]byte(`{"type":"register","app":"shop","id":"r1","sid":"s9","cursor":"c",
		"subs":[{"resource":"messages","where":{"eq":["channel_id","xyz"]}}],"fields":["body"]}`))
	if err != nil || m.Type != RegisterType || m.App != "shop" || m.ID != "r1" || m.Sid != "s9" ||
		m.Cursor != "c" || len(m.Subs) != 1 || m.Subs[0].Resource != "messages" || m.Fields[0] != "body" {
		t.Fatalf("got %+v %v", m, err)
	}
	if m, err := parse([]byte(`{"type":"unregister","app":"shop","id":"r1"}` + "\n")); err != nil || m.Type != UnregisterType {
		t.Fatalf("unregister %+v %v", m, err)
	}
	for _, data := range []string{`{"type":"gap","app":"shop","id":"r1"}`, `{"type":"register","id":"r1"}`,
		`{"type":"register","app":"shop","id":"r1","x":1}`, `{"type":"unregister","app":"shop","id":"r1"} {}`,
		`{"type":"unregister","app":"shop","id":"r1"}]`, `[]`} {
		if _, err := parse([]byte(data)); !errors.Is(err, errMessage) {
			t.Errorf("%s: %v", data, err)
		}
	}
}
