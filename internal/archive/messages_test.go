package archive

import "testing"

func TestParseRename(t *testing.T) {
	r, err := parseRename([]byte(`{"rename":{"schema":"public","table":"messages","from":"text","to":"body"}}`))
	if err != nil || *r != (rename{Schema: "public", Table: "messages", From: "text", To: "body"}) {
		t.Fatalf("got %+v, %v", r, err)
	}
	if r, err := parseRename([]byte(`{"index":{}}`)); r != nil || err != nil {
		t.Fatalf("a schema message without a rename gave %+v, %v", r, err)
	}
	for _, bad := range []string{`not json`, `{"rename":{"table":"messages","from":"a","to":"b"}}`} {
		if _, err := parseRename([]byte(bad)); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
}
