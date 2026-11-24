package config

import (
	"strings"
	"testing"
)

const archived = `{"listen": ":8080", "apps": [{"name": "shop", "database_url": "postgres://localhost/shop",
  "slot": "protobase_changes", "publication": "protobase_changes", "tail_transactions": 1000,
  "issuer": "https://shop.example", "key_files": ["/run/secrets/shop_key"],
  "archive": {"namespace": "shop", "segment_bytes": 67108864, "segment_seconds": 900, "retention_hours": 168,
    "catalog": {"type": "rest", "url": "https://catalog.example/catalog", "warehouse": "shop",
      "properties": {"s3.endpoint": "https://s3.example"}}}}]}`

func TestParseArchive(t *testing.T) {
	c, err := Parse(strings.NewReader(archived))
	if err != nil {
		t.Fatal(err)
	}
	a := c.Apps[0].Archive
	if a == nil || a.Catalog.Type != RESTCatalog || a.Catalog.Properties["s3.endpoint"] != "https://s3.example" || a.SegmentSeconds != 900 {
		t.Fatalf("parsed %+v", a)
	}
}

func TestParseSQLArchive(t *testing.T) {
	input := strings.Replace(archived, `"type": "rest", "url": "https://catalog.example/catalog", "warehouse": "shop"`,
		`"type": "sql", "path": ".protobase/changes/catalog.db", "warehouse": "file:///srv/changes"`, 1)
	if _, err := Parse(strings.NewReader(input)); err != nil {
		t.Fatal(err)
	}
}

func TestParseRefusesArchive(t *testing.T) {
	cases := map[string][2]string{
		"namespace":        {`"namespace": "shop"`, `"namespace": "Shop"`},
		"segment bytes":    {`"segment_bytes": 67108864`, `"segment_bytes": 0`},
		"segment seconds":  {`"segment_seconds": 900`, `"segment_seconds": 901`},
		"no seconds":       {`"segment_seconds": 900`, `"segment_seconds": 0`},
		"retention":        {`"retention_hours": 168`, `"retention_hours": 0`},
		"catalog type":     {`"type": "rest"`, `"type": "glue"`},
		"catalog url":      {`"https://catalog.example/catalog"`, `"catalog.example"`},
		"rest path":        {`"warehouse": "shop"`, `"warehouse": "shop", "path": "x.db"`},
		"credential+token": {`"warehouse": "shop"`, `"warehouse": "shop", "credential": "a:b", "token": "t"`},
		"sql without path": {`"type": "rest", "url": "https://catalog.example/catalog"`, `"type": "sql"`},
		"sql with url":     {`"type": "rest"`, `"type": "sql", "path": "x.db"`},
		"unknown field":    {`"retention_hours": 168`, `"retention_hours": 168, "retention": 1`},
	}
	for name, edit := range cases {
		input := strings.Replace(archived, edit[0], edit[1], 1)
		if input == archived {
			t.Fatalf("%s: the edit changed nothing", name)
		}
		if _, err := Parse(strings.NewReader(input)); err == nil {
			t.Errorf("%s: parsed without error", name)
		}
	}
}
