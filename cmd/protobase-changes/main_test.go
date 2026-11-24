package main

import (
	"strings"
	"testing"
)

func TestPrintVersion(t *testing.T) {
	var out strings.Builder
	if err := printVersion(&out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "dev\n" {
		t.Fatalf("printed %q, want %q", got, "dev\n")
	}
}
