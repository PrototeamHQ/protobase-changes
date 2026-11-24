package main

import (
	"strings"
	"testing"
)

func TestPrintVersion(t *testing.T) {
	var out strings.Builder
	printVersion(&out)
	if got := out.String(); got != "dev\n" {
		t.Fatalf("printed %q, want %q", got, "dev\n")
	}
}
