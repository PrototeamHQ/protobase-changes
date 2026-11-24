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

func TestRunPrintsVersion(t *testing.T) {
	var out, errOut strings.Builder
	if err := run([]string{"--version"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "dev\n" {
		t.Fatalf("printed %q, want %q", got, "dev\n")
	}
}

func TestRunRequiresConfig(t *testing.T) {
	var out, errOut strings.Builder
	if err := run(nil, &out, &errOut); err == nil || err.Error() != "--config is required" {
		t.Fatalf("got %v, want the missing --config error", err)
	}
}
