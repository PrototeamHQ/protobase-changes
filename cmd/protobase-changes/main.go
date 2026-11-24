// Command protobase-changes prints its version and exits.
package main

import (
	"fmt"
	"io"
	"os"
)

// version is set at release builds with -ldflags "-X main.version=<version>".
var version = "dev"

func main() {
	if err := printVersion(os.Stdout); err != nil {
		os.Exit(1)
	}
}

func printVersion(w io.Writer) error {
	_, err := fmt.Fprintln(w, version)
	return err
}
