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
	printVersion(os.Stdout)
}

func printVersion(w io.Writer) {
	fmt.Fprintln(w, version)
}
