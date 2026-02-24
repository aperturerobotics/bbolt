package main

import (
	"fmt"
	"os"

	"github.com/aperturerobotics/bbolt/cmd/bbolt/command"
)

func main() {
	rootCmd := command.NewRootCommand()
	if err := rootCmd.Execute(); err != nil {
		if rootCmd.SilenceErrors {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		os.Exit(1)
	}
}
