package main

import (
	"context"
	"fmt"
	"os"

	"github.com/vibe-agi/human/internal/humandcmd"
)

func main() {
	if err := humandcmd.New().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
