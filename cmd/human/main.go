package main

import (
	"context"
	"fmt"
	"os"

	"github.com/vibe-agi/human/internal/humancmd"
)

func main() {
	if err := humancmd.New().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
