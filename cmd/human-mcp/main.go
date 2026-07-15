package main

import (
	"context"
	"fmt"
	"os"

	"github.com/vibe-agi/human/internal/humanmcpcmd"
)

func main() {
	if err := humanmcpcmd.New().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
