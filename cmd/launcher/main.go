package main

import (
	"fmt"
	"os"

	"github.com/renorris/vintagestory-restic/internal/downloader"
)

func main() {
	targetDir := "/serverbinaries"

	if err := downloader.DoServerBinaryDownload(targetDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
