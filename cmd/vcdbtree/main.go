// Command vcdbtree converts Vintage Story .vcdbs savegame files to and from the
// vcdbtree directory format optimized for deduplication algorithms.
//
// Usage:
//
//	vcdbtree split <input.vcdbs> <output_dir>
//	    Convert a .vcdbs SQLite database into a vcdbtree directory structure.
//
//	vcdbtree combine <input_dir> <output.vcdbs>
//	    Reconstruct a .vcdbs SQLite database from a vcdbtree directory structure.
//
// The vcdbtree format uses hex-sharded subdirectories for position-based tables
// (chunk, mapchunk, mapregion) and flat directories for small tables (gamedata,
// playerdata). This format maximizes Restic's deduplication efficiency.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/renorris/vintagestory-restic/internal/vcdbtree"
)

const usage = `vcdbtree - Convert Vintage Story .vcdbs savegames to/from deduplication-optimized format

Usage:
  vcdbtree split <input.vcdbs> <output_dir>
      Convert a .vcdbs SQLite database into a vcdbtree directory structure.
      The output directory will contain:
        - chunks/      2-level hex-sharded directory for chunk table
        - mapchunks/   2-level hex-sharded directory for mapchunk table
        - mapregions/  2-level hex-sharded directory for mapregion table
        - gamedata/    flat directory for gamedata table
        - playerdata/  flat directory for playerdata table

  vcdbtree combine <input_dir> <output.vcdbs>
      Reconstruct a .vcdbs SQLite database from a vcdbtree directory structure.

Examples:
  vcdbtree split /gamedata/Backups/backup.vcdbs /tmp/backup-tree
  vcdbtree combine /tmp/backup-tree /gamedata/Saves/restored.vcdbs
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "split":
		if len(os.Args) != 4 {
			fmt.Fprintf(os.Stderr, "Usage: vcdbtree split <input.vcdbs> <output_dir>\n")
			os.Exit(1)
		}
		inputDB := os.Args[2]
		outputDir := os.Args[3]

		fmt.Printf("Splitting %s -> %s\n", inputDB, outputDir)
		start := time.Now()

		if err := vcdbtree.Split(inputDB, outputDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Split complete in %v\n", time.Since(start))

	case "combine":
		if len(os.Args) != 4 {
			fmt.Fprintf(os.Stderr, "Usage: vcdbtree combine <input_dir> <output.vcdbs>\n")
			os.Exit(1)
		}
		inputDir := os.Args[2]
		outputDB := os.Args[3]

		fmt.Printf("Combining %s -> %s\n", inputDir, outputDB)
		start := time.Now()

		if err := vcdbtree.Combine(inputDir, outputDB); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Combine complete in %v\n", time.Since(start))

	case "-h", "--help", "help":
		fmt.Print(usage)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}
}
