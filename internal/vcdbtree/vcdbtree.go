// Package vcdbtree provides functionality to convert Vintage Story .vcdbs savegame files
// (SQLite databases) into a directory tree format optimized for deduplication algorithms.
//
// The format is called "vcdbtree" (Vintage Story Chunked Database Tree) and uses:
//   - 2-level coordinate-based subdirectories for position-based tables (chunk, mapchunk, mapregion)
//     organized by chunkZ/chunkX extracted from the ChunkPos position
//   - Flat directories for small tables (gamedata, playerdata)
//
// ChunkPos format (64 bits, MSB first):
// | reserved(1) | chunkY(9) | dimHigh(5) | guard(1) | chunkZ(21) | dimLow(5) | guard(1) | chunkX(21) |
//
// This format maximizes Restic's deduplication efficiency by ensuring unchanged BLOBs
// produce identical byte sequences, unlike SQLite's non-deterministic serialization.
// Geographic sharding by chunkZ/chunkX groups nearby chunks together, improving
// deduplication for geographically clustered changes.
package vcdbtree

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// ChunkPos bit field constants for extracting coordinates from position values.
// ChunkPos format (64 bits, MSB first):
// | reserved(1) | chunkY(9) | dimHigh(5) | guard(1) | chunkZ(21) | dimLow(5) | guard(1) | chunkX(21) |
const (
	chunkXMask   = 0x1FFFFF         // 21 bits for chunkX (bits 0-20)
	chunkZShift  = 27               // chunkZ starts at bit 27
	chunkZMask   = 0x1FFFFF         // 21 bits for chunkZ (bits 27-47)
	signBit21    = 0x100000         // Sign bit for 21-bit signed integer
	signExtend21 = ^int64(0x1FFFFF) // Mask for sign extension from 21 bits
)

// extractChunkX extracts the signed chunkX coordinate from a ChunkPos position.
func extractChunkX(position int64) int32 {
	raw := position & chunkXMask
	if raw&signBit21 != 0 {
		raw |= signExtend21
	}
	return int32(raw)
}

// extractChunkZ extracts the signed chunkZ coordinate from a ChunkPos position.
func extractChunkZ(position int64) int32 {
	raw := (position >> chunkZShift) & chunkZMask
	if raw&signBit21 != 0 {
		raw |= signExtend21
	}
	return int32(raw)
}

// Split converts a .vcdbs SQLite database into a vcdbtree directory structure.
// The output directory will contain:
//   - chunks/     - 2-level coordinate-sharded directory for chunk table (chunkZ/chunkX)
//   - mapchunks/  - 2-level coordinate-sharded directory for mapchunk table (chunkZ/chunkX)
//   - mapregions/ - 2-level coordinate-sharded directory for mapregion table (chunkZ/chunkX)
//   - gamedata/   - flat directory for gamedata table
//   - playerdata/ - flat directory for playerdata table
func Split(inputDBPath, outputDir string) error {
	// Open the SQLite database
	db, err := sql.Open("sqlite3", inputDBPath+"?mode=ro")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Process each table
	if err := splitShardedTable(db, outputDir, "chunk", "chunks"); err != nil {
		return fmt.Errorf("failed to split chunk table: %w", err)
	}

	if err := splitShardedTable(db, outputDir, "mapchunk", "mapchunks"); err != nil {
		return fmt.Errorf("failed to split mapchunk table: %w", err)
	}

	if err := splitShardedTable(db, outputDir, "mapregion", "mapregions"); err != nil {
		return fmt.Errorf("failed to split mapregion table: %w", err)
	}

	if err := splitGamedata(db, outputDir); err != nil {
		return fmt.Errorf("failed to split gamedata table: %w", err)
	}

	if err := splitPlayerdata(db, outputDir); err != nil {
		return fmt.Errorf("failed to split playerdata table: %w", err)
	}

	return nil
}

// splitShardedTable extracts data from a position-based table into a 2-level coordinate-sharded directory.
// The sharding uses chunkZ and chunkX extracted from the ChunkPos position value.
// Directory structure: <subdir>/<chunkZ>/<chunkX>/<position_hex>.bin
func splitShardedTable(db *sql.DB, outputDir, tableName, subdir string) error {
	rows, err := db.Query(fmt.Sprintf("SELECT position, data FROM %s", tableName))
	if err != nil {
		return fmt.Errorf("failed to query %s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var position int64
		var data []byte

		if err := rows.Scan(&position, &data); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if data == nil {
			continue
		}

		// Extract chunkZ and chunkX from ChunkPos
		chunkZ := extractChunkZ(position)
		chunkX := extractChunkX(position)

		// Create directory structure: <subdir>/<chunkZ>/<chunkX>/
		zDir := strconv.FormatInt(int64(chunkZ), 10)
		xDir := strconv.FormatInt(int64(chunkX), 10)
		filename := fmt.Sprintf("%016x.bin", uint64(position))

		// Create the sharded directory path
		dirPath := filepath.Join(outputDir, subdir, zDir, xDir)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
		}

		// Write the blob
		filePath := filepath.Join(dirPath, filename)
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", filePath, err)
		}
	}

	return rows.Err()
}

// splitGamedata extracts data from the gamedata table into a flat directory.
func splitGamedata(db *sql.DB, outputDir string) error {
	subdir := filepath.Join(outputDir, "gamedata")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		return fmt.Errorf("failed to create gamedata directory: %w", err)
	}

	rows, err := db.Query("SELECT savegameid, data FROM gamedata")
	if err != nil {
		return fmt.Errorf("failed to query gamedata: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var savegameid int64
		var data []byte

		if err := rows.Scan(&savegameid, &data); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if data == nil {
			continue
		}

		filename := fmt.Sprintf("%d.bin", savegameid)
		filePath := filepath.Join(subdir, filename)
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", filePath, err)
		}
	}

	return rows.Err()
}

// splitPlayerdata extracts data from the playerdata table into a flat directory.
// Player UIDs are converted to base64url format (replacing + with -, / with _) for filesystem safety.
func splitPlayerdata(db *sql.DB, outputDir string) error {
	subdir := filepath.Join(outputDir, "playerdata")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		return fmt.Errorf("failed to create playerdata directory: %w", err)
	}

	rows, err := db.Query("SELECT playeruid, data FROM playerdata")
	if err != nil {
		return fmt.Errorf("failed to query playerdata: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var playeruid string
		var data []byte

		if err := rows.Scan(&playeruid, &data); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if playeruid == "" || data == nil {
			continue
		}

		// Sanitize playeruid for filesystem (base64 to base64url)
		safeUID := sanitizePlayerUID(playeruid)
		filename := safeUID + ".bin"
		filePath := filepath.Join(subdir, filename)
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", filePath, err)
		}
	}

	return rows.Err()
}

// sanitizePlayerUID converts a base64 playeruid to filesystem-safe base64url format.
// Replaces + with -, / with _, and removes padding =.
func sanitizePlayerUID(playeruid string) string {
	s := strings.ReplaceAll(playeruid, "+", "-")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.TrimRight(s, "=")
	return s
}

// unsanitizePlayerUID converts a base64url-safe string back to original base64 format.
func unsanitizePlayerUID(safeUID string) string {
	s := strings.ReplaceAll(safeUID, "-", "+")
	s = strings.ReplaceAll(s, "_", "/")
	return s
}

// Combine reconstructs a .vcdbs SQLite database from a vcdbtree directory structure.
func Combine(inputDir, outputDBPath string) error {
	// Remove existing output file if present
	os.Remove(outputDBPath)

	// Create the new database
	db, err := sql.Open("sqlite3", outputDBPath)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	// Set page size and create schema
	if _, err := db.Exec("PRAGMA page_size = 4096"); err != nil {
		return fmt.Errorf("failed to set page size: %w", err)
	}

	schema := `
		CREATE TABLE chunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapchunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapregion (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE gamedata (savegameid integer PRIMARY KEY, data BLOB);
		CREATE TABLE playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB);
		CREATE INDEX index_playeruid ON playerdata (playeruid);
	`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Combine each table
	if err := combineShardedTable(db, inputDir, "chunk", "chunks"); err != nil {
		return fmt.Errorf("failed to combine chunk table: %w", err)
	}

	if err := combineShardedTable(db, inputDir, "mapchunk", "mapchunks"); err != nil {
		return fmt.Errorf("failed to combine mapchunk table: %w", err)
	}

	if err := combineShardedTable(db, inputDir, "mapregion", "mapregions"); err != nil {
		return fmt.Errorf("failed to combine mapregion table: %w", err)
	}

	if err := combineGamedata(db, inputDir); err != nil {
		return fmt.Errorf("failed to combine gamedata table: %w", err)
	}

	if err := combinePlayerdata(db, inputDir); err != nil {
		return fmt.Errorf("failed to combine playerdata table: %w", err)
	}

	// VACUUM for compactness and determinism
	if _, err := db.Exec("VACUUM"); err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	return nil
}

// combineShardedTable reconstructs a position-based table from a 2-level coordinate-sharded directory.
func combineShardedTable(db *sql.DB, inputDir, tableName, subdir string) error {
	subdirPath := filepath.Join(inputDir, subdir)

	// Check if directory exists
	if _, err := os.Stat(subdirPath); os.IsNotExist(err) {
		return nil // Directory doesn't exist, skip
	}

	// Use a transaction for better performance
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT OR REPLACE INTO %s (position, data) VALUES (?, ?)", tableName))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Walk the sharded directory
	err = filepath.Walk(subdirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(info.Name(), ".bin") {
			return nil
		}

		// Reconstruct position from filename (the full position is stored in the filename)
		position, err := reconstructPositionFromPath(path)
		if err != nil {
			return fmt.Errorf("failed to reconstruct position from %s: %w", path, err)
		}

		// Read the blob
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}

		// Insert into database
		if _, err := stmt.Exec(position, data); err != nil {
			return fmt.Errorf("failed to insert position %d: %w", position, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return tx.Commit()
}

// reconstructPositionFromPath extracts the position integer from a file path.
// Path structure: <subdir>/<chunkZ>/<chunkX>/<position_hex>.bin
// The full position is stored in the filename as a 16-digit hex value.
func reconstructPositionFromPath(path string) (int64, error) {
	filename := filepath.Base(path)

	if !strings.HasSuffix(filename, ".bin") {
		return 0, fmt.Errorf("invalid filename: %s", filename)
	}

	hexStr := strings.TrimSuffix(filename, ".bin")
	if len(hexStr) != 16 {
		return 0, fmt.Errorf("invalid hex length: expected 16, got %d", len(hexStr))
	}

	position, err := strconv.ParseUint(hexStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse hex %s: %w", hexStr, err)
	}

	return int64(position), nil
}

// combineGamedata reconstructs the gamedata table from a flat directory.
func combineGamedata(db *sql.DB, inputDir string) error {
	subdirPath := filepath.Join(inputDir, "gamedata")

	if _, err := os.Stat(subdirPath); os.IsNotExist(err) {
		return nil
	}

	entries, err := os.ReadDir(subdirPath)
	if err != nil {
		return fmt.Errorf("failed to read gamedata directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}

		// Parse savegameid from filename
		idStr := strings.TrimSuffix(entry.Name(), ".bin")
		savegameid, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue // Skip invalid filenames
		}

		// Read data
		data, err := os.ReadFile(filepath.Join(subdirPath, entry.Name()))
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", entry.Name(), err)
		}

		// Insert
		if _, err := db.Exec("INSERT OR REPLACE INTO gamedata (savegameid, data) VALUES (?, ?)", savegameid, data); err != nil {
			return fmt.Errorf("failed to insert savegameid %d: %w", savegameid, err)
		}
	}

	return nil
}

// combinePlayerdata reconstructs the playerdata table from a flat directory.
func combinePlayerdata(db *sql.DB, inputDir string) error {
	subdirPath := filepath.Join(inputDir, "playerdata")

	if _, err := os.Stat(subdirPath); os.IsNotExist(err) {
		return nil
	}

	entries, err := os.ReadDir(subdirPath)
	if err != nil {
		return fmt.Errorf("failed to read playerdata directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}

		// Extract safe UID from filename and unsanitize
		safeUID := strings.TrimSuffix(entry.Name(), ".bin")
		playeruid := unsanitizePlayerUID(safeUID)

		// Read data
		data, err := os.ReadFile(filepath.Join(subdirPath, entry.Name()))
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", entry.Name(), err)
		}

		// Insert
		if _, err := db.Exec("INSERT INTO playerdata (playeruid, data) VALUES (?, ?)", playeruid, data); err != nil {
			return fmt.Errorf("failed to insert playeruid %s: %w", playeruid, err)
		}
	}

	return nil
}

// GetShardedPath returns the sharded file path for a given position.
// This is useful for the backup manager to write directly to the staging directory.
// Path structure: <baseDir>/<tablePlural>/<chunkZ>/<chunkX>/<position_hex>.bin
func GetShardedPath(baseDir, tablePlural string, position int64) string {
	chunkZ := extractChunkZ(position)
	chunkX := extractChunkX(position)
	zDir := strconv.FormatInt(int64(chunkZ), 10)
	xDir := strconv.FormatInt(int64(chunkX), 10)
	filename := fmt.Sprintf("%016x.bin", uint64(position))
	return filepath.Join(baseDir, tablePlural, zDir, xDir, filename)
}

// SplitWithCache converts a .vcdbs SQLite database into a vcdbtree directory structure,
// preserving files that haven't changed to maintain their metadata (mtime, ctime).
// This optimizes for Restic efficiency - unchanged files produce zero diff.
//
// The function also removes files from the cache that no longer exist in the database,
// ensuring the cache stays in sync with the current state.
//
// Returns the number of files written (changed) and the number of files skipped (unchanged).
func SplitWithCache(inputDBPath, cacheDir string) (written, skipped int, err error) {
	// Open the SQLite database
	db, err := sql.Open("sqlite3", inputDBPath+"?mode=ro")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create output directory
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return 0, 0, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Track all files that should exist in the cache
	expectedFiles := make(map[string]bool)

	// Process each table
	w, s, err := splitShardedTableWithCache(db, cacheDir, "chunk", "chunks", expectedFiles)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to split chunk table: %w", err)
	}
	written += w
	skipped += s

	w, s, err = splitShardedTableWithCache(db, cacheDir, "mapchunk", "mapchunks", expectedFiles)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to split mapchunk table: %w", err)
	}
	written += w
	skipped += s

	w, s, err = splitShardedTableWithCache(db, cacheDir, "mapregion", "mapregions", expectedFiles)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to split mapregion table: %w", err)
	}
	written += w
	skipped += s

	w, s, err = splitGamedataWithCache(db, cacheDir, expectedFiles)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to split gamedata table: %w", err)
	}
	written += w
	skipped += s

	w, s, err = splitPlayerdataWithCache(db, cacheDir, expectedFiles)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to split playerdata table: %w", err)
	}
	written += w
	skipped += s

	// Clean up files that no longer exist in the database
	if err := cleanupStaleFiles(cacheDir, expectedFiles); err != nil {
		return written, skipped, fmt.Errorf("failed to cleanup stale files: %w", err)
	}

	return written, skipped, nil
}

// splitShardedTableWithCache extracts data with caching support.
func splitShardedTableWithCache(db *sql.DB, outputDir, tableName, subdir string, expectedFiles map[string]bool) (written, skipped int, err error) {
	rows, err := db.Query(fmt.Sprintf("SELECT position, data FROM %s", tableName))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query %s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var position int64
		var data []byte

		if err := rows.Scan(&position, &data); err != nil {
			return written, skipped, fmt.Errorf("failed to scan row: %w", err)
		}

		if data == nil {
			continue
		}

		// Get the file path
		filePath := GetShardedPath(outputDir, subdir, position)
		expectedFiles[filePath] = true

		// Check if file exists and has same content
		if fileMatchesContent(filePath, data) {
			skipped++
			continue
		}

		// Create directory and write file
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return written, skipped, fmt.Errorf("failed to create directory: %w", err)
		}

		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return written, skipped, fmt.Errorf("failed to write %s: %w", filePath, err)
		}
		written++
	}

	return written, skipped, rows.Err()
}

// splitGamedataWithCache extracts gamedata with caching support.
func splitGamedataWithCache(db *sql.DB, outputDir string, expectedFiles map[string]bool) (written, skipped int, err error) {
	subdir := filepath.Join(outputDir, "gamedata")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		return 0, 0, fmt.Errorf("failed to create gamedata directory: %w", err)
	}

	rows, err := db.Query("SELECT savegameid, data FROM gamedata")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query gamedata: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var savegameid int64
		var data []byte

		if err := rows.Scan(&savegameid, &data); err != nil {
			return written, skipped, fmt.Errorf("failed to scan row: %w", err)
		}

		if data == nil {
			continue
		}

		filename := fmt.Sprintf("%d.bin", savegameid)
		filePath := filepath.Join(subdir, filename)
		expectedFiles[filePath] = true

		if fileMatchesContent(filePath, data) {
			skipped++
			continue
		}

		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return written, skipped, fmt.Errorf("failed to write %s: %w", filePath, err)
		}
		written++
	}

	return written, skipped, rows.Err()
}

// splitPlayerdataWithCache extracts playerdata with caching support.
func splitPlayerdataWithCache(db *sql.DB, outputDir string, expectedFiles map[string]bool) (written, skipped int, err error) {
	subdir := filepath.Join(outputDir, "playerdata")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		return 0, 0, fmt.Errorf("failed to create playerdata directory: %w", err)
	}

	rows, err := db.Query("SELECT playeruid, data FROM playerdata")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query playerdata: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var playeruid string
		var data []byte

		if err := rows.Scan(&playeruid, &data); err != nil {
			return written, skipped, fmt.Errorf("failed to scan row: %w", err)
		}

		if playeruid == "" || data == nil {
			continue
		}

		safeUID := sanitizePlayerUID(playeruid)
		filename := safeUID + ".bin"
		filePath := filepath.Join(subdir, filename)
		expectedFiles[filePath] = true

		if fileMatchesContent(filePath, data) {
			skipped++
			continue
		}

		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return written, skipped, fmt.Errorf("failed to write %s: %w", filePath, err)
		}
		written++
	}

	return written, skipped, rows.Err()
}

// fileMatchesContent checks if a file exists and has the exact same content as data.
// Uses size comparison first for efficiency, then compares content.
func fileMatchesContent(filePath string, data []byte) bool {
	info, err := os.Stat(filePath)
	if err != nil {
		return false // File doesn't exist or can't be read
	}

	// Quick size check first
	if info.Size() != int64(len(data)) {
		return false
	}

	// Read and compare content
	existing, err := os.ReadFile(filePath)
	if err != nil {
		return false
	}

	return bytes.Equal(existing, data)
}

// cleanupStaleFiles removes files from the cache that are no longer in the database.
// This handles cases where chunks are deleted from the game world.
func cleanupStaleFiles(cacheDir string, expectedFiles map[string]bool) error {
	// Define the subdirectories to scan
	subdirs := []string{"chunks", "mapchunks", "mapregions", "gamedata", "playerdata"}

	for _, subdir := range subdirs {
		subdirPath := filepath.Join(cacheDir, subdir)

		if _, err := os.Stat(subdirPath); os.IsNotExist(err) {
			continue
		}

		err := filepath.Walk(subdirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			if !strings.HasSuffix(info.Name(), ".bin") {
				return nil
			}

			// If file is not in expected set, remove it
			if !expectedFiles[path] {
				if err := os.Remove(path); err != nil {
					return fmt.Errorf("failed to remove stale file %s: %w", path, err)
				}
			}

			return nil
		})

		if err != nil {
			return err
		}

		// Clean up empty directories
		if err := cleanupEmptyDirs(subdirPath); err != nil {
			return err
		}
	}

	return nil
}

// cleanupEmptyDirs removes empty directories recursively.
func cleanupEmptyDirs(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subdir := filepath.Join(dir, entry.Name())
			if err := cleanupEmptyDirs(subdir); err != nil {
				return err
			}
		}
	}

	// Re-read after cleaning subdirs
	entries, err = os.ReadDir(dir)
	if err != nil {
		return err
	}

	// Don't remove the root subdirs (chunks, mapchunks, etc.)
	// Only remove the coordinate-based subdirectories
	if len(entries) == 0 && !isRootSubdir(dir) {
		return os.Remove(dir)
	}

	return nil
}

// isRootSubdir checks if a directory is one of the root subdirectories.
func isRootSubdir(dir string) bool {
	base := filepath.Base(dir)
	rootDirs := []string{"chunks", "mapchunks", "mapregions", "gamedata", "playerdata"}
	for _, rd := range rootDirs {
		if base == rd {
			return true
		}
	}
	return false
}

// CopyFileIfChanged copies a file only if the destination doesn't exist or has different content.
// Returns true if the file was written, false if skipped.
func CopyFileIfChanged(src, dst string) (bool, error) {
	// Read source file
	srcData, err := os.ReadFile(src)
	if err != nil {
		return false, fmt.Errorf("failed to read source file: %w", err)
	}

	// Check if destination matches
	if fileMatchesContent(dst, srcData) {
		return false, nil
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return false, fmt.Errorf("failed to create directory: %w", err)
	}

	// Write destination file
	if err := os.WriteFile(dst, srcData, 0644); err != nil {
		return false, fmt.Errorf("failed to write destination file: %w", err)
	}

	return true, nil
}

// CopyDirIfChanged recursively copies a directory, only writing files that have changed.
// Returns the number of files written and skipped.
func CopyDirIfChanged(src, dst string) (written, skipped int, err error) {
	return copyDirIfChangedWithTracking(src, dst, nil)
}

// copyDirIfChangedWithTracking is the internal implementation that tracks expected files.
func copyDirIfChangedWithTracking(src, dst string, expectedFiles map[string]bool) (written, skipped int, err error) {
	err = filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		if expectedFiles != nil {
			expectedFiles[dstPath] = true
		}

		changed, err := CopyFileIfChanged(path, dstPath)
		if err != nil {
			return err
		}

		if changed {
			written++
		} else {
			skipped++
		}

		return nil
	})

	return written, skipped, err
}

// SyncDir synchronizes a source directory to a destination, copying changed files
// and removing files in the destination that don't exist in the source.
// Returns the number of files written, skipped, and removed.
func SyncDir(src, dst string) (written, skipped, removed int, err error) {
	// Track expected files
	expectedFiles := make(map[string]bool)

	// Copy changed files
	written, skipped, err = copyDirIfChangedWithTracking(src, dst, expectedFiles)
	if err != nil {
		return written, skipped, 0, err
	}

	// Remove files in dst that don't exist in src
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		err = filepath.Walk(dst, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}

			if info.IsDir() {
				return nil
			}

			if !expectedFiles[path] {
				if rmErr := os.Remove(path); rmErr != nil {
					return rmErr
				}
				removed++
			}

			return nil
		})

		if err != nil {
			return written, skipped, removed, err
		}

		// Clean up empty directories
		cleanupEmptyDirsInPath(dst)
	}

	return written, skipped, removed, nil
}

// cleanupEmptyDirsInPath removes empty directories within a path.
func cleanupEmptyDirsInPath(root string) {
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.IsDir() || path == root {
			return nil
		}

		entries, err := os.ReadDir(path)
		if err == nil && len(entries) == 0 {
			os.Remove(path)
		}
		return nil
	})
}

// SyncFile copies a single file if changed, or removes the destination if source doesn't exist.
// Returns: written (1 if written, 0 otherwise), removed (1 if removed, 0 otherwise), error
func SyncFile(src, dst string) (written, removed int, err error) {
	_, srcErr := os.Stat(src)
	if os.IsNotExist(srcErr) {
		// Source doesn't exist, remove destination if it exists
		if _, dstErr := os.Stat(dst); dstErr == nil {
			if err := os.Remove(dst); err != nil {
				return 0, 0, err
			}
			return 0, 1, nil
		}
		return 0, 0, nil
	}

	if srcErr != nil {
		return 0, 0, srcErr
	}

	// Source exists, copy if changed
	changed, err := CopyFileIfChanged(src, dst)
	if err != nil {
		return 0, 0, err
	}

	if changed {
		return 1, 0, nil
	}
	return 0, 0, nil
}
