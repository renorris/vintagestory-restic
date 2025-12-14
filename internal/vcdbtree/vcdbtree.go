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
