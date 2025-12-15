package vcdbtree

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// createTestDatabase creates a test .vcdbs database with sample data.
func createTestDatabase(t *testing.T, dbPath string) {
	t.Helper()

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create schema matching Vintage Story format
	schema := `
		PRAGMA page_size = 4096;
		CREATE TABLE chunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapchunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapregion (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE gamedata (savegameid integer PRIMARY KEY, data BLOB);
		CREATE TABLE playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB);
		CREATE INDEX index_playeruid ON playerdata (playeruid);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Insert test data

	// Chunks with various position values
	chunks := []struct {
		position int64
		data     []byte
	}{
		{0, []byte("chunk_zero")},
		{12345678901234, []byte("chunk_large_position")},
		{0x00000012abff341c, []byte("chunk_hex_example")}, // 78988977180
		{0x0bff341c00005678, []byte("chunk_another")},     // fits in int64
	}
	for _, c := range chunks {
		if _, err := db.Exec("INSERT INTO chunk (position, data) VALUES (?, ?)", c.position, c.data); err != nil {
			t.Fatalf("Failed to insert chunk: %v", err)
		}
	}

	// Mapchunks
	mapchunks := []struct {
		position int64
		data     []byte
	}{
		{100, []byte("mapchunk_100")},
		{999999999, []byte("mapchunk_large")},
	}
	for _, mc := range mapchunks {
		if _, err := db.Exec("INSERT INTO mapchunk (position, data) VALUES (?, ?)", mc.position, mc.data); err != nil {
			t.Fatalf("Failed to insert mapchunk: %v", err)
		}
	}

	// Mapregions
	if _, err := db.Exec("INSERT INTO mapregion (position, data) VALUES (?, ?)", 42, []byte("mapregion_data")); err != nil {
		t.Fatalf("Failed to insert mapregion: %v", err)
	}

	// Gamedata
	if _, err := db.Exec("INSERT INTO gamedata (savegameid, data) VALUES (?, ?)", 1, []byte("gamedata_blob")); err != nil {
		t.Fatalf("Failed to insert gamedata: %v", err)
	}

	// Playerdata with base64-like UIDs (containing + and /)
	players := []struct {
		playeruid string
		data      []byte
	}{
		{"B5fZ7vAsz3Kt+fmEV8GeK8Gu", []byte("player1_data")},
		{"ABC123/DEF456+xyz", []byte("player2_data")},
		{"SimplePlayer", []byte("player3_data")},
	}
	for _, p := range players {
		if _, err := db.Exec("INSERT INTO playerdata (playeruid, data) VALUES (?, ?)", p.playeruid, p.data); err != nil {
			t.Fatalf("Failed to insert playerdata: %v", err)
		}
	}
}

func TestSplit_CreatesCorrectStructure(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	outputDir := filepath.Join(tmpDir, "output")

	createTestDatabase(t, dbPath)

	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed: %v", err)
	}

	// Verify directory structure exists
	expectedDirs := []string{
		"chunks",
		"mapchunks",
		"mapregions",
		"gamedata",
		"playerdata",
	}
	for _, dir := range expectedDirs {
		path := filepath.Join(outputDir, dir)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("Expected directory %s to exist: %v", dir, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("Expected %s to be a directory", dir)
		}
	}
}

func TestSplit_CreatesShardedChunkFiles(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	outputDir := filepath.Join(tmpDir, "output")

	createTestDatabase(t, dbPath)

	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed: %v", err)
	}

	// Check that chunk with position 0x00000012abff341c is properly sharded by chunkZ/chunkX
	// Position 0x00000012abff341c:
	//   chunkX (bits 0-20, signed): -52196
	//   chunkZ (bits 27-47, signed): 597
	expectedPath := filepath.Join(outputDir, "chunks", "597", "-52196", "00000012abff341c.bin")
	data, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read sharded chunk file: %v", err)
	}
	if string(data) != "chunk_hex_example" {
		t.Errorf("Chunk data = %q, want %q", string(data), "chunk_hex_example")
	}
}

func TestSplit_CreatesPlayerDataWithSanitizedFilenames(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	outputDir := filepath.Join(tmpDir, "output")

	createTestDatabase(t, dbPath)

	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed: %v", err)
	}

	// Check that playeruid "B5fZ7vAsz3Kt+fmEV8GeK8Gu" is sanitized to "B5fZ7vAsz3Kt-fmEV8GeK8Gu"
	expectedPath := filepath.Join(outputDir, "playerdata", "B5fZ7vAsz3Kt-fmEV8GeK8Gu.bin")
	data, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read playerdata file: %v", err)
	}
	if string(data) != "player1_data" {
		t.Errorf("Player data = %q, want %q", string(data), "player1_data")
	}

	// Check that playeruid "ABC123/DEF456+xyz" is sanitized to "ABC123_DEF456-xyz"
	expectedPath2 := filepath.Join(outputDir, "playerdata", "ABC123_DEF456-xyz.bin")
	data2, err := os.ReadFile(expectedPath2)
	if err != nil {
		t.Fatalf("Failed to read playerdata file: %v", err)
	}
	if string(data2) != "player2_data" {
		t.Errorf("Player data = %q, want %q", string(data2), "player2_data")
	}
}

func TestSplit_CreatesGameDataFiles(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	outputDir := filepath.Join(tmpDir, "output")

	createTestDatabase(t, dbPath)

	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed: %v", err)
	}

	expectedPath := filepath.Join(outputDir, "gamedata", "1.bin")
	data, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read gamedata file: %v", err)
	}
	if string(data) != "gamedata_blob" {
		t.Errorf("Gamedata = %q, want %q", string(data), "gamedata_blob")
	}
}

func TestCombine_ReconstructsDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	outputDir := filepath.Join(tmpDir, "split")
	restoredPath := filepath.Join(tmpDir, "restored.vcdbs")

	createTestDatabase(t, dbPath)

	// Split the database
	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed: %v", err)
	}

	// Combine back
	if err := Combine(outputDir, restoredPath); err != nil {
		t.Fatalf("Combine() failed: %v", err)
	}

	// Verify restored database has correct data
	db, err := sql.Open("sqlite3", restoredPath)
	if err != nil {
		t.Fatalf("Failed to open restored database: %v", err)
	}
	defer db.Close()

	// Check chunk count
	var chunkCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM chunk").Scan(&chunkCount); err != nil {
		t.Fatalf("Failed to count chunks: %v", err)
	}
	if chunkCount != 4 {
		t.Errorf("Chunk count = %d, want 4", chunkCount)
	}

	// Check specific chunk data
	var data []byte
	if err := db.QueryRow("SELECT data FROM chunk WHERE position = ?", 0x00000012abff341c).Scan(&data); err != nil {
		t.Fatalf("Failed to query chunk: %v", err)
	}
	if string(data) != "chunk_hex_example" {
		t.Errorf("Chunk data = %q, want %q", string(data), "chunk_hex_example")
	}

	// Check mapchunk count
	var mapchunkCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM mapchunk").Scan(&mapchunkCount); err != nil {
		t.Fatalf("Failed to count mapchunks: %v", err)
	}
	if mapchunkCount != 2 {
		t.Errorf("Mapchunk count = %d, want 2", mapchunkCount)
	}

	// Check mapregion count
	var mapregionCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM mapregion").Scan(&mapregionCount); err != nil {
		t.Fatalf("Failed to count mapregions: %v", err)
	}
	if mapregionCount != 1 {
		t.Errorf("Mapregion count = %d, want 1", mapregionCount)
	}

	// Check gamedata
	var gamedataCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM gamedata").Scan(&gamedataCount); err != nil {
		t.Fatalf("Failed to count gamedata: %v", err)
	}
	if gamedataCount != 1 {
		t.Errorf("Gamedata count = %d, want 1", gamedataCount)
	}

	// Check playerdata count
	var playerCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM playerdata").Scan(&playerCount); err != nil {
		t.Fatalf("Failed to count playerdata: %v", err)
	}
	if playerCount != 3 {
		t.Errorf("Playerdata count = %d, want 3", playerCount)
	}
}

func TestRoundTrip_PreservesAllData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	outputDir := filepath.Join(tmpDir, "split")
	restoredPath := filepath.Join(tmpDir, "restored.vcdbs")

	createTestDatabase(t, dbPath)

	// Split and combine
	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed: %v", err)
	}
	if err := Combine(outputDir, restoredPath); err != nil {
		t.Fatalf("Combine() failed: %v", err)
	}

	// Open both databases
	origDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open original database: %v", err)
	}
	defer origDB.Close()

	restoredDB, err := sql.Open("sqlite3", restoredPath)
	if err != nil {
		t.Fatalf("Failed to open restored database: %v", err)
	}
	defer restoredDB.Close()

	// Compare all chunks
	origRows, err := origDB.Query("SELECT position, data FROM chunk ORDER BY position")
	if err != nil {
		t.Fatalf("Failed to query original chunks: %v", err)
	}
	defer origRows.Close()

	restoredRows, err := restoredDB.Query("SELECT position, data FROM chunk ORDER BY position")
	if err != nil {
		t.Fatalf("Failed to query restored chunks: %v", err)
	}
	defer restoredRows.Close()

	for origRows.Next() {
		if !restoredRows.Next() {
			t.Fatal("Restored database has fewer chunks than original")
		}

		var origPos, restoredPos int64
		var origData, restoredData []byte

		if err := origRows.Scan(&origPos, &origData); err != nil {
			t.Fatalf("Failed to scan original row: %v", err)
		}
		if err := restoredRows.Scan(&restoredPos, &restoredData); err != nil {
			t.Fatalf("Failed to scan restored row: %v", err)
		}

		if origPos != restoredPos {
			t.Errorf("Position mismatch: original %d, restored %d", origPos, restoredPos)
		}
		if string(origData) != string(restoredData) {
			t.Errorf("Data mismatch for position %d", origPos)
		}
	}

	// Check playerdata UIDs are correctly restored (including special characters)
	var playerUID string
	if err := restoredDB.QueryRow("SELECT playeruid FROM playerdata WHERE data = ?", []byte("player1_data")).Scan(&playerUID); err != nil {
		t.Fatalf("Failed to query player: %v", err)
	}
	if playerUID != "B5fZ7vAsz3Kt+fmEV8GeK8Gu" {
		t.Errorf("Player UID = %q, want %q", playerUID, "B5fZ7vAsz3Kt+fmEV8GeK8Gu")
	}

	// Check player with / in UID
	if err := restoredDB.QueryRow("SELECT playeruid FROM playerdata WHERE data = ?", []byte("player2_data")).Scan(&playerUID); err != nil {
		t.Fatalf("Failed to query player: %v", err)
	}
	if playerUID != "ABC123/DEF456+xyz" {
		t.Errorf("Player UID = %q, want %q", playerUID, "ABC123/DEF456+xyz")
	}
}

func TestExtractChunkX(t *testing.T) {
	tests := []struct {
		position int64
		expected int32
	}{
		{0, 0},
		{42, 42},
		{0x0FFFFF, 0x0FFFFF},         // Max positive 21-bit value: 1048575 (bit 20 not set)
		{0x100000, -1048576},         // Sign bit set: most negative 21-bit value
		{0x1FFFFF, -1},               // All 21 bits set = -1 in signed representation
		{0x00000012abff341c, -52196}, // Real example: 0x1f341c has bit 20 set
		{0x0bff341c00005678, 22136},  // Another example: 0x5678 = 22136 (positive)
		{0x200000, 0},                // chunkX bits are 0, other bits set
	}

	for _, tc := range tests {
		result := extractChunkX(tc.position)
		if result != tc.expected {
			t.Errorf("extractChunkX(0x%x) = %d, want %d", tc.position, result, tc.expected)
		}
	}
}

func TestExtractChunkZ(t *testing.T) {
	tests := []struct {
		position int64
		expected int32
	}{
		{0, 0},
		{0x08000000, 1},                  // chunkZ = 1 (bit 27 set)
		{0x00000012abff341c, 597},        // Real example
		{0x0bff341c00005678, 426880},     // Another example
		{int64(0x0000FFFF80000000), -16}, // chunkZ with sign bit set
	}

	for _, tc := range tests {
		result := extractChunkZ(tc.position)
		if result != tc.expected {
			t.Errorf("extractChunkZ(0x%x) = %d, want %d", tc.position, result, tc.expected)
		}
	}
}

func TestSanitizePlayerUID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"B5fZ7vAsz3Kt+fmEV8GeK8Gu", "B5fZ7vAsz3Kt-fmEV8GeK8Gu"},
		{"ABC123/DEF456+xyz", "ABC123_DEF456-xyz"},
		{"SimplePlayer", "SimplePlayer"},
		{"a+b/c=", "a-b_c"},
		{"+++///===", "---___"},
	}

	for _, tc := range tests {
		result := sanitizePlayerUID(tc.input)
		if result != tc.expected {
			t.Errorf("sanitizePlayerUID(%q) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestUnsanitizePlayerUID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"B5fZ7vAsz3Kt-fmEV8GeK8Gu", "B5fZ7vAsz3Kt+fmEV8GeK8Gu"},
		{"ABC123_DEF456-xyz", "ABC123/DEF456+xyz"},
		{"SimplePlayer", "SimplePlayer"},
		{"a-b_c", "a+b/c"},
	}

	for _, tc := range tests {
		result := unsanitizePlayerUID(tc.input)
		if result != tc.expected {
			t.Errorf("unsanitizePlayerUID(%q) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestGetShardedPath(t *testing.T) {
	tests := []struct {
		baseDir     string
		tablePlural string
		position    int64
		expected    string
	}{
		// Position 0: chunkZ=0, chunkX=0
		{"/tmp/backup", "chunks", 0, "/tmp/backup/chunks/0/0/0000000000000000.bin"},
		// Position 0x00000012abff341c: chunkZ=597, chunkX=-52196
		{"/tmp/backup", "chunks", 0x00000012abff341c, "/tmp/backup/chunks/597/-52196/00000012abff341c.bin"},
		// Position 0x0bff341c00005678: chunkZ=426880, chunkX=22136
		{"/tmp/backup", "mapchunks", 0x0bff341c00005678, "/tmp/backup/mapchunks/426880/22136/0bff341c00005678.bin"},
		// Position 42: chunkZ=0, chunkX=42
		{"/data", "mapregions", 42, "/data/mapregions/0/42/000000000000002a.bin"},
	}

	for _, tc := range tests {
		result := GetShardedPath(tc.baseDir, tc.tablePlural, tc.position)
		if result != tc.expected {
			t.Errorf("GetShardedPath(%q, %q, %d) = %q, want %q",
				tc.baseDir, tc.tablePlural, tc.position, result, tc.expected)
		}
	}
}

func TestReconstructPositionFromPath(t *testing.T) {
	tests := []struct {
		path     string
		expected int64
	}{
		{"/tmp/chunks/0/0/0000000000000000.bin", 0},
		{"/tmp/chunks/37/2044956/00000012abff341c.bin", 0x00000012abff341c},
		{"/tmp/chunks/1048294/22136/0bff341c00005678.bin", 0x0bff341c00005678},
	}

	for _, tc := range tests {
		result, err := reconstructPositionFromPath(tc.path)
		if err != nil {
			t.Errorf("reconstructPositionFromPath(%q) error: %v", tc.path, err)
			continue
		}
		if result != tc.expected {
			t.Errorf("reconstructPositionFromPath(%q) = %d, want %d",
				tc.path, result, tc.expected)
		}
	}
}

func TestReconstructPositionFromPath_InvalidPaths(t *testing.T) {
	tests := []struct {
		path string
		desc string
	}{
		{"/tmp/chunks/0/0/0000000000000000.txt", "wrong extension"},
		{"/tmp/chunks/0/0/000000000000.bin", "short hex (12 digits instead of 16)"},
		{"/tmp/chunks/0/0/zzzzzzzzzzzzzzzz.bin", "non-hex filename"},
		{"/tmp/chunks/0/0/00000000000000000.bin", "too long hex (17 digits)"},
	}

	for _, tc := range tests {
		_, err := reconstructPositionFromPath(tc.path)
		if err == nil {
			t.Errorf("reconstructPositionFromPath(%q) expected error for %s",
				tc.path, tc.desc)
		}
	}
}

func TestSplit_HandlesMissingTables(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "minimal.vcdbs")
	outputDir := filepath.Join(tmpDir, "output")

	// Create database with only required tables but no data
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	schema := `
		CREATE TABLE chunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapchunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapregion (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE gamedata (savegameid integer PRIMARY KEY, data BLOB);
		CREATE TABLE playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		t.Fatalf("Failed to create schema: %v", err)
	}
	db.Close()

	// Split should succeed with empty tables
	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed on empty database: %v", err)
	}

	// Verify flat directories were created (gamedata and playerdata always get created)
	for _, dir := range []string{"gamedata", "playerdata"} {
		path := filepath.Join(outputDir, dir)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Expected directory %s to exist", dir)
		}
	}

	// Sharded directories may or may not exist (only created when there's data)
	// This is fine - empty tables don't need directories
}

func TestCombine_HandlesMissingDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	inputDir := filepath.Join(tmpDir, "incomplete")
	outputPath := filepath.Join(tmpDir, "output.vcdbs")

	// Create only gamedata directory with one file
	gamedataDir := filepath.Join(inputDir, "gamedata")
	if err := os.MkdirAll(gamedataDir, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gamedataDir, "1.bin"), []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Combine should succeed even with missing directories
	if err := Combine(inputDir, outputPath); err != nil {
		t.Fatalf("Combine() failed: %v", err)
	}

	// Verify the database has the gamedata
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM gamedata").Scan(&count); err != nil {
		t.Fatalf("Failed to count gamedata: %v", err)
	}
	if count != 1 {
		t.Errorf("Gamedata count = %d, want 1", count)
	}
}

func TestSplit_LargePositionValues(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	outputDir := filepath.Join(tmpDir, "output")

	// Create database with large position values (like real world data)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	schema := `
		CREATE TABLE chunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapchunk (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE mapregion (position integer PRIMARY KEY, data BLOB);
		CREATE TABLE gamedata (savegameid integer PRIMARY KEY, data BLOB);
		CREATE TABLE playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Insert large position values (like 2144262438527 from real data)
	largePositions := []int64{
		2144262438527,
		9223372036854775807, // Max int64
		1,
		0,
	}

	for _, pos := range largePositions {
		if _, err := db.Exec("INSERT INTO chunk (position, data) VALUES (?, ?)",
			pos, []byte("data")); err != nil {
			db.Close()
			t.Fatalf("Failed to insert chunk with position %d: %v", pos, err)
		}
	}
	db.Close()

	// Split
	if err := Split(dbPath, outputDir); err != nil {
		t.Fatalf("Split() failed: %v", err)
	}

	// Verify each position can be found and has correct path
	for _, pos := range largePositions {
		expectedPath := GetShardedPath(outputDir, "chunks", pos)
		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Errorf("Expected file at %s for position %d", expectedPath, pos)
		}
	}
}

// === SplitWithCache Tests ===

func TestSplitWithCache_FirstRun(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	cacheDir := filepath.Join(tmpDir, "cache")

	createTestDatabase(t, dbPath)

	written, skipped, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("SplitWithCache() failed: %v", err)
	}

	// On first run, all files should be written
	if written == 0 {
		t.Error("Expected some files to be written on first run")
	}
	if skipped != 0 {
		t.Errorf("Expected 0 skipped on first run, got %d", skipped)
	}

	// Verify directory structure exists
	expectedDirs := []string{"chunks", "mapchunks", "mapregions", "gamedata", "playerdata"}
	for _, dir := range expectedDirs {
		path := filepath.Join(cacheDir, dir)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("Expected directory %s to exist: %v", dir, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("Expected %s to be a directory", dir)
		}
	}
}

func TestSplitWithCache_SecondRunNoChanges(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	cacheDir := filepath.Join(tmpDir, "cache")

	createTestDatabase(t, dbPath)

	// First run
	written1, skipped1, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("First SplitWithCache() failed: %v", err)
	}
	totalFiles := written1 + skipped1

	// Get mtimes of all files
	mtimes := make(map[string]int64)
	filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			mtimes[path] = info.ModTime().UnixNano()
		}
		return nil
	})

	// Second run with same data
	written2, skipped2, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("Second SplitWithCache() failed: %v", err)
	}

	// All files should be skipped (unchanged)
	if written2 != 0 {
		t.Errorf("Expected 0 files written on second run, got %d", written2)
	}
	if skipped2 != totalFiles {
		t.Errorf("Expected %d files skipped on second run, got %d", totalFiles, skipped2)
	}

	// Verify mtimes are unchanged
	filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			if mtimes[path] != info.ModTime().UnixNano() {
				t.Errorf("File %s mtime changed when it shouldn't have", path)
			}
		}
		return nil
	})
}

func TestSplitWithCache_ChangedData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	cacheDir := filepath.Join(tmpDir, "cache")

	createTestDatabase(t, dbPath)

	// First run
	_, _, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("First SplitWithCache() failed: %v", err)
	}

	// Modify the database - update one chunk
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	_, err = db.Exec("UPDATE chunk SET data = ? WHERE position = 0", []byte("modified_chunk_zero"))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to update chunk: %v", err)
	}
	db.Close()

	// Second run
	written2, skipped2, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("Second SplitWithCache() failed: %v", err)
	}

	// Only one file should be written
	if written2 != 1 {
		t.Errorf("Expected 1 file written on second run, got %d", written2)
	}
	if skipped2 == 0 {
		t.Error("Expected some files to be skipped on second run")
	}

	// Verify the updated content
	filePath := GetShardedPath(cacheDir, "chunks", 0)
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read updated chunk: %v", err)
	}
	if string(data) != "modified_chunk_zero" {
		t.Errorf("Chunk data = %q, want %q", string(data), "modified_chunk_zero")
	}
}

func TestSplitWithCache_DeletedChunks(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	cacheDir := filepath.Join(tmpDir, "cache")

	createTestDatabase(t, dbPath)

	// First run
	_, _, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("First SplitWithCache() failed: %v", err)
	}

	// Get the path of the chunk at position 0
	chunkPath := GetShardedPath(cacheDir, "chunks", 0)

	// Verify it exists
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		t.Fatalf("Expected chunk file to exist at %s", chunkPath)
	}

	// Delete the chunk from database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	_, err = db.Exec("DELETE FROM chunk WHERE position = 0")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to delete chunk: %v", err)
	}
	db.Close()

	// Second run
	_, _, err = SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("Second SplitWithCache() failed: %v", err)
	}

	// Verify the chunk file was removed
	if _, err := os.Stat(chunkPath); !os.IsNotExist(err) {
		t.Errorf("Expected chunk file to be deleted at %s", chunkPath)
	}
}

func TestSplitWithCache_NewChunks(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.vcdbs")
	cacheDir := filepath.Join(tmpDir, "cache")

	createTestDatabase(t, dbPath)

	// First run
	written1, _, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("First SplitWithCache() failed: %v", err)
	}

	// Add a new chunk to the database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	newPosition := int64(9999999)
	_, err = db.Exec("INSERT INTO chunk (position, data) VALUES (?, ?)", newPosition, []byte("new_chunk"))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to insert new chunk: %v", err)
	}
	db.Close()

	// Second run
	written2, skipped2, err := SplitWithCache(dbPath, cacheDir)
	if err != nil {
		t.Fatalf("Second SplitWithCache() failed: %v", err)
	}

	// One new file should be written
	if written2 != 1 {
		t.Errorf("Expected 1 file written on second run, got %d", written2)
	}

	// Previous files should be skipped
	if skipped2 != written1 {
		t.Errorf("Expected %d files skipped on second run, got %d", written1, skipped2)
	}

	// Verify the new chunk exists
	newChunkPath := GetShardedPath(cacheDir, "chunks", newPosition)
	data, err := os.ReadFile(newChunkPath)
	if err != nil {
		t.Fatalf("Failed to read new chunk: %v", err)
	}
	if string(data) != "new_chunk" {
		t.Errorf("New chunk data = %q, want %q", string(data), "new_chunk")
	}
}

func TestFileMatchesContent(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.bin")

	testData := []byte("test content")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	t.Run("matching content", func(t *testing.T) {
		if !fileMatchesContent(filePath, testData) {
			t.Error("Expected fileMatchesContent to return true for matching content")
		}
	})

	t.Run("different content", func(t *testing.T) {
		if fileMatchesContent(filePath, []byte("different")) {
			t.Error("Expected fileMatchesContent to return false for different content")
		}
	})

	t.Run("different size", func(t *testing.T) {
		if fileMatchesContent(filePath, []byte("longer content here")) {
			t.Error("Expected fileMatchesContent to return false for different size")
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		if fileMatchesContent(filepath.Join(tmpDir, "nonexistent"), testData) {
			t.Error("Expected fileMatchesContent to return false for non-existent file")
		}
	})
}

func TestCopyFileIfChanged(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "src.bin")
	dstPath := filepath.Join(tmpDir, "dst.bin")

	srcData := []byte("source content")
	if err := os.WriteFile(srcPath, srcData, 0644); err != nil {
		t.Fatalf("Failed to write source file: %v", err)
	}

	t.Run("destination doesn't exist", func(t *testing.T) {
		written, err := CopyFileIfChanged(srcPath, dstPath)
		if err != nil {
			t.Fatalf("CopyFileIfChanged failed: %v", err)
		}
		if !written {
			t.Error("Expected file to be written")
		}

		dstData, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatalf("Failed to read destination: %v", err)
		}
		if string(dstData) != string(srcData) {
			t.Errorf("Destination content = %q, want %q", string(dstData), string(srcData))
		}
	})

	t.Run("destination matches", func(t *testing.T) {
		written, err := CopyFileIfChanged(srcPath, dstPath)
		if err != nil {
			t.Fatalf("CopyFileIfChanged failed: %v", err)
		}
		if written {
			t.Error("Expected file to be skipped (unchanged)")
		}
	})

	t.Run("destination differs", func(t *testing.T) {
		// Modify destination
		if err := os.WriteFile(dstPath, []byte("different"), 0644); err != nil {
			t.Fatalf("Failed to modify destination: %v", err)
		}

		written, err := CopyFileIfChanged(srcPath, dstPath)
		if err != nil {
			t.Fatalf("CopyFileIfChanged failed: %v", err)
		}
		if !written {
			t.Error("Expected file to be written")
		}

		dstData, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatalf("Failed to read destination: %v", err)
		}
		if string(dstData) != string(srcData) {
			t.Errorf("Destination content = %q, want %q", string(dstData), string(srcData))
		}
	})
}

func TestSyncDir(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	dstDir := filepath.Join(tmpDir, "dst")

	// Create source directory structure
	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0755)
	os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0644)
	os.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("content2"), 0644)

	t.Run("initial sync", func(t *testing.T) {
		written, skipped, removed, err := SyncDir(srcDir, dstDir)
		if err != nil {
			t.Fatalf("SyncDir failed: %v", err)
		}
		if written != 2 {
			t.Errorf("Expected 2 files written, got %d", written)
		}
		if skipped != 0 {
			t.Errorf("Expected 0 files skipped, got %d", skipped)
		}
		if removed != 0 {
			t.Errorf("Expected 0 files removed, got %d", removed)
		}
	})

	t.Run("sync unchanged", func(t *testing.T) {
		written, skipped, removed, err := SyncDir(srcDir, dstDir)
		if err != nil {
			t.Fatalf("SyncDir failed: %v", err)
		}
		if written != 0 {
			t.Errorf("Expected 0 files written, got %d", written)
		}
		if skipped != 2 {
			t.Errorf("Expected 2 files skipped, got %d", skipped)
		}
		if removed != 0 {
			t.Errorf("Expected 0 files removed, got %d", removed)
		}
	})

	t.Run("sync with removed file", func(t *testing.T) {
		// Remove a file from source
		os.Remove(filepath.Join(srcDir, "file1.txt"))

		written, skipped, removed, err := SyncDir(srcDir, dstDir)
		if err != nil {
			t.Fatalf("SyncDir failed: %v", err)
		}
		if written != 0 {
			t.Errorf("Expected 0 files written, got %d", written)
		}
		if skipped != 1 {
			t.Errorf("Expected 1 file skipped, got %d", skipped)
		}
		if removed != 1 {
			t.Errorf("Expected 1 file removed, got %d", removed)
		}

		// Verify file was removed from destination
		if _, err := os.Stat(filepath.Join(dstDir, "file1.txt")); !os.IsNotExist(err) {
			t.Error("Expected file1.txt to be removed from destination")
		}
	})
}

func TestSyncFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "src.txt")
	dstPath := filepath.Join(tmpDir, "dst.txt")

	t.Run("source doesn't exist, destination doesn't exist", func(t *testing.T) {
		written, removed, err := SyncFile(srcPath, dstPath)
		if err != nil {
			t.Fatalf("SyncFile failed: %v", err)
		}
		if written != 0 || removed != 0 {
			t.Errorf("Expected (0, 0), got (%d, %d)", written, removed)
		}
	})

	t.Run("source exists, destination doesn't", func(t *testing.T) {
		os.WriteFile(srcPath, []byte("content"), 0644)

		written, removed, err := SyncFile(srcPath, dstPath)
		if err != nil {
			t.Fatalf("SyncFile failed: %v", err)
		}
		if written != 1 || removed != 0 {
			t.Errorf("Expected (1, 0), got (%d, %d)", written, removed)
		}
	})

	t.Run("source exists, destination matches", func(t *testing.T) {
		written, removed, err := SyncFile(srcPath, dstPath)
		if err != nil {
			t.Fatalf("SyncFile failed: %v", err)
		}
		if written != 0 || removed != 0 {
			t.Errorf("Expected (0, 0), got (%d, %d)", written, removed)
		}
	})

	t.Run("source removed, destination exists", func(t *testing.T) {
		os.Remove(srcPath)

		written, removed, err := SyncFile(srcPath, dstPath)
		if err != nil {
			t.Fatalf("SyncFile failed: %v", err)
		}
		if written != 0 || removed != 1 {
			t.Errorf("Expected (0, 1), got (%d, %d)", written, removed)
		}

		// Verify destination was removed
		if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
			t.Error("Expected destination to be removed")
		}
	})
}

func TestCopyDirIfChanged(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	dstDir := filepath.Join(tmpDir, "dst")

	// Create source directory structure
	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0755)
	os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0644)
	os.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("content2"), 0644)

	t.Run("initial copy", func(t *testing.T) {
		written, skipped, err := CopyDirIfChanged(srcDir, dstDir)
		if err != nil {
			t.Fatalf("CopyDirIfChanged failed: %v", err)
		}
		if written != 2 {
			t.Errorf("Expected 2 files written, got %d", written)
		}
		if skipped != 0 {
			t.Errorf("Expected 0 files skipped, got %d", skipped)
		}
	})

	t.Run("copy unchanged", func(t *testing.T) {
		written, skipped, err := CopyDirIfChanged(srcDir, dstDir)
		if err != nil {
			t.Fatalf("CopyDirIfChanged failed: %v", err)
		}
		if written != 0 {
			t.Errorf("Expected 0 files written, got %d", written)
		}
		if skipped != 2 {
			t.Errorf("Expected 2 files skipped, got %d", skipped)
		}
	})

	t.Run("copy with changed file", func(t *testing.T) {
		// Modify a file in source
		os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("modified"), 0644)

		written, skipped, err := CopyDirIfChanged(srcDir, dstDir)
		if err != nil {
			t.Fatalf("CopyDirIfChanged failed: %v", err)
		}
		if written != 1 {
			t.Errorf("Expected 1 file written, got %d", written)
		}
		if skipped != 1 {
			t.Errorf("Expected 1 file skipped, got %d", skipped)
		}
	})
}
