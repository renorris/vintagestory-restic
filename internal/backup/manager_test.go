package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// mockServer implements ServerCommander for testing.
type mockServer struct {
	mu        sync.Mutex
	commands  []string
	onCommand func(cmd string) error
}

// mockBootChecker implements BootChecker for testing.
type mockBootChecker struct {
	mu        sync.Mutex
	hasBooted bool
}

func (m *mockBootChecker) HasBooted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasBooted
}

func (m *mockBootChecker) SetBooted(booted bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hasBooted = booted
}

func (m *mockServer) SendCommand(cmd string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = append(m.commands, cmd)
	if m.onCommand != nil {
		return m.onCommand(cmd)
	}
	return nil
}

func (m *mockServer) getCommands() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.commands...)
}

func TestManager_Start_Validation(t *testing.T) {
	t.Run("missing server", func(t *testing.T) {
		m := &Manager{
			Interval: time.Second,
		}
		ctx := context.Background()
		err := m.Start(ctx)
		if err == nil {
			t.Error("Start() expected error for missing server")
		}
	})

	t.Run("zero interval", func(t *testing.T) {
		m := &Manager{
			Interval: 0,
			Server:   &mockServer{},
		}
		ctx := context.Background()
		err := m.Start(ctx)
		if err == nil {
			t.Error("Start() expected error for zero interval")
		}
	})

	t.Run("negative interval", func(t *testing.T) {
		m := &Manager{
			Interval: -time.Second,
			Server:   &mockServer{},
		}
		ctx := context.Background()
		err := m.Start(ctx)
		if err == nil {
			t.Error("Start() expected error for negative interval")
		}
	})

	t.Run("already started", func(t *testing.T) {
		m := &Manager{
			Interval:    time.Hour, // Long interval so it doesn't trigger
			Server:      &mockServer{},
			GameDataDir: t.TempDir(),
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := m.Start(ctx); err != nil {
			t.Fatalf("First Start() failed: %v", err)
		}
		defer m.Stop()

		if err := m.Start(ctx); err == nil {
			t.Error("Second Start() expected error")
		}
	})
}

func TestManager_StartStop(t *testing.T) {
	m := &Manager{
		Interval:    time.Hour, // Long interval so it doesn't trigger
		Server:      &mockServer{},
		GameDataDir: t.TempDir(),
	}

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Stop should work
	m.Stop()

	// Done channel should be closed
	select {
	case <-m.Done():
		// Good
	default:
		t.Error("Done() channel should be closed after Stop()")
	}
}

func TestManager_ContextCancellation(t *testing.T) {
	m := &Manager{
		Interval:    time.Hour,
		Server:      &mockServer{},
		GameDataDir: t.TempDir(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Cancel context
	cancel()

	// Manager should stop
	select {
	case <-m.Done():
		// Good
	case <-time.After(5 * time.Second):
		t.Error("Manager did not stop after context cancellation")
	}
}

func TestManager_GetSaveFileName(t *testing.T) {
	tmpDir := t.TempDir()

	// Create serverconfig.json with a custom save file location
	config := map[string]interface{}{
		"WorldConfig": map[string]interface{}{
			"SaveFileLocation": "/gamedata/Saves/myworld.vcdbs",
		},
	}
	configData, _ := json.Marshal(config)
	if err := os.WriteFile(filepath.Join(tmpDir, "serverconfig.json"), configData, 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	m := &Manager{
		Interval:    time.Second,
		Server:      &mockServer{},
		GameDataDir: tmpDir,
	}

	saveFileName, err := m.getSaveFileName()
	if err != nil {
		t.Fatalf("getSaveFileName() failed: %v", err)
	}

	if saveFileName != "myworld.vcdbs" {
		t.Errorf("getSaveFileName() = %q, want %q", saveFileName, "myworld.vcdbs")
	}
}

func TestManager_GetSaveFileName_Default(t *testing.T) {
	tmpDir := t.TempDir()

	// Create serverconfig.json without SaveFileLocation
	config := map[string]interface{}{
		"WorldConfig": map[string]interface{}{},
	}
	configData, _ := json.Marshal(config)
	if err := os.WriteFile(filepath.Join(tmpDir, "serverconfig.json"), configData, 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	m := &Manager{
		Interval:    time.Second,
		Server:      &mockServer{},
		GameDataDir: tmpDir,
	}

	saveFileName, err := m.getSaveFileName()
	if err != nil {
		t.Fatalf("getSaveFileName() failed: %v", err)
	}

	if saveFileName != "default.vcdbs" {
		t.Errorf("getSaveFileName() = %q, want %q", saveFileName, "default.vcdbs")
	}
}

func TestManager_WaitForBackupFile(t *testing.T) {
	tmpDir := t.TempDir()
	backupsDir := filepath.Join(tmpDir, "Backups")
	if err := os.MkdirAll(backupsDir, 0755); err != nil {
		t.Fatalf("Failed to create Backups dir: %v", err)
	}

	m := &Manager{
		Interval:      time.Second,
		Server:        &mockServer{},
		GameDataDir:   tmpDir,
		BackupTimeout: 5 * time.Second,
	}

	// Record time before creating the file
	beforeCreate := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Create a backup file
	backupFileName := "2024-01-01_12-00-00.vcdbs"
	backupFilePath := filepath.Join(backupsDir, backupFileName)
	if err := os.WriteFile(backupFilePath, []byte("test backup data"), 0644); err != nil {
		t.Fatalf("Failed to write backup file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	foundFile, err := m.waitForBackupFile(ctx, beforeCreate)
	if err != nil {
		t.Fatalf("waitForBackupFile() failed: %v", err)
	}

	if foundFile != backupFilePath {
		t.Errorf("waitForBackupFile() = %q, want %q", foundFile, backupFilePath)
	}
}

func TestManager_WaitForBackupFile_Timeout(t *testing.T) {
	tmpDir := t.TempDir()
	backupsDir := filepath.Join(tmpDir, "Backups")
	if err := os.MkdirAll(backupsDir, 0755); err != nil {
		t.Fatalf("Failed to create Backups dir: %v", err)
	}

	m := &Manager{
		Interval:      time.Second,
		Server:        &mockServer{},
		GameDataDir:   tmpDir,
		BackupTimeout: time.Second,
	}

	// Use a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := m.waitForBackupFile(ctx, time.Now())
	if err != context.DeadlineExceeded {
		t.Errorf("waitForBackupFile() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestManager_WaitForBackupFile_IgnoresOldFiles(t *testing.T) {
	tmpDir := t.TempDir()
	backupsDir := filepath.Join(tmpDir, "Backups")
	if err := os.MkdirAll(backupsDir, 0755); err != nil {
		t.Fatalf("Failed to create Backups dir: %v", err)
	}

	// Create an old backup file first
	oldBackupPath := filepath.Join(backupsDir, "old-backup.vcdbs")
	if err := os.WriteFile(oldBackupPath, []byte("old backup"), 0644); err != nil {
		t.Fatalf("Failed to write old backup file: %v", err)
	}

	// Wait a moment to ensure time difference
	time.Sleep(50 * time.Millisecond)
	afterOldFile := time.Now()
	time.Sleep(50 * time.Millisecond)

	m := &Manager{
		Interval:      time.Second,
		Server:        &mockServer{},
		GameDataDir:   tmpDir,
		BackupTimeout: 5 * time.Second,
	}

	// Create a new backup file in a goroutine
	newBackupPath := filepath.Join(backupsDir, "new-backup.vcdbs")
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.WriteFile(newBackupPath, []byte("new backup"), 0644)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	foundFile, err := m.waitForBackupFile(ctx, afterOldFile)
	if err != nil {
		t.Fatalf("waitForBackupFile() failed: %v", err)
	}

	if foundFile != newBackupPath {
		t.Errorf("waitForBackupFile() = %q, want %q (should ignore old file)", foundFile, newBackupPath)
	}
}

func TestManager_CreateStagingDirectory(t *testing.T) {
	// Create game data directory with test content
	gameDataDir := t.TempDir()
	stagingDir := t.TempDir()

	// Create test directories
	logsDir := filepath.Join(gameDataDir, "Logs")
	playerDataDir := filepath.Join(gameDataDir, "Playerdata")
	modsDir := filepath.Join(gameDataDir, "Mods")
	backupsDir := filepath.Join(gameDataDir, "Backups")

	for _, dir := range []string{logsDir, playerDataDir, modsDir, backupsDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create dir %s: %v", dir, err)
		}
	}

	// Create test files
	if err := os.WriteFile(filepath.Join(logsDir, "server.log"), []byte("log data"), 0644); err != nil {
		t.Fatalf("Failed to write log file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(playerDataDir, "player1.json"), []byte("player data"), 0644); err != nil {
		t.Fatalf("Failed to write player file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modsDir, "mod1.zip"), []byte("mod data"), 0644); err != nil {
		t.Fatalf("Failed to write mod file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gameDataDir, "servermagicnumbers.json"), []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to write magic numbers: %v", err)
	}

	// Create a backup file
	backupFile := filepath.Join(backupsDir, "backup.vcdbs")
	if err := os.WriteFile(backupFile, []byte("backup data"), 0644); err != nil {
		t.Fatalf("Failed to write backup file: %v", err)
	}

	m := &Manager{
		Interval:    time.Second,
		Server:      &mockServer{},
		GameDataDir: gameDataDir,
		StagingDir:  stagingDir,
		// Mock VACUUM to just copy the file (simulates what VACUUM INTO does)
		SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
			return copyFileRegular(srcPath, dstPath)
		},
	}

	// Create staging directory
	if err := m.createStagingDirectory(backupFile, "default.vcdbs"); err != nil {
		t.Fatalf("createStagingDirectory() failed: %v", err)
	}

	// Verify directories exist in staging
	for _, dir := range []string{"Logs", "Playerdata", "Mods", "Saves"} {
		path := filepath.Join(stagingDir, dir)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Expected directory %s to exist in staging", dir)
		}
	}

	// Verify config files exist
	for _, file := range []string{"serverconfig.json", "servermagicnumbers.json"} {
		path := filepath.Join(stagingDir, file)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Expected file %s to exist in staging", file)
		}
	}

	// Verify the save file exists with correct name
	saveFile := filepath.Join(stagingDir, "Saves", "default.vcdbs")
	if _, err := os.Stat(saveFile); os.IsNotExist(err) {
		t.Error("Expected save file to exist in staging")
	}

	// Verify content was vacuumed (copied) to staging
	content, err := os.ReadFile(saveFile)
	if err != nil {
		t.Fatalf("Failed to read save file: %v", err)
	}
	if string(content) != "backup data" {
		t.Errorf("Save file content = %q, want %q", string(content), "backup data")
	}

	// Verify the original backup file was removed after vacuum
	if _, err := os.Stat(backupFile); !os.IsNotExist(err) {
		t.Error("Expected original backup file to be removed after vacuum")
	}
}

func TestManager_SendsGenbackupCommand(t *testing.T) {
	gameDataDir := t.TempDir()
	stagingDir := t.TempDir()
	backupsDir := filepath.Join(gameDataDir, "Backups")
	os.MkdirAll(backupsDir, 0755)

	// Create serverconfig.json
	config := map[string]interface{}{
		"WorldConfig": map[string]interface{}{
			"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
		},
	}
	configData, _ := json.Marshal(config)
	os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

	server := &mockServer{}

	m := &Manager{
		Interval:      time.Second,
		Server:        server,
		GameDataDir:   gameDataDir,
		StagingDir:    stagingDir,
		BackupTimeout: 2 * time.Second,
	}

	// Create a backup file that will be found
	go func() {
		time.Sleep(100 * time.Millisecond)
		backupFile := filepath.Join(backupsDir, "backup.vcdbs")
		os.WriteFile(backupFile, []byte("backup data"), 0644)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Disable restic since we don't have it installed in tests
	os.Unsetenv("RESTIC_REPOSITORY")

	// performBackup will fail at the restic step, but we can verify the command was sent
	_ = m.performBackup(ctx)

	commands := server.getCommands()
	found := false
	for _, cmd := range commands {
		if cmd == "/genbackup" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected /genbackup command to be sent to server")
	}
}

func TestManager_Done_BeforeStart(t *testing.T) {
	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
	}

	// Done should return a closed channel before Start is called
	select {
	case <-m.Done():
		// Good - channel is closed
	default:
		t.Error("Done() should return closed channel before Start()")
	}
}

func TestManager_Callbacks(t *testing.T) {
	gameDataDir := t.TempDir()
	stagingDir := t.TempDir()
	backupsDir := filepath.Join(gameDataDir, "Backups")
	os.MkdirAll(backupsDir, 0755)

	// Create serverconfig.json
	config := map[string]interface{}{
		"WorldConfig": map[string]interface{}{
			"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
		},
	}
	configData, _ := json.Marshal(config)
	os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

	var startCalled, completeCalled bool
	var completeDuration time.Duration
	var completeErr error
	var mu sync.Mutex

	m := &Manager{
		Interval:      time.Second,
		Server:        &mockServer{},
		GameDataDir:   gameDataDir,
		StagingDir:    stagingDir,
		BackupTimeout: 2 * time.Second,
		OnBackupStart: func() {
			mu.Lock()
			startCalled = true
			mu.Unlock()
		},
		OnBackupComplete: func(err error, duration time.Duration) {
			mu.Lock()
			completeCalled = true
			completeErr = err
			completeDuration = duration
			mu.Unlock()
		},
	}

	// Create a backup file that will be found
	go func() {
		time.Sleep(100 * time.Millisecond)
		backupFile := filepath.Join(backupsDir, "backup.vcdbs")
		os.WriteFile(backupFile, []byte("backup data"), 0644)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Disable restic - will fail at restic step
	os.Unsetenv("RESTIC_REPOSITORY")

	m.runBackup(ctx)

	mu.Lock()
	defer mu.Unlock()

	if !startCalled {
		t.Error("OnBackupStart callback was not called")
	}

	if !completeCalled {
		t.Error("OnBackupComplete callback was not called")
	}

	if completeDuration <= 0 {
		t.Error("OnBackupComplete duration should be positive")
	}

	// Should have failed because RESTIC_REPOSITORY is not set
	if completeErr == nil {
		t.Error("Expected backup to fail without RESTIC_REPOSITORY")
	}
}

func TestCopyFileRegular(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")
	dstFile := filepath.Join(tmpDir, "dest.txt")

	content := "test content for copy"
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write source file: %v", err)
	}

	if err := copyFileRegular(srcFile, dstFile); err != nil {
		t.Fatalf("copyFileRegular() failed: %v", err)
	}

	read, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("Failed to read dest file: %v", err)
	}

	if string(read) != content {
		t.Errorf("copyFileRegular() content = %q, want %q", string(read), content)
	}
}

func TestCopyDirCoW(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dest")

	// Create source structure
	subDir := filepath.Join(srcDir, "subdir")
	os.MkdirAll(subDir, 0755)

	os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0644)
	os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("content2"), 0644)

	if err := copyDirCoW(srcDir, dstDir); err != nil {
		t.Fatalf("copyDirCoW() failed: %v", err)
	}

	// Verify structure
	if _, err := os.Stat(filepath.Join(dstDir, "file1.txt")); os.IsNotExist(err) {
		t.Error("Expected file1.txt to exist")
	}
	if _, err := os.Stat(filepath.Join(dstDir, "subdir", "file2.txt")); os.IsNotExist(err) {
		t.Error("Expected subdir/file2.txt to exist")
	}

	// Verify content
	content, _ := os.ReadFile(filepath.Join(dstDir, "subdir", "file2.txt"))
	if string(content) != "content2" {
		t.Errorf("File content = %q, want %q", string(content), "content2")
	}
}

func TestManager_IsFileUnlocked_UnlockedFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "unlocked.txt")

	// Create an unlocked file
	if err := os.WriteFile(filePath, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
	}

	if !m.isFileUnlocked(filePath) {
		t.Error("isFileUnlocked() = false, want true for unlocked file")
	}
}

func TestManager_IsFileUnlocked_LockedFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "locked.txt")

	// Create a file
	if err := os.WriteFile(filePath, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Lock the file
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Failed to lock file: %v", err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
	}

	if m.isFileUnlocked(filePath) {
		t.Error("isFileUnlocked() = true, want false for locked file")
	}
}

func TestManager_IsFileUnlocked_NonExistentFile(t *testing.T) {
	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
	}

	if m.isFileUnlocked("/nonexistent/path/file.txt") {
		t.Error("isFileUnlocked() = true, want false for non-existent file")
	}
}

func TestManager_WaitForBackupFile_WaitsForUnlock(t *testing.T) {
	tmpDir := t.TempDir()
	backupsDir := filepath.Join(tmpDir, "Backups")
	if err := os.MkdirAll(backupsDir, 0755); err != nil {
		t.Fatalf("Failed to create Backups dir: %v", err)
	}

	m := &Manager{
		Interval:      time.Second,
		Server:        &mockServer{},
		GameDataDir:   tmpDir,
		BackupTimeout: 5 * time.Second,
	}

	// Record time before creating the file
	beforeCreate := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Create a backup file and lock it
	backupFileName := "2024-01-01_12-00-00.vcdbs"
	backupFilePath := filepath.Join(backupsDir, backupFileName)
	if err := os.WriteFile(backupFilePath, []byte("test backup data"), 0644); err != nil {
		t.Fatalf("Failed to write backup file: %v", err)
	}

	// Lock the file
	lockedFile, err := os.Open(backupFilePath)
	if err != nil {
		t.Fatalf("Failed to open file for locking: %v", err)
	}

	if err := syscall.Flock(int(lockedFile.Fd()), syscall.LOCK_EX); err != nil {
		lockedFile.Close()
		t.Fatalf("Failed to lock file: %v", err)
	}

	// Unlock the file after a short delay in a goroutine
	go func() {
		time.Sleep(600 * time.Millisecond)
		syscall.Flock(int(lockedFile.Fd()), syscall.LOCK_UN)
		lockedFile.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	startTime := time.Now()
	foundFile, err := m.waitForBackupFile(ctx, beforeCreate)
	elapsed := time.Since(startTime)

	if err != nil {
		t.Fatalf("waitForBackupFile() failed: %v", err)
	}

	if foundFile != backupFilePath {
		t.Errorf("waitForBackupFile() = %q, want %q", foundFile, backupFilePath)
	}

	// Verify that we actually waited for the lock to be released
	// The file was locked for 600ms, so we should have waited at least 500ms
	if elapsed < 500*time.Millisecond {
		t.Errorf("waitForBackupFile() returned too quickly (elapsed=%v), expected to wait for lock", elapsed)
	}
}

func TestManager_PerformBackup_CleansUpBackupFile(t *testing.T) {
	gameDataDir := t.TempDir()
	stagingDir := t.TempDir()
	backupsDir := filepath.Join(gameDataDir, "Backups")
	os.MkdirAll(backupsDir, 0755)

	// Create serverconfig.json
	config := map[string]interface{}{
		"WorldConfig": map[string]interface{}{
			"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
		},
	}
	configData, _ := json.Marshal(config)
	os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

	m := &Manager{
		Interval:      time.Second,
		Server:        &mockServer{},
		GameDataDir:   gameDataDir,
		StagingDir:    stagingDir,
		BackupTimeout: 2 * time.Second,
		// Mock restic to succeed
		ResticRunner: func(ctx context.Context, stagingDir string) error {
			return nil
		},
		// Mock VACUUM to copy the file
		SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
			return copyFileRegular(srcPath, dstPath)
		},
	}

	// Create a backup file that will be found
	backupFile := filepath.Join(backupsDir, "backup.vcdbs")
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.WriteFile(backupFile, []byte("backup data"), 0644)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.performBackup(ctx)
	if err != nil {
		t.Fatalf("performBackup() failed: %v", err)
	}

	// Verify the backup file was deleted
	if _, err := os.Stat(backupFile); !os.IsNotExist(err) {
		t.Error("Expected backup file to be deleted after successful backup")
	}

	// Verify the staging directory was cleaned up
	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Error("Expected staging directory to be cleaned up after backup")
	}
}

func TestManager_PerformBackup_CleansUpStagingOnResticFailure(t *testing.T) {
	gameDataDir := t.TempDir()
	stagingDir := t.TempDir()
	backupsDir := filepath.Join(gameDataDir, "Backups")
	os.MkdirAll(backupsDir, 0755)

	// Create serverconfig.json
	config := map[string]interface{}{
		"WorldConfig": map[string]interface{}{
			"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
		},
	}
	configData, _ := json.Marshal(config)
	os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

	m := &Manager{
		Interval:      time.Second,
		Server:        &mockServer{},
		GameDataDir:   gameDataDir,
		StagingDir:    stagingDir,
		BackupTimeout: 2 * time.Second,
		// Mock restic to fail
		ResticRunner: func(ctx context.Context, stagingDir string) error {
			return fmt.Errorf("simulated restic failure")
		},
		// Mock VACUUM to copy the file
		SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
			return copyFileRegular(srcPath, dstPath)
		},
	}

	// Create a backup file that will be found
	backupFile := filepath.Join(backupsDir, "backup.vcdbs")
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.WriteFile(backupFile, []byte("backup data"), 0644)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.performBackup(ctx)
	if err == nil {
		t.Fatal("performBackup() expected to fail when restic fails")
	}

	// The backup file was moved to staging and then staging was cleaned up,
	// so the file should no longer exist at the original location.
	// This is a trade-off: we use move instead of copy to avoid copying
	// potentially very large save files.
	if _, err := os.Stat(backupFile); !os.IsNotExist(err) {
		t.Error("Backup file should not exist at original location (was moved to staging)")
	}

	// Staging directory should be cleaned up via defer
	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Error("Expected staging directory to be cleaned up even on failure")
	}
}

func TestManager_PerformBackup_BootCheckGuard(t *testing.T) {
	t.Run("backup fails when server not booted", func(t *testing.T) {
		gameDataDir := t.TempDir()

		bootChecker := &mockBootChecker{hasBooted: false}

		m := &Manager{
			Interval:    time.Second,
			Server:      &mockServer{},
			BootChecker: bootChecker,
			GameDataDir: gameDataDir,
		}

		ctx := context.Background()
		err := m.performBackup(ctx)

		if err != ErrServerNotBooted {
			t.Errorf("performBackup() error = %v, want ErrServerNotBooted", err)
		}
	})

	t.Run("backup proceeds when server has booted", func(t *testing.T) {
		gameDataDir := t.TempDir()
		stagingDir := t.TempDir()
		backupsDir := filepath.Join(gameDataDir, "Backups")
		os.MkdirAll(backupsDir, 0755)

		// Create serverconfig.json
		config := map[string]interface{}{
			"WorldConfig": map[string]interface{}{
				"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
			},
		}
		configData, _ := json.Marshal(config)
		os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

		bootChecker := &mockBootChecker{hasBooted: true}

		m := &Manager{
			Interval:      time.Second,
			Server:        &mockServer{},
			BootChecker:   bootChecker,
			GameDataDir:   gameDataDir,
			StagingDir:    stagingDir,
			BackupTimeout: 2 * time.Second,
			ResticRunner: func(ctx context.Context, stagingDir string) error {
				return nil
			},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				return copyFileRegular(srcPath, dstPath)
			},
		}

		// Create a backup file that will be found
		go func() {
			time.Sleep(100 * time.Millisecond)
			backupFile := filepath.Join(backupsDir, "backup.vcdbs")
			os.WriteFile(backupFile, []byte("backup data"), 0644)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := m.performBackup(ctx)
		if err != nil {
			t.Errorf("performBackup() unexpected error: %v", err)
		}
	})

	t.Run("backup proceeds when no boot checker configured", func(t *testing.T) {
		gameDataDir := t.TempDir()
		stagingDir := t.TempDir()
		backupsDir := filepath.Join(gameDataDir, "Backups")
		os.MkdirAll(backupsDir, 0755)

		// Create serverconfig.json
		config := map[string]interface{}{
			"WorldConfig": map[string]interface{}{
				"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
			},
		}
		configData, _ := json.Marshal(config)
		os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

		// No BootChecker set
		m := &Manager{
			Interval:      time.Second,
			Server:        &mockServer{},
			GameDataDir:   gameDataDir,
			StagingDir:    stagingDir,
			BackupTimeout: 2 * time.Second,
			ResticRunner: func(ctx context.Context, stagingDir string) error {
				return nil
			},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				return copyFileRegular(srcPath, dstPath)
			},
		}

		// Create a backup file that will be found
		go func() {
			time.Sleep(100 * time.Millisecond)
			backupFile := filepath.Join(backupsDir, "backup.vcdbs")
			os.WriteFile(backupFile, []byte("backup data"), 0644)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := m.performBackup(ctx)
		if err != nil {
			t.Errorf("performBackup() unexpected error: %v", err)
		}
	})
}

// mockPlayerChecker implements PlayerCheckerInterface for testing.
type mockPlayerChecker struct {
	mu           sync.Mutex
	shouldBackup bool
}

func (m *mockPlayerChecker) ShouldBackup() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.shouldBackup
}

func (m *mockPlayerChecker) SetShouldBackup(shouldBackup bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldBackup = shouldBackup
}

func TestManager_PerformBackup_PlayerCheckGuard(t *testing.T) {
	t.Run("backup skips when no players online and PauseWhenNoPlayers enabled", func(t *testing.T) {
		gameDataDir := t.TempDir()

		playerChecker := &mockPlayerChecker{shouldBackup: false}
		bootChecker := &mockBootChecker{hasBooted: true}

		m := &Manager{
			Interval:           time.Second,
			Server:             &mockServer{},
			BootChecker:        bootChecker,
			PlayerChecker:      playerChecker,
			PauseWhenNoPlayers: true,
			GameDataDir:        gameDataDir,
		}

		ctx := context.Background()
		err := m.performBackup(ctx)

		if err != ErrNoPlayersOnline {
			t.Errorf("performBackup() error = %v, want ErrNoPlayersOnline", err)
		}
	})

	t.Run("backup proceeds when players are online", func(t *testing.T) {
		gameDataDir := t.TempDir()
		stagingDir := t.TempDir()
		backupsDir := filepath.Join(gameDataDir, "Backups")
		os.MkdirAll(backupsDir, 0755)

		// Create serverconfig.json
		config := map[string]interface{}{
			"WorldConfig": map[string]interface{}{
				"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
			},
		}
		configData, _ := json.Marshal(config)
		os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

		playerChecker := &mockPlayerChecker{shouldBackup: true}
		bootChecker := &mockBootChecker{hasBooted: true}

		m := &Manager{
			Interval:           time.Second,
			Server:             &mockServer{},
			BootChecker:        bootChecker,
			PlayerChecker:      playerChecker,
			PauseWhenNoPlayers: true,
			GameDataDir:        gameDataDir,
			StagingDir:         stagingDir,
			BackupTimeout:      2 * time.Second,
			ResticRunner: func(ctx context.Context, stagingDir string) error {
				return nil
			},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				return copyFileRegular(srcPath, dstPath)
			},
		}

		// Create a backup file that will be found
		go func() {
			time.Sleep(100 * time.Millisecond)
			backupFile := filepath.Join(backupsDir, "backup.vcdbs")
			os.WriteFile(backupFile, []byte("backup data"), 0644)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := m.performBackup(ctx)
		if err != nil {
			t.Errorf("performBackup() unexpected error: %v", err)
		}
	})

	t.Run("backup proceeds when PauseWhenNoPlayers is false", func(t *testing.T) {
		gameDataDir := t.TempDir()
		stagingDir := t.TempDir()
		backupsDir := filepath.Join(gameDataDir, "Backups")
		os.MkdirAll(backupsDir, 0755)

		// Create serverconfig.json
		config := map[string]interface{}{
			"WorldConfig": map[string]interface{}{
				"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
			},
		}
		configData, _ := json.Marshal(config)
		os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

		// Player checker says no backup needed, but PauseWhenNoPlayers is false
		playerChecker := &mockPlayerChecker{shouldBackup: false}
		bootChecker := &mockBootChecker{hasBooted: true}

		m := &Manager{
			Interval:           time.Second,
			Server:             &mockServer{},
			BootChecker:        bootChecker,
			PlayerChecker:      playerChecker,
			PauseWhenNoPlayers: false, // Disabled
			GameDataDir:        gameDataDir,
			StagingDir:         stagingDir,
			BackupTimeout:      2 * time.Second,
			ResticRunner: func(ctx context.Context, stagingDir string) error {
				return nil
			},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				return copyFileRegular(srcPath, dstPath)
			},
		}

		// Create a backup file that will be found
		go func() {
			time.Sleep(100 * time.Millisecond)
			backupFile := filepath.Join(backupsDir, "backup.vcdbs")
			os.WriteFile(backupFile, []byte("backup data"), 0644)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := m.performBackup(ctx)
		if err != nil {
			t.Errorf("performBackup() unexpected error: %v", err)
		}
	})

	t.Run("backup proceeds when no PlayerChecker configured", func(t *testing.T) {
		gameDataDir := t.TempDir()
		stagingDir := t.TempDir()
		backupsDir := filepath.Join(gameDataDir, "Backups")
		os.MkdirAll(backupsDir, 0755)

		// Create serverconfig.json
		config := map[string]interface{}{
			"WorldConfig": map[string]interface{}{
				"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
			},
		}
		configData, _ := json.Marshal(config)
		os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

		bootChecker := &mockBootChecker{hasBooted: true}

		m := &Manager{
			Interval:           time.Second,
			Server:             &mockServer{},
			BootChecker:        bootChecker,
			PlayerChecker:      nil, // No player checker
			PauseWhenNoPlayers: true,
			GameDataDir:        gameDataDir,
			StagingDir:         stagingDir,
			BackupTimeout:      2 * time.Second,
			ResticRunner: func(ctx context.Context, stagingDir string) error {
				return nil
			},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				return copyFileRegular(srcPath, dstPath)
			},
		}

		// Create a backup file that will be found
		go func() {
			time.Sleep(100 * time.Millisecond)
			backupFile := filepath.Join(backupsDir, "backup.vcdbs")
			os.WriteFile(backupFile, []byte("backup data"), 0644)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := m.performBackup(ctx)
		if err != nil {
			t.Errorf("performBackup() unexpected error: %v", err)
		}
	})

	t.Run("multiple backups succeed while ShouldBackup returns true", func(t *testing.T) {
		gameDataDir := t.TempDir()
		stagingDir := t.TempDir()
		backupsDir := filepath.Join(gameDataDir, "Backups")
		os.MkdirAll(backupsDir, 0755)

		// Create serverconfig.json
		config := map[string]interface{}{
			"WorldConfig": map[string]interface{}{
				"SaveFileLocation": "/gamedata/Saves/test.vcdbs",
			},
		}
		configData, _ := json.Marshal(config)
		os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), configData, 0644)

		playerChecker := &mockPlayerChecker{shouldBackup: true}
		bootChecker := &mockBootChecker{hasBooted: true}

		m := &Manager{
			Interval:           time.Second,
			Server:             &mockServer{},
			BootChecker:        bootChecker,
			PlayerChecker:      playerChecker,
			PauseWhenNoPlayers: true,
			GameDataDir:        gameDataDir,
			StagingDir:         stagingDir,
			BackupTimeout:      2 * time.Second,
			ResticRunner: func(ctx context.Context, stagingDir string) error {
				return nil
			},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				return copyFileRegular(srcPath, dstPath)
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// First backup should succeed
		go func() {
			time.Sleep(100 * time.Millisecond)
			backupFile := filepath.Join(backupsDir, "backup1.vcdbs")
			os.WriteFile(backupFile, []byte("backup data"), 0644)
		}()
		err := m.performBackup(ctx)
		if err != nil {
			t.Errorf("First performBackup() unexpected error: %v", err)
		}

		// Second backup should also succeed
		go func() {
			time.Sleep(100 * time.Millisecond)
			backupFile := filepath.Join(backupsDir, "backup2.vcdbs")
			os.WriteFile(backupFile, []byte("backup data"), 0644)
		}()
		err = m.performBackup(ctx)
		if err != nil {
			t.Errorf("Second performBackup() unexpected error: %v", err)
		}

		// Now set ShouldBackup to false and try again
		playerChecker.SetShouldBackup(false)
		err = m.performBackup(ctx)
		if err != ErrNoPlayersOnline {
			t.Errorf("Third performBackup() error = %v, want ErrNoPlayersOnline", err)
		}
	})
}

func TestManager_EnsureRepoInitialized_AlreadyInitialized(t *testing.T) {
	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
		CommandRunner: func(ctx context.Context, name string, args ...string) (int, error) {
			// Simulate "restic cat config" succeeding (repo already initialized)
			if name == "restic" && len(args) >= 2 && args[0] == "cat" && args[1] == "config" {
				return 0, nil
			}
			return 1, fmt.Errorf("unexpected command")
		},
	}

	ctx := context.Background()
	err := m.ensureRepoInitialized(ctx)
	if err != nil {
		t.Errorf("ensureRepoInitialized() unexpected error: %v", err)
	}
}

func TestManager_EnsureRepoInitialized_NeedsInit(t *testing.T) {
	var initCalled bool

	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
		CommandRunner: func(ctx context.Context, name string, args ...string) (int, error) {
			if name == "restic" {
				if len(args) >= 2 && args[0] == "cat" && args[1] == "config" {
					// Exit code 10 = repository not initialized
					return 10, nil
				}
				if len(args) >= 1 && args[0] == "init" {
					initCalled = true
					return 0, nil
				}
			}
			return 1, fmt.Errorf("unexpected command: %s %v", name, args)
		},
	}

	ctx := context.Background()
	err := m.ensureRepoInitialized(ctx)
	if err != nil {
		t.Errorf("ensureRepoInitialized() unexpected error: %v", err)
	}

	if !initCalled {
		t.Error("expected restic init to be called")
	}
}

func TestManager_EnsureRepoInitialized_InitFails(t *testing.T) {
	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
		CommandRunner: func(ctx context.Context, name string, args ...string) (int, error) {
			if name == "restic" {
				if len(args) >= 2 && args[0] == "cat" && args[1] == "config" {
					// Exit code 10 = repository not initialized
					return 10, nil
				}
				if len(args) >= 1 && args[0] == "init" {
					// Init fails
					return 1, nil
				}
			}
			return 1, fmt.Errorf("unexpected command")
		},
	}

	ctx := context.Background()
	err := m.ensureRepoInitialized(ctx)
	if err == nil {
		t.Error("expected error when restic init fails")
	}
}

func TestManager_EnsureRepoInitialized_OtherExitCodeIsError(t *testing.T) {
	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
		CommandRunner: func(ctx context.Context, name string, args ...string) (int, error) {
			if name == "restic" && len(args) >= 2 && args[0] == "cat" && args[1] == "config" {
				// Exit code 1 = error (wrong password, etc.)
				return 1, nil
			}
			return 1, fmt.Errorf("unexpected command")
		},
	}

	ctx := context.Background()
	err := m.ensureRepoInitialized(ctx)
	if err == nil {
		t.Error("expected error when restic cat config fails with non-10 exit code")
	}
}

func TestManager_RunCommandWithOutput(t *testing.T) {
	m := &Manager{
		Interval: time.Second,
		Server:   &mockServer{},
	}

	ctx := context.Background()

	// Test successful command
	exitCode, _, err := m.runCommandWithOutput(ctx, "true")
	if err != nil {
		t.Errorf("runCommandWithOutput('true') unexpected error: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("runCommandWithOutput('true') exit code = %d, want 0", exitCode)
	}

	// Test failing command
	exitCode, _, err = m.runCommandWithOutput(ctx, "false")
	if err != nil {
		t.Errorf("runCommandWithOutput('false') unexpected error: %v", err)
	}
	if exitCode != 1 {
		t.Errorf("runCommandWithOutput('false') exit code = %d, want 1", exitCode)
	}

	// Test command with output
	exitCode, output, err := m.runCommandWithOutput(ctx, "echo", "hello")
	if err != nil {
		t.Errorf("runCommandWithOutput('echo hello') unexpected error: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("runCommandWithOutput('echo hello') exit code = %d, want 0", exitCode)
	}
	if strings.TrimSpace(output) != "hello" {
		t.Errorf("runCommandWithOutput('echo hello') output = %q, want %q", output, "hello")
	}
}

func TestManager_VacuumDatabase(t *testing.T) {
	t.Run("successful vacuum with real sqlite3", func(t *testing.T) {
		tmpDir := t.TempDir()
		srcPath := filepath.Join(tmpDir, "source.db")
		dstPath := filepath.Join(tmpDir, "dest.db")

		// Create a simple SQLite database
		// We'll use the sqlite3 command to create it
		cmd := exec.Command("sqlite3", srcPath, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO test VALUES (1, 'hello');")
		if err := cmd.Run(); err != nil {
			t.Skipf("sqlite3 not available: %v", err)
		}

		m := &Manager{
			Interval: time.Second,
			Server:   &mockServer{},
		}

		ctx := context.Background()
		err := m.vacuumDatabase(ctx, srcPath, dstPath)
		if err != nil {
			t.Fatalf("vacuumDatabase() failed: %v", err)
		}

		// Verify the destination file exists
		if _, err := os.Stat(dstPath); os.IsNotExist(err) {
			t.Error("destination file should exist after vacuum")
		}

		// Verify the destination file is a valid SQLite database with our data
		output, err := exec.Command("sqlite3", dstPath, "SELECT name FROM test WHERE id=1;").Output()
		if err != nil {
			t.Fatalf("failed to query vacuumed database: %v", err)
		}
		if strings.TrimSpace(string(output)) != "hello" {
			t.Errorf("vacuumed database content = %q, want %q", strings.TrimSpace(string(output)), "hello")
		}
	})

	t.Run("uses custom SQLiteVacuumRunner when provided", func(t *testing.T) {
		var runnerCalled bool
		var capturedSrc, capturedDst string

		m := &Manager{
			Interval: time.Second,
			Server:   &mockServer{},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				runnerCalled = true
				capturedSrc = srcPath
				capturedDst = dstPath
				return nil
			},
		}

		ctx := context.Background()
		err := m.vacuumDatabase(ctx, "/src/path.db", "/dst/path.db")
		if err != nil {
			t.Fatalf("vacuumDatabase() failed: %v", err)
		}

		if !runnerCalled {
			t.Error("custom SQLiteVacuumRunner should have been called")
		}
		if capturedSrc != "/src/path.db" {
			t.Errorf("srcPath = %q, want %q", capturedSrc, "/src/path.db")
		}
		if capturedDst != "/dst/path.db" {
			t.Errorf("dstPath = %q, want %q", capturedDst, "/dst/path.db")
		}
	})

	t.Run("returns error from custom SQLiteVacuumRunner", func(t *testing.T) {
		expectedErr := fmt.Errorf("simulated vacuum failure")

		m := &Manager{
			Interval: time.Second,
			Server:   &mockServer{},
			SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
				return expectedErr
			},
		}

		ctx := context.Background()
		err := m.vacuumDatabase(ctx, "/src/path.db", "/dst/path.db")
		if err != expectedErr {
			t.Errorf("vacuumDatabase() error = %v, want %v", err, expectedErr)
		}
	})

	t.Run("fails with invalid destination path", func(t *testing.T) {
		tmpDir := t.TempDir()
		srcPath := filepath.Join(tmpDir, "source.db")

		// Create a simple SQLite database
		cmd := exec.Command("sqlite3", srcPath, "CREATE TABLE test (id INTEGER);")
		if err := cmd.Run(); err != nil {
			t.Skipf("sqlite3 not available: %v", err)
		}

		// Destination in a non-existent directory should fail
		dstPath := filepath.Join(tmpDir, "nonexistent", "subdir", "dest.db")

		m := &Manager{
			Interval: time.Second,
			Server:   &mockServer{},
		}

		ctx := context.Background()
		err := m.vacuumDatabase(ctx, srcPath, dstPath)
		if err == nil {
			t.Error("vacuumDatabase() expected error for invalid destination path")
		}
	})
}

func TestManager_CreateStagingDirectory_VacuumsBackupFile(t *testing.T) {
	// Create game data directory with test content
	gameDataDir := t.TempDir()
	stagingDir := t.TempDir()

	// Create test directories
	backupsDir := filepath.Join(gameDataDir, "Backups")
	if err := os.MkdirAll(backupsDir, 0755); err != nil {
		t.Fatalf("Failed to create Backups dir: %v", err)
	}

	// Create test config
	if err := os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	// Create a backup file
	backupFile := filepath.Join(backupsDir, "backup.vcdbs")
	if err := os.WriteFile(backupFile, []byte("backup data"), 0644); err != nil {
		t.Fatalf("Failed to write backup file: %v", err)
	}

	var vacuumCalled bool
	var vacuumSrc, vacuumDst string

	m := &Manager{
		Interval:    time.Second,
		Server:      &mockServer{},
		GameDataDir: gameDataDir,
		StagingDir:  stagingDir,
		SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
			vacuumCalled = true
			vacuumSrc = srcPath
			vacuumDst = dstPath
			// Simulate VACUUM by copying the file
			return copyFileRegular(srcPath, dstPath)
		},
	}

	// Create staging directory
	if err := m.createStagingDirectory(backupFile, "default.vcdbs"); err != nil {
		t.Fatalf("createStagingDirectory() failed: %v", err)
	}

	// Verify vacuum was called
	if !vacuumCalled {
		t.Error("SQLiteVacuumRunner should have been called")
	}

	// Verify vacuum was called with correct paths
	if vacuumSrc != backupFile {
		t.Errorf("vacuum srcPath = %q, want %q", vacuumSrc, backupFile)
	}
	expectedDst := filepath.Join(stagingDir, "Saves", "default.vcdbs")
	if vacuumDst != expectedDst {
		t.Errorf("vacuum dstPath = %q, want %q", vacuumDst, expectedDst)
	}

	// Verify the vacuumed file exists in staging
	if _, err := os.Stat(expectedDst); os.IsNotExist(err) {
		t.Error("Expected vacuumed save file to exist in staging")
	}

	// Verify the original backup file was removed
	if _, err := os.Stat(backupFile); !os.IsNotExist(err) {
		t.Error("Expected original backup file to be removed after vacuum")
	}
}

func TestManager_CreateStagingDirectory_VacuumFailure(t *testing.T) {
	gameDataDir := t.TempDir()
	stagingDir := t.TempDir()
	backupsDir := filepath.Join(gameDataDir, "Backups")
	os.MkdirAll(backupsDir, 0755)

	// Create test config
	os.WriteFile(filepath.Join(gameDataDir, "serverconfig.json"), []byte("{}"), 0644)

	// Create a backup file
	backupFile := filepath.Join(backupsDir, "backup.vcdbs")
	os.WriteFile(backupFile, []byte("backup data"), 0644)

	m := &Manager{
		Interval:    time.Second,
		Server:      &mockServer{},
		GameDataDir: gameDataDir,
		StagingDir:  stagingDir,
		SQLiteVacuumRunner: func(ctx context.Context, srcPath, dstPath string) error {
			return fmt.Errorf("simulated vacuum failure")
		},
	}

	err := m.createStagingDirectory(backupFile, "default.vcdbs")
	if err == nil {
		t.Error("createStagingDirectory() expected error when vacuum fails")
	}

	if !strings.Contains(err.Error(), "vacuum") {
		t.Errorf("error should mention vacuum: %v", err)
	}

	// The original backup file should still exist since vacuum failed
	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		t.Error("Original backup file should still exist when vacuum fails")
	}
}

func TestManager_RunBackup_BootCheckGuard(t *testing.T) {
	t.Run("runBackup skips when server not booted", func(t *testing.T) {
		gameDataDir := t.TempDir()

		bootChecker := &mockBootChecker{hasBooted: false}

		var completeCalled bool
		var completeErr error
		var mu sync.Mutex

		m := &Manager{
			Interval:    time.Second,
			Server:      &mockServer{},
			BootChecker: bootChecker,
			GameDataDir: gameDataDir,
			OnBackupComplete: func(err error, duration time.Duration) {
				mu.Lock()
				completeCalled = true
				completeErr = err
				mu.Unlock()
			},
		}

		ctx := context.Background()
		m.runBackup(ctx)

		mu.Lock()
		defer mu.Unlock()

		if !completeCalled {
			t.Error("OnBackupComplete should have been called")
		}

		if completeErr != ErrServerNotBooted {
			t.Errorf("OnBackupComplete error = %v, want ErrServerNotBooted", completeErr)
		}
	})
}
