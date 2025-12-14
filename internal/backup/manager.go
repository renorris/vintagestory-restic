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
	"time"

	"github.com/renorris/vintagestory-restic/internal/server"
)

// ServerCommander is an interface for sending commands to the server.
// This allows for testing without a real server, and for using a rate-limited
// command queue that wraps the server.
type ServerCommander interface {
	SendCommand(cmd string) error
}

// BootChecker is an interface for checking if the server has fully booted.
// This allows the backup manager to wait until the server is ready before
// attempting backups.
type BootChecker interface {
	HasBooted() bool
}

// ErrServerNotBooted is returned when a backup is attempted before the server has fully booted.
var ErrServerNotBooted = fmt.Errorf("server has not fully booted yet")

// ResticRunner is a function type for running restic backups.
// This allows for testing without actually running restic.
type ResticRunner func(ctx context.Context, stagingDir string) error

// CommandRunner is a function type for running shell commands.
// This allows for testing without actually running commands.
// Returns the exit code and any error.
type CommandRunner func(ctx context.Context, name string, args ...string) (exitCode int, err error)

// SQLiteVacuumRunner is a function type for running sqlite3 VACUUM INTO.
// This allows for testing without actually running sqlite3.
// srcPath is the source database file, dstPath is the destination.
type SQLiteVacuumRunner func(ctx context.Context, srcPath, dstPath string) error

// PlayerCheckerInterface is an interface for checking if players are online.
// This allows for testing without a real player checker.
type PlayerCheckerInterface interface {
	// ShouldBackup returns true if a backup should run based on player status.
	// It returns true if players are currently online, OR if players were
	// online at the last check but are now all offline (final backup).
	ShouldBackup() bool
}

// ErrNoPlayersOnline is returned when a backup is skipped because no players are online.
var ErrNoPlayersOnline = fmt.Errorf("no players online, backup skipped")

// Manager handles periodic backups of the Vintage Story server.
type Manager struct {
	// Interval is the time between backups.
	Interval time.Duration

	// GameDataDir is the path to the game data directory (e.g., /gamedata).
	GameDataDir string

	// StagingDir is the path where the staging directory will be created.
	// If empty, defaults to /tmp/vs-backup-staging.
	StagingDir string

	// Server is the Vintage Story server to send backup commands to.
	Server ServerCommander

	// BootChecker is used to check if the server has fully booted.
	// If set, backups will only run after the server has booted.
	// If nil, the boot check is skipped.
	BootChecker BootChecker

	// PlayerChecker is used to check if players are online.
	// If set and PauseWhenNoPlayers is true, backups will only run when players are online.
	PlayerChecker PlayerCheckerInterface

	// PauseWhenNoPlayers indicates whether backups should be skipped when no players are online.
	PauseWhenNoPlayers bool

	// OnBackupStart is called when a backup starts. Optional.
	OnBackupStart func()

	// OnBackupComplete is called when a backup completes. Optional.
	// The error parameter is nil on success.
	OnBackupComplete func(err error, duration time.Duration)

	// BackupTimeout is the maximum time to wait for a backup file to appear.
	// Defaults to 5 minutes if not set.
	BackupTimeout time.Duration

	// ResticRunner is a custom function to run restic backup.
	// If nil, the default restic backup command is used.
	// This is primarily for testing.
	ResticRunner ResticRunner

	// CommandRunner is a custom function to run shell commands.
	// If nil, the default exec.Command is used.
	// This is primarily for testing.
	CommandRunner CommandRunner

	// SQLiteVacuumRunner is a custom function to run sqlite3 VACUUM INTO.
	// If nil, the default sqlite3 command is used.
	// This is primarily for testing.
	SQLiteVacuumRunner SQLiteVacuumRunner

	done   chan struct{}
	wg     sync.WaitGroup
	cancel context.CancelFunc
	mu     sync.Mutex
}

// serverConfig represents the structure of serverconfig.json for extracting save file location.
type serverConfig struct {
	WorldConfig struct {
		SaveFileLocation string `json:"SaveFileLocation"`
	} `json:"WorldConfig"`
}

// Start begins the periodic backup loop.
// The context controls the lifecycle - when cancelled, the manager will stop.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done != nil {
		return fmt.Errorf("backup manager already started")
	}

	if m.Interval <= 0 {
		return fmt.Errorf("backup interval must be positive")
	}

	if m.Server == nil {
		return fmt.Errorf("server is required")
	}

	if m.GameDataDir == "" {
		m.GameDataDir = "/gamedata"
	}

	if m.StagingDir == "" {
		m.StagingDir = "/tmp/vs-backup-staging"
	}

	if m.BackupTimeout <= 0 {
		m.BackupTimeout = 5 * time.Minute
	}

	ctx, m.cancel = context.WithCancel(ctx)
	m.done = make(chan struct{})

	m.wg.Add(1)
	go m.runLoop(ctx)

	return nil
}

// Stop stops the backup manager and waits for it to finish.
func (m *Manager) Stop() {
	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
}

// Done returns a channel that is closed when the manager stops.
func (m *Manager) Done() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return m.done
}

// runLoop is the main backup loop.
func (m *Manager) runLoop(ctx context.Context) {
	defer m.wg.Done()
	defer close(m.done)

	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runBackup(ctx)
		}
	}
}

// runBackup performs a single backup operation.
func (m *Manager) runBackup(ctx context.Context) {
	startTime := time.Now()

	if m.OnBackupStart != nil {
		m.OnBackupStart()
	}

	err := m.performBackup(ctx)

	if m.OnBackupComplete != nil {
		m.OnBackupComplete(err, time.Since(startTime))
	}
}

// performBackup executes the full backup workflow.
func (m *Manager) performBackup(ctx context.Context) error {
	// Step 0a: Check if server has booted (if BootChecker is configured)
	if m.BootChecker != nil && !m.BootChecker.HasBooted() {
		return ErrServerNotBooted
	}

	// Step 0b: Check if backup should run based on player status
	// ShouldBackup() returns true if players are online, OR if players
	// were online previously but have now all logged off (final backup).
	if m.PauseWhenNoPlayers && m.PlayerChecker != nil {
		if !m.PlayerChecker.ShouldBackup() {
			return ErrNoPlayersOnline
		}
	}

	// Step 1: Get the save file name from serverconfig.json
	saveFileName, err := m.getSaveFileName()
	if err != nil {
		return fmt.Errorf("failed to get save file name: %w", err)
	}

	// Step 2: Record the current time before sending genbackup
	beforeGenbackup := time.Now()

	// Step 3: Send /genbackup command to the server
	if err := m.Server.SendCommand("/genbackup"); err != nil {
		return fmt.Errorf("failed to send genbackup command: %w", err)
	}

	// Step 4: Wait for new backup file to appear
	backupCtx, cancel := context.WithTimeout(ctx, m.BackupTimeout)
	defer cancel()

	backupFile, err := m.waitForBackupFile(backupCtx, beforeGenbackup)
	if err != nil {
		return fmt.Errorf("failed to wait for backup file: %w", err)
	}

	// Step 5: Create staging directory with CoW copies
	if err := m.createStagingDirectory(backupFile, saveFileName); err != nil {
		return fmt.Errorf("failed to create staging directory: %w", err)
	}
	defer m.cleanupStaging()

	// Step 6: Run restic backup on the staging directory
	if err := m.runRestic(ctx); err != nil {
		return fmt.Errorf("failed to run restic backup: %w", err)
	}

	// Note: The backup file was moved to the staging directory in step 5,
	// so it will be automatically cleaned up when the staging directory is removed.

	return nil
}

// getSaveFileName reads serverconfig.json and extracts the save file name.
func (m *Manager) getSaveFileName() (string, error) {
	configPath := filepath.Join(m.GameDataDir, "serverconfig.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("failed to read serverconfig.json: %w", err)
	}

	var config serverConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("failed to parse serverconfig.json: %w", err)
	}

	saveLocation := config.WorldConfig.SaveFileLocation
	if saveLocation == "" {
		return "default.vcdbs", nil // fallback
	}

	// Extract just the filename from the path
	return filepath.Base(saveLocation), nil
}

// waitForBackupFile waits for a new .vcdbs file to appear in the Backups directory.
// It also waits until the file is no longer locked for writing before returning.
func (m *Manager) waitForBackupFile(ctx context.Context, afterTime time.Time) (string, error) {
	backupsDir := filepath.Join(m.GameDataDir, "Backups")

	// Ensure the backups directory exists
	if err := os.MkdirAll(backupsDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create backups directory: %w", err)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			entries, err := os.ReadDir(backupsDir)
			if err != nil {
				continue // Directory might not exist yet
			}

			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}

				if !strings.HasSuffix(entry.Name(), ".vcdbs") {
					continue
				}

				info, err := entry.Info()
				if err != nil {
					continue
				}

				// Check if the file was created after we sent /genbackup
				if info.ModTime().After(afterTime) {
					filePath := filepath.Join(backupsDir, entry.Name())

					// Wait until the file is ready (no write locks held by other processes)
					if m.isFileUnlocked(filePath) {
						return filePath, nil
					}
					// File exists but is still being written to, continue waiting
				}
			}
		}
	}
}

// isFileUnlocked checks if a file can be safely read by verifying no write locks are held on it.
// Returns true if the file can be exclusively locked (meaning no other process has it locked).
func (m *Manager) isFileUnlocked(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	// Try to acquire an exclusive lock non-blocking.
	// If successful, no other process holds a lock on this file.
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return false // File is locked by another process
	}

	// Release the lock immediately
	syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return true
}

// createStagingDirectory creates the staging directory with copy-on-write copies.
func (m *Manager) createStagingDirectory(backupFile, saveFileName string) error {
	// Clean up any existing staging directory
	if err := os.RemoveAll(m.StagingDir); err != nil {
		return fmt.Errorf("failed to remove existing staging directory: %w", err)
	}

	// Create the staging directory
	if err := os.MkdirAll(m.StagingDir, 0755); err != nil {
		return fmt.Errorf("failed to create staging directory: %w", err)
	}

	// Copy directories using CoW: Logs, Playerdata, Mods
	dirsToLink := []string{"Logs", "Playerdata", "Mods"}
	for _, dir := range dirsToLink {
		srcDir := filepath.Join(m.GameDataDir, dir)
		dstDir := filepath.Join(m.StagingDir, dir)

		if _, err := os.Stat(srcDir); err == nil {
			if err := copyDirCoW(srcDir, dstDir); err != nil {
				return fmt.Errorf("failed to copy %s: %w", dir, err)
			}
		}
	}

	// Copy config files
	configFiles := []string{"serverconfig.json", "servermagicnumbers.json"}
	for _, file := range configFiles {
		srcFile := filepath.Join(m.GameDataDir, file)
		dstFile := filepath.Join(m.StagingDir, file)

		if _, err := os.Stat(srcFile); err == nil {
			if err := copyFileCoW(srcFile, dstFile); err != nil {
				return fmt.Errorf("failed to copy %s: %w", file, err)
			}
		}
	}

	// Create the Saves directory
	savesDir := filepath.Join(m.StagingDir, "Saves")
	if err := os.MkdirAll(savesDir, 0755); err != nil {
		return fmt.Errorf("failed to create Saves directory: %w", err)
	}

	// VACUUM the backup file into the staging directory.
	// This produces a canonical, deterministic SQLite database that maximizes
	// restic's deduplication efficiency by ensuring unchanged data produces
	// identical byte sequences.
	dstSaveFile := filepath.Join(savesDir, saveFileName)
	if err := m.vacuumDatabase(context.Background(), backupFile, dstSaveFile); err != nil {
		return fmt.Errorf("failed to vacuum backup file: %w", err)
	}

	// Remove the original backup file since we've vacuumed it to the destination
	if err := os.Remove(backupFile); err != nil {
		return fmt.Errorf("failed to remove original backup file: %w", err)
	}

	return nil
}

// copyDirCoW recursively copies a directory using copy-on-write when available.
func copyDirCoW(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Calculate the destination path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		return copyFileCoW(path, dstPath)
	})
}

// copyFileCoW copies a file using copy-on-write (reflink) when available.
// Falls back to regular copy if CoW is not supported.
func copyFileCoW(src, dst string) error {
	// Ensure the destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	// Try using cp --reflink=auto for CoW copy
	// This works on filesystems that support it (btrfs, xfs, zfs, etc.)
	cmd := exec.Command("cp", "--reflink=auto", src, dst)
	if err := cmd.Run(); err != nil {
		// Fall back to regular copy
		return copyFileRegular(src, dst)
	}

	return nil
}

// copyFileRegular performs a regular file copy.
func copyFileRegular(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return err
	}
	defer dstFile.Close()

	buf := make([]byte, 1024*1024) // 1MB buffer
	for {
		n, readErr := srcFile.Read(buf)
		if n > 0 {
			if _, writeErr := dstFile.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
		}
		if readErr != nil {
			if readErr.Error() == "EOF" {
				break
			}
			return readErr
		}
	}

	return nil
}

// vacuumDatabase runs sqlite3 VACUUM INTO to create a canonical, deterministic
// copy of the SQLite database at dstPath. This maximizes restic's deduplication
// efficiency by ensuring unchanged data produces identical byte sequences.
func (m *Manager) vacuumDatabase(ctx context.Context, srcPath, dstPath string) error {
	// Use custom runner if provided (for testing)
	if m.SQLiteVacuumRunner != nil {
		return m.SQLiteVacuumRunner(ctx, srcPath, dstPath)
	}

	// Build the VACUUM INTO command
	// sqlite3 src.db "VACUUM INTO 'dst.db'"
	vacuumCmd := fmt.Sprintf("VACUUM INTO '%s'", dstPath)
	cmd := exec.CommandContext(ctx, "sqlite3", srcPath, vacuumCmd)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sqlite3 VACUUM INTO failed: %w\nOutput: %s", err, string(output))
	}

	return nil
}

// cleanupStaging removes the staging directory.
func (m *Manager) cleanupStaging() {
	os.RemoveAll(m.StagingDir)
}

// runRestic runs restic backup on the staging directory.
func (m *Manager) runRestic(ctx context.Context) error {
	// Use custom runner if provided (for testing)
	if m.ResticRunner != nil {
		return m.ResticRunner(ctx, m.StagingDir)
	}

	// Check that required environment variables are set
	if os.Getenv("RESTIC_REPOSITORY") == "" {
		return fmt.Errorf("RESTIC_REPOSITORY environment variable is not set")
	}

	// Ensure the repository is initialized before running backup
	if err := m.ensureRepoInitialized(ctx); err != nil {
		return fmt.Errorf("failed to initialize restic repository: %w", err)
	}

	// Run restic backup
	cmd := exec.CommandContext(ctx, "restic", "backup", m.StagingDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("restic backup failed: %w", err)
	}

	return nil
}

// ensureRepoInitialized checks if the restic repository is initialized and initializes it if not.
// Uses "restic cat config" to check - exit code 10 means uninitialized (since restic 0.17.0).
func (m *Manager) ensureRepoInitialized(ctx context.Context) error {
	exitCode, output, err := m.runCommandWithOutput(ctx, "restic", "cat", "config")

	// Exit code 0 means repository is already initialized
	if exitCode == 0 {
		return nil
	}

	// Exit code 10 means repository is not initialized (restic 0.17.0+)
	if exitCode == 10 {
		initExitCode, _, initErr := m.runCommandWithOutput(ctx, "restic", "init")
		if initErr != nil {
			return fmt.Errorf("restic init failed: %v", initErr)
		}
		if initExitCode != 0 {
			return fmt.Errorf("restic init failed with exit code %d", initExitCode)
		}
		return nil
	}

	// Any other exit code is an error (e.g., wrong password, network error)
	if err != nil {
		return fmt.Errorf("restic cat config failed (exit code %d): %v\nOutput: %s", exitCode, err, output)
	}
	return fmt.Errorf("restic cat config failed with exit code %d\nOutput: %s", exitCode, output)
}

// runCommandWithOutput runs a command and returns its exit code and combined output.
func (m *Manager) runCommandWithOutput(ctx context.Context, name string, args ...string) (int, string, error) {
	// Use custom runner if provided (for testing)
	if m.CommandRunner != nil {
		exitCode, err := m.CommandRunner(ctx, name, args...)
		return exitCode, "", err
	}

	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(output), nil
	}

	// Extract exit code from error
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode(), string(output), nil
	}

	return -1, string(output), err
}

// RunBackupNow triggers an immediate backup. This is useful for testing.
func (m *Manager) RunBackupNow(ctx context.Context) error {
	return m.performBackup(ctx)
}

// Ensure Server implements ServerCommander at compile time.
var _ ServerCommander = (*server.Server)(nil)

// Ensure Server implements BootChecker at compile time.
var _ BootChecker = (*server.Server)(nil)
