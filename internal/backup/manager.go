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
	"github.com/renorris/vintagestory-restic/internal/vcdbtree"
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

// PruneRunner is a function type for running restic forget --prune.
// This allows for testing without actually running restic.
type PruneRunner func(ctx context.Context, retentionOptions string) error

// CommandRunner is a function type for running shell commands.
// This allows for testing without actually running commands.
// Returns the exit code and any error.
type CommandRunner func(ctx context.Context, name string, args ...string) (exitCode int, err error)

// VCDBTreeSplitter is a function type for splitting a .vcdbs file into vcdbtree format.
// This allows for testing without actually running the split operation.
// srcPath is the source .vcdbs file, dstDir is the destination directory.
// Returns the number of files written (changed) and skipped (unchanged).
type VCDBTreeSplitter func(srcPath, dstDir string) (written, skipped int, err error)

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

// BackupCompletionWaiter is an interface for waiting for the server to signal backup completion.
// The server sends "[Server Notification] Backup complete!" when the backup is finished.
type BackupCompletionWaiter interface {
	// WaitForBackupComplete waits for the server to send the backup completion message.
	// Returns the matching line, or an error if the context expires or the server exits.
	WaitForBackupComplete(ctx context.Context) error
}

// BackupCompletePattern is the exact suffix that indicates a backup has completed.
const BackupCompletePattern = "[Server Notification] Backup complete!"

// Manager handles periodic backups of the Vintage Story server.
type Manager struct {
	// Interval is the time between backups.
	Interval time.Duration

	// GameDataDir is the path to the game data directory (e.g., /gamedata).
	GameDataDir string

	// StagingDir is the path to the persistent staging directory.
	// This directory persists between backups to optimize for Restic efficiency.
	// If empty, defaults to /backupcache/staging.
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

	// BackupCompletionWaiter is used to wait for the server to signal backup completion.
	// If set, the manager will wait for the "[Server Notification] Backup complete!"
	// message before attempting to split the backup file into vcdbtree format.
	BackupCompletionWaiter BackupCompletionWaiter

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

	// PruneRunner is a custom function to run restic forget --prune.
	// If nil, the default restic forget command is used.
	// This is primarily for testing.
	PruneRunner PruneRunner

	// CommandRunner is a custom function to run shell commands.
	// If nil, the default exec.Command is used.
	// This is primarily for testing.
	CommandRunner CommandRunner

	// VCDBTreeSplitter is a custom function to split .vcdbs into vcdbtree format.
	// If nil, the default vcdbtree.Split is used.
	// This is primarily for testing.
	VCDBTreeSplitter VCDBTreeSplitter

	// PruneRetention contains the retention options for restic forget --prune.
	// If set, runs `restic forget <options> --prune` after each backup.
	// Example: "--keep-daily 7 --keep-weekly 4 --keep-monthly 12"
	PruneRetention string

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
		m.StagingDir = "/backupcache/staging"
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

	err := m.performBackup(ctx, false) // Normal periodic backups respect player check

	if m.OnBackupComplete != nil {
		m.OnBackupComplete(err, time.Since(startTime))
	}
}

// performBackup executes the full backup workflow.
// skipPlayerCheck, if true, bypasses the player check and always runs the backup.
func (m *Manager) performBackup(ctx context.Context, skipPlayerCheck bool) error {
	// Step 0a: Check if server has booted (if BootChecker is configured)
	if m.BootChecker != nil && !m.BootChecker.HasBooted() {
		return ErrServerNotBooted
	}

	// Step 0b: Check if backup should run based on player status
	// ShouldBackup() returns true if players are online, OR if players
	// were online previously but have now all logged off (final backup).
	// Skip this check if skipPlayerCheck is true (e.g., for boot-time backups).
	if !skipPlayerCheck && m.PauseWhenNoPlayers && m.PlayerChecker != nil {
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

	// Step 5: Update persistent staging directory with changed files only
	if err := m.updateStagingDirectory(backupFile, saveFileName); err != nil {
		return fmt.Errorf("failed to update staging directory: %w", err)
	}

	// Step 6: Run restic backup on the staging directory
	if err := m.runRestic(ctx); err != nil {
		return fmt.Errorf("failed to run restic backup: %w", err)
	}

	// Step 7: Run restic forget --prune if retention is configured
	if err := m.runResticPrune(ctx); err != nil {
		return fmt.Errorf("failed to run restic prune: %w", err)
	}

	// Note: The staging directory is persistent and not cleaned up after backup.
	// This preserves file metadata for unchanged files, optimizing Restic efficiency.

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
// It first waits for the server to send the "[Server Notification] Backup complete!" message
// (if BackupCompletionWaiter is configured), then waits for the file to appear and be unlocked.
func (m *Manager) waitForBackupFile(ctx context.Context, afterTime time.Time) (string, error) {
	// First, wait for the server to signal that the backup is complete.
	// This ensures we don't try to access the file while the server is still writing to it.
	if m.BackupCompletionWaiter != nil {
		if err := m.BackupCompletionWaiter.WaitForBackupComplete(ctx); err != nil {
			return "", fmt.Errorf("failed waiting for backup completion: %w", err)
		}
	}

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

// updateStagingDirectory updates the persistent staging directory with changed files only.
// The savegame is converted to vcdbtree format (a directory tree optimized for deduplication).
// Files that haven't changed preserve their metadata (mtime), optimizing Restic efficiency.
func (m *Manager) updateStagingDirectory(backupFile, saveFileName string) error {
	// Ensure the staging directory exists
	if err := os.MkdirAll(m.StagingDir, 0755); err != nil {
		return fmt.Errorf("failed to create staging directory: %w", err)
	}

	// Sync directories: Logs, Playerdata, Mods
	// Only changed files are written, preserving metadata for unchanged files
	dirsToSync := []string{"Logs", "Playerdata", "Mods"}
	for _, dir := range dirsToSync {
		srcDir := filepath.Join(m.GameDataDir, dir)
		dstDir := filepath.Join(m.StagingDir, dir)

		if _, err := os.Stat(srcDir); err == nil {
			if _, _, _, err := vcdbtree.SyncDir(srcDir, dstDir); err != nil {
				return fmt.Errorf("failed to sync %s: %w", dir, err)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat %s: %w", dir, err)
		}
	}

	// Sync config files
	configFiles := []string{"serverconfig.json", "servermagicnumbers.json"}
	for _, file := range configFiles {
		srcFile := filepath.Join(m.GameDataDir, file)
		dstFile := filepath.Join(m.StagingDir, file)

		if _, _, err := vcdbtree.SyncFile(srcFile, dstFile); err != nil {
			return fmt.Errorf("failed to sync %s: %w", file, err)
		}
	}

	// Create the Saves directory for the vcdbtree output
	// The saveFileName (without .vcdbs extension) becomes the directory name
	saveBaseName := strings.TrimSuffix(saveFileName, ".vcdbs")
	savesDir := filepath.Join(m.StagingDir, "Saves", saveBaseName)
	if err := os.MkdirAll(savesDir, 0755); err != nil {
		return fmt.Errorf("failed to create Saves directory: %w", err)
	}

	// Split the backup file into vcdbtree format with caching.
	// Only writes files that have changed, preserving metadata for unchanged files.
	// This optimizes Restic's deduplication - unchanged files show zero diff.
	written, skipped, err := m.splitToVCDBTree(backupFile, savesDir)
	if err != nil {
		return fmt.Errorf("failed to split backup to vcdbtree: %w", err)
	}
	fmt.Printf("vcdbtree: %d files written, %d files unchanged\n", written, skipped)

	// Remove the original backup file since we've processed it
	if err := os.Remove(backupFile); err != nil {
		return fmt.Errorf("failed to remove original backup file: %w", err)
	}

	return nil
}

// splitToVCDBTree converts a .vcdbs SQLite database into vcdbtree format with caching.
// Only writes files that have changed, preserving metadata for unchanged files.
// Returns the number of files written (changed) and skipped (unchanged).
func (m *Manager) splitToVCDBTree(srcPath, dstDir string) (written, skipped int, err error) {
	// Use custom splitter if provided (for testing)
	if m.VCDBTreeSplitter != nil {
		fmt.Printf("Splitting vcdbs to vcdbtree (cached): %s -> %s\n", srcPath, dstDir)
		return m.VCDBTreeSplitter(srcPath, dstDir)
	}

	fmt.Printf("Splitting vcdbs to vcdbtree (cached): %s -> %s\n", srcPath, dstDir)

	return vcdbtree.SplitWithCache(srcPath, dstDir)
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

// runResticPrune runs restic forget with the configured retention options and --prune.
// This removes old snapshots according to the retention policy.
func (m *Manager) runResticPrune(ctx context.Context) error {
	if m.PruneRetention == "" {
		return nil // No pruning configured
	}

	// Use custom runner if provided (for testing)
	if m.PruneRunner != nil {
		return m.PruneRunner(ctx, m.PruneRetention)
	}

	fmt.Printf("Running restic forget with retention: %s\n", m.PruneRetention)

	// Parse the retention options string into arguments
	// Split on whitespace to get individual arguments
	args := strings.Fields(m.PruneRetention)
	// Always add --prune at the end
	args = append(args, "--prune")

	// Build the command: restic forget <options> --prune
	cmd := exec.CommandContext(ctx, "restic", append([]string{"forget"}, args...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("restic forget --prune failed: %w", err)
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
// skipPlayerCheck, if true, bypasses the player check and always runs the backup.
// This is useful for boot-time backups that should run regardless of player status.
func (m *Manager) RunBackupNow(ctx context.Context, skipPlayerCheck bool) error {
	return m.performBackup(ctx, skipPlayerCheck)
}

// Ensure Server implements ServerCommander at compile time.
var _ ServerCommander = (*server.Server)(nil)

// Ensure Server implements BootChecker at compile time.
var _ BootChecker = (*server.Server)(nil)

// Ensure Server implements BackupCompletionWaiter at compile time.
var _ BackupCompletionWaiter = (*server.Server)(nil)
