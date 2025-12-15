package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/renorris/vintagestory-restic/internal/backup"
	"github.com/renorris/vintagestory-restic/internal/downloader"
	"github.com/renorris/vintagestory-restic/internal/server"
)

const (
	serverBinariesDir = "/serverbinaries"
	// gracefulShutdownTimeout is how long to wait for the server to stop
	// after the first interrupt signal before force killing it.
	gracefulShutdownTimeout = 30 * time.Second
)

func main() {
	// Run the launcher
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Set up signal channel to receive SIGINT and SIGTERM
	// Use a buffered channel of size 2 to ensure we don't miss signals
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	// Create a context that we'll cancel on first signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a goroutine to cancel context on first signal
	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived %v, cancelling operations...\n", sig)
		cancel()
	}()

	// Load backup configuration
	backupConfig, err := backup.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load backup config: %w", err)
	}

	if !backupConfig.Enabled {
		fmt.Println("WARNING: BACKUP_INTERVAL not set. Periodic backups are disabled.")
	} else {
		fmt.Printf("Backups enabled with interval: %v\n", backupConfig.Interval)
		if backupConfig.BackupOnServerStart {
			fmt.Println("Backup on server start is enabled.")
		}
		if backupConfig.PauseWhenNoPlayers {
			fmt.Println("Backups will pause when no players are online.")
		}

		// Validate that required restic environment variables are set
		if err := backup.ValidateResticEnv(); err != nil {
			return err
		}
	}

	// Stage 1: Download server binaries if needed
	if err := downloader.DoServerBinaryDownload(ctx, serverBinariesDir); err != nil {
		if ctx.Err() != nil {
			// Context was cancelled, exit cleanly
			return nil
		}
		return fmt.Errorf("failed to download server binaries: %w", err)
	}

	// Stage 2: Create player checker if needed (before server so we can wire up OnOutput)
	var playerChecker *backup.PlayerChecker
	if backupConfig.Enabled && backupConfig.PauseWhenNoPlayers {
		playerChecker = &backup.PlayerChecker{}
	}

	// Stage 3: Start the Vintage Story server
	srv := &server.Server{
		WorkingDir: serverBinariesDir,
		Args:       []string{"--dataPath", "/gamedata"},
		OnOutput: func(line string) bool {
			fmt.Println(line)
			// Forward output to player checker if enabled
			if playerChecker != nil {
				playerChecker.HandleOutput(line)
			}
			return true
		},
	}

	// Stage 4: Create the command queue for rate-limited command submission
	// This ensures a minimum 100ms delay between all commands sent to the server
	cmdQueue := &server.CommandQueue{
		Sender: srv,
		OnError: func(cmd string, err error) {
			if err != nil {
				fmt.Printf("Failed to send command %q: %v\n", cmd, err)
			}
		},
	}

	// Stage 5: Start backup manager if enabled (create before starting server so we can use OnBoot)
	var backupManager *backup.Manager
	if backupConfig.Enabled {
		backupManager = &backup.Manager{
			Interval:               backupConfig.Interval,
			GameDataDir:            "/gamedata",
			Server:                 cmdQueue, // Use the command queue for rate-limited commands
			BootChecker:            srv,
			BackupCompletionWaiter: srv, // Wait for "[Server Notification] Backup complete!" before vacuuming
			PlayerChecker:          playerChecker,
			PauseWhenNoPlayers:     backupConfig.PauseWhenNoPlayers,
			OnBackupStart: func() {
				fmt.Println("Starting backup...")
			},
			OnBackupComplete: func(err error, duration time.Duration) {
				if err != nil {
					if err == backup.ErrNoPlayersOnline {
						fmt.Printf("Backup skipped: %v\n", err)
					} else {
						fmt.Printf("Backup failed after %v: %v\n", duration, err)
					}
				} else {
					fmt.Printf("Backup completed successfully in %v\n", duration)
				}
			},
		}
	}

	// Set up OnBoot callback to always trigger backup-on-start
	srv.OnBoot = func() {
		// Always trigger backup-on-start when backups are enabled
		// This ensures a backup is performed as soon as the server boots,
		// even if there are no players online.
		if backupConfig.Enabled {
			fmt.Println("Triggering immediate backup on server boot...")
			go func() {
				// Skip player check for boot-time backup to ensure it always runs
				if err := backupManager.RunBackupNow(ctx, true); err != nil {
					fmt.Printf("Backup on server start failed: %v\n", err)
				}
			}()
		}
	}

	fmt.Println("Starting Vintage Story server...")
	if err := srv.Start(ctx); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	fmt.Printf("Server started with PID %d\n", srv.PID())

	// Start the command queue now that the server is running
	cmdQueue.Start()
	defer cmdQueue.Stop()

	// Start the backup manager after the server has started
	if backupManager != nil {
		if err := backupManager.Start(ctx); err != nil {
			fmt.Printf("WARNING: Failed to start backup manager: %v\n", err)
		} else {
			fmt.Println("Backup manager started.")
			defer backupManager.Stop()
		}
	}

	// Start goroutine to read commands from stdin and pipe them to the server
	go readStdinCommands(ctx, cmdQueue)

	// Wait for either the server to exit or context cancellation (from signal)
	select {
	case <-srv.Done():
		// Server exited on its own
		if err := srv.ExitError(); err != nil {
			return fmt.Errorf("server exited with error: %w", err)
		}
		fmt.Println("Server exited cleanly.")
		return nil

	case <-ctx.Done():
		// Context cancelled (signal received) - start graceful shutdown
		fmt.Println("Initiating graceful shutdown (30s timeout)...")

		// Wait for either:
		// 1. Server to exit gracefully
		// 2. 30 second timeout
		shutdownTimer := time.NewTimer(gracefulShutdownTimeout)
		defer shutdownTimer.Stop()

		select {
		case <-srv.Done():
			// Server stopped gracefully
			fmt.Println("Server shutdown complete.")
			return nil

		case <-shutdownTimer.C:
			// Timeout elapsed - force kill
			fmt.Println("Graceful shutdown timeout elapsed, force killing server...")
			srv.Kill()
			<-srv.Done() // Wait for process to actually terminate
			fmt.Println("Server killed.")
			return nil
		}
	}
}

// readStdinCommands reads commands from stdin and submits them to the command queue.
// This allows users to send commands directly to the Vintage Story server.
func readStdinCommands(ctx context.Context, cmdQueue *server.CommandQueue) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Note: scanner.Scan() blocks, but that's okay since we're in a goroutine.
		// When the context is cancelled and the process exits, this goroutine
		// will be terminated along with the process.
		if scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				cmdQueue.Submit(line)
			}
		} else {
			// EOF or error - stop reading
			if err := scanner.Err(); err != nil {
				fmt.Printf("Error reading stdin: %v\n", err)
			}
			return
		}
	}
}
