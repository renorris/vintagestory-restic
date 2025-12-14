package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	// Stage 1: Download server binaries if needed
	if err := downloader.DoServerBinaryDownload(ctx, serverBinariesDir); err != nil {
		if ctx.Err() != nil {
			// Context was cancelled, exit cleanly
			return nil
		}
		return fmt.Errorf("failed to download server binaries: %w", err)
	}

	// Stage 2: Start the Vintage Story server
	srv := &server.Server{
		WorkingDir: serverBinariesDir,
		Args:       []string{"--dataPath", "/gamedata"},
		OnOutput: func(line string) bool {
			fmt.Println(line)
			return true
		},
	}

	fmt.Println("Starting Vintage Story server...")
	if err := srv.Start(ctx); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	fmt.Printf("Server started with PID %d\n", srv.PID())

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
