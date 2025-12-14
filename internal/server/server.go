// Package server provides a wrapper for managing the Vintage Story server process.
// It handles stdin/stdout pipe management, signal propagation via contexts, and
// provides an intuitive API for sending commands and awaiting output patterns.
package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrServerNotRunning is returned when attempting operations on a server that isn't running.
var ErrServerNotRunning = errors.New("server is not running")

// ErrPatternTimeout is returned when WaitForPattern times out.
var ErrPatternTimeout = errors.New("timed out waiting for pattern")

// ErrServerExited is returned when the server exits unexpectedly while waiting for a pattern.
var ErrServerExited = errors.New("server exited unexpectedly")

// OutputHandler is a callback function for handling server output lines.
// Return false to unsubscribe from further output.
type OutputHandler func(line string) bool

// BootPattern is the pattern that indicates the server has fully booted.
const BootPattern = "Dedicated Server now running"

// Server wraps a Vintage Story server process and provides methods for
// interacting with its stdin/stdout streams.
type Server struct {
	// ServerPath is the path to the server executable.
	// If empty, defaults to using '/usr/bin/dotnet /serverbinaries/VintageStoryServer.dll'.
	// This allows tests to override the command while production uses dotnet.
	ServerPath string

	// WorkingDir is the working directory for the server process.
	// If empty, uses the directory containing the server executable.
	WorkingDir string

	// Args contains additional command-line arguments for the server.
	Args []string

	// Env contains additional environment variables for the server.
	// If nil, inherits the current process environment.
	Env []string

	// OnOutput is called for each line of output from the server.
	// This is useful for logging or monitoring. It runs in a separate goroutine.
	OnOutput OutputHandler

	// OnBoot is called exactly once when the server has fully booted.
	// This is triggered when the "Dedicated Server now running" pattern is detected.
	OnBoot func()

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
	done    chan struct{}
	err     error
	errLock sync.RWMutex

	outputMu       sync.RWMutex
	outputHandlers []OutputHandler

	started   bool
	mu        sync.Mutex
	hasBooted atomic.Bool
	bootOnce  sync.Once
}

// Start launches the server process and begins reading its output.
// The provided context controls the server lifecycle - when cancelled,
// the server will be gracefully terminated.
//
// Start returns immediately after the process is launched. Use WaitForPattern
// to wait for the server to be ready.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return errors.New("server already started")
	}

	// Create the command
	// If ServerPath is set, use it (for tests/backward compatibility)
	// Otherwise, use dotnet to run the Vintage Story server DLL
	if s.ServerPath != "" {
		s.cmd = exec.Command(s.ServerPath, s.Args...)
	} else {
		args := append([]string{"/serverbinaries/VintageStoryServer.dll"}, s.Args...)
		s.cmd = exec.Command("/usr/bin/dotnet", args...)
	}
	if s.WorkingDir != "" {
		s.cmd.Dir = s.WorkingDir
	}
	if s.Env != nil {
		s.cmd.Env = append(os.Environ(), s.Env...)
	}

	// Set up stdin pipe
	stdin, err := s.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	s.stdin = stdin

	// Set up stdout pipe
	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	s.stdout = stdout

	// Set up stderr pipe (merge with stdout for unified output handling)
	stderr, err := s.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}
	s.stderr = stderr

	// Initialize done channel
	s.done = make(chan struct{})

	// Start the process
	if err := s.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	s.started = true

	// Start goroutines for reading output
	go s.readOutput(s.stdout, "[stdout]")
	go s.readOutput(s.stderr, "[stderr]")

	// Start goroutine to wait for process exit
	go s.waitForExit()

	// Start goroutine to handle context cancellation
	go s.handleContextCancel(ctx)

	return nil
}

// readOutput reads lines from the given reader and dispatches them to handlers.
func (s *Server) readOutput(r io.Reader, prefix string) {
	scanner := bufio.NewScanner(r)
	// Increase buffer size for potentially long log lines
	const maxScanTokenSize = 1024 * 1024 // 1MB
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, maxScanTokenSize)

	for scanner.Scan() {
		line := scanner.Text()

		// Check for boot pattern and set hasBooted flag (only once)
		if strings.Contains(line, BootPattern) {
			s.bootOnce.Do(func() {
				s.hasBooted.Store(true)
				if s.OnBoot != nil {
					s.OnBoot()
				}
			})
		}

		// Call the main output handler if set
		if s.OnOutput != nil {
			s.OnOutput(line)
		}

		// Call registered handlers
		s.dispatchToHandlers(line)
	}
}

// dispatchToHandlers sends the line to all registered output handlers.
func (s *Server) dispatchToHandlers(line string) {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()

	// Filter handlers that return false (want to unsubscribe)
	stillActive := s.outputHandlers[:0]
	for _, handler := range s.outputHandlers {
		if handler(line) {
			stillActive = append(stillActive, handler)
		}
	}
	s.outputHandlers = stillActive
}

// addHandler registers an output handler that will receive all output lines.
func (s *Server) addHandler(handler OutputHandler) {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	s.outputHandlers = append(s.outputHandlers, handler)
}

// waitForExit waits for the process to exit and records any error.
func (s *Server) waitForExit() {
	err := s.cmd.Wait()
	s.errLock.Lock()
	s.err = err
	s.errLock.Unlock()
	close(s.done)
}

// handleContextCancel watches for context cancellation and gracefully stops the server.
func (s *Server) handleContextCancel(ctx context.Context) {
	select {
	case <-ctx.Done():
		// Context cancelled - attempt graceful shutdown via /stop command
		// The caller is responsible for managing timeouts and escalation to SIGKILL
		s.Stop()
	case <-s.done:
		// Process exited on its own
	}
}

// Stop attempts to gracefully stop the server by sending the /stop command
// followed by SIGINT. This does not wait for the server to exit - use Wait()
// or Done() for that. The caller is responsible for managing timeouts and
// escalating to Kill() if needed.
func (s *Server) Stop() {
	// Try sending /stop command first (Vintage Story server command)
	_ = s.SendCommand("/stop")

	// Also send SIGINT for processes that don't respond to /stop
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Signal(os.Interrupt)
	}
}

// Kill forcefully terminates the server process with SIGKILL.
// This should be used when graceful shutdown times out.
func (s *Server) Kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
	}
}

// SendCommand sends a command to the server's stdin pipe.
// The command is written followed by a newline, and the pipe is flushed.
// Returns ErrServerNotRunning if the server is not running.
func (s *Server) SendCommand(cmd string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started || s.stdin == nil {
		return ErrServerNotRunning
	}

	// Check if server has already exited
	select {
	case <-s.done:
		return ErrServerNotRunning
	default:
	}

	// Write the command with newline
	_, err := fmt.Fprintln(s.stdin, cmd)
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	return nil
}

// WaitForPattern waits until a line matching the given regex pattern appears in
// the server output, or until the context is cancelled/times out.
//
// The pattern is compiled as a regular expression. Use regexp.QuoteMeta for
// literal string matching.
//
// Returns the first matching line, or an error if the context expires or
// the server exits before a match is found.
func (s *Server) WaitForPattern(ctx context.Context, pattern string) (string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", fmt.Errorf("invalid pattern: %w", err)
	}

	return s.WaitForRegex(ctx, re)
}

// WaitForRegex waits until a line matching the given compiled regex appears in
// the server output, or until the context is cancelled/times out.
//
// Returns the first matching line, or an error if the context expires or
// the server exits before a match is found.
func (s *Server) WaitForRegex(ctx context.Context, re *regexp.Regexp) (string, error) {
	// Check if server is running
	select {
	case <-s.done:
		return "", ErrServerNotRunning
	default:
	}

	matchCh := make(chan string, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)

	// Register handler to watch for pattern
	s.addHandler(func(line string) bool {
		select {
		case <-doneCh:
			return false // Unsubscribe
		default:
		}

		if re.MatchString(line) {
			select {
			case matchCh <- line:
			default:
			}
			return false // Unsubscribe after match
		}
		return true // Keep listening
	})

	// Wait for match, context cancellation, or server exit
	select {
	case line := <-matchCh:
		return line, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return "", ErrPatternTimeout
		}
		return "", ctx.Err()
	case <-s.done:
		// Check if we got a match before the server exited
		select {
		case line := <-matchCh:
			return line, nil
		default:
			return "", ErrServerExited
		}
	}
}

// Wait blocks until the server process exits.
// Returns the exit error from the process, or nil if it exited cleanly.
func (s *Server) Wait() error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return ErrServerNotRunning
	}
	s.mu.Unlock()

	<-s.done

	s.errLock.RLock()
	defer s.errLock.RUnlock()
	return s.err
}

// Done returns a channel that is closed when the server exits.
func (s *Server) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done == nil {
		// Return a closed channel if not started
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return s.done
}

// Running returns true if the server is currently running.
func (s *Server) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started || s.done == nil {
		return false
	}

	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

// HasBooted returns true if the server has fully booted.
// This is determined by detecting the "Dedicated Server now running" pattern
// in the server output. Once set, this flag cannot be unset.
func (s *Server) HasBooted() bool {
	return s.hasBooted.Load()
}

// ExitError returns the error from the server process exit, if any.
// Returns nil if the server hasn't exited yet or exited cleanly.
func (s *Server) ExitError() error {
	s.errLock.RLock()
	defer s.errLock.RUnlock()
	return s.err
}

// PID returns the process ID of the running server, or 0 if not running.
func (s *Server) PID() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd != nil && s.cmd.Process != nil {
		return s.cmd.Process.Pid
	}
	return 0
}

// BackupCompletePattern is the exact suffix that indicates a backup has completed.
const BackupCompletePattern = "[Server Notification] Backup complete!"

// WaitForBackupComplete waits for the server to send the backup completion notification.
// It uses strings.HasSuffix to match lines ending with exactly "[Server Notification] Backup complete!".
// Returns nil on success, or an error if the context expires or the server exits.
func (s *Server) WaitForBackupComplete(ctx context.Context) error {
	// Check if server is running
	select {
	case <-s.Done():
		return ErrServerNotRunning
	default:
	}

	matchCh := make(chan struct{}, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)

	// Register handler to watch for backup complete pattern
	s.addHandler(func(line string) bool {
		select {
		case <-doneCh:
			return false // Unsubscribe
		default:
		}

		if strings.HasSuffix(line, BackupCompletePattern) {
			select {
			case matchCh <- struct{}{}:
			default:
			}
			return false // Unsubscribe after match
		}
		return true // Keep listening
	})

	// Wait for match, context cancellation, or server exit
	select {
	case <-matchCh:
		return nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrPatternTimeout
		}
		return ctx.Err()
	case <-s.Done():
		// Check if we got a match before the server exited
		select {
		case <-matchCh:
			return nil
		default:
			return ErrServerExited
		}
	}
}
