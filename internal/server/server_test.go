package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestServer_Start tests that the server starts successfully.
func TestServer_Start(t *testing.T) {
	// Use a simple command that exits quickly
	s := &Server{
		ServerPath: "echo",
		Args:       []string{"hello"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the process to complete
	if err := s.Wait(); err != nil {
		t.Errorf("Wait returned unexpected error: %v", err)
	}
}

// TestServer_StartTwice tests that starting twice returns an error.
func TestServer_StartTwice(t *testing.T) {
	s := &Server{
		ServerPath: "sleep",
		Args:       []string{"300"},
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := s.Start(ctx); err != nil {
		t.Fatalf("First Start failed: %v", err)
	}

	if err := s.Start(ctx); err == nil {
		t.Error("Second Start should have returned an error")
	}

	// Cancel and wait for cleanup
	cancel()
	<-s.Done()
}

// TestServer_SendCommand tests sending commands via stdin.
func TestServer_SendCommand(t *testing.T) {
	// Create a script that reads stdin and echoes it
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "echo_stdin.sh")
	scriptContent := `#!/bin/sh
while read line; do
    echo "received: $line"
done
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	var receivedLines []string
	var mu sync.Mutex

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
		OnOutput: func(line string) bool {
			mu.Lock()
			receivedLines = append(receivedLines, line)
			mu.Unlock()
			return true
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the script time to start
	time.Sleep(100 * time.Millisecond)

	// Send a command
	if err := s.SendCommand("test message"); err != nil {
		t.Errorf("SendCommand failed: %v", err)
	}

	// Wait a bit for output
	time.Sleep(200 * time.Millisecond)

	// Check output
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, line := range receivedLines {
		if strings.Contains(line, "received: test message") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to receive 'test message', got: %v", receivedLines)
	}
}

// TestServer_SendCommand_NotRunning tests that SendCommand returns error when not running.
func TestServer_SendCommand_NotRunning(t *testing.T) {
	s := &Server{
		ServerPath: "echo",
	}

	if err := s.SendCommand("test"); err != ErrServerNotRunning {
		t.Errorf("Expected ErrServerNotRunning, got: %v", err)
	}
}

// TestServer_WaitForPattern tests waiting for a pattern in output.
func TestServer_WaitForPattern(t *testing.T) {
	// Create a script that outputs multiple lines
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "pattern_test.sh")
	scriptContent := `#!/bin/sh
echo "starting up..."
sleep 0.1
echo "loading modules..."
sleep 0.1
echo "SERVER READY"
sleep 0.1
echo "accepting connections"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the "SERVER READY" pattern
	line, err := s.WaitForPattern(ctx, "SERVER READY")
	if err != nil {
		t.Fatalf("WaitForPattern failed: %v", err)
	}

	if line != "SERVER READY" {
		t.Errorf("Expected 'SERVER READY', got: %q", line)
	}
}

// TestServer_WaitForPattern_Regex tests waiting for a regex pattern.
func TestServer_WaitForPattern_Regex(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "regex_test.sh")
	scriptContent := `#!/bin/sh
echo "Server started on port 12345"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for a pattern with regex
	line, err := s.WaitForPattern(ctx, `Server started on port \d+`)
	if err != nil {
		t.Fatalf("WaitForPattern failed: %v", err)
	}

	if !strings.Contains(line, "port 12345") {
		t.Errorf("Expected line with port number, got: %q", line)
	}
}

// TestServer_WaitForPattern_Timeout tests that WaitForPattern times out correctly.
func TestServer_WaitForPattern_Timeout(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "timeout_test.sh")
	scriptContent := `#!/bin/sh
echo "starting..."
sleep 300
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for a pattern that won't appear, with a short timeout
	shortCtx, shortCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer shortCancel()

	_, err := s.WaitForPattern(shortCtx, "NEVER_APPEARS")
	if err != ErrPatternTimeout {
		t.Errorf("Expected ErrPatternTimeout, got: %v", err)
	}
}

// TestServer_WaitForPattern_ServerExits tests behavior when server exits before pattern match.
func TestServer_WaitForPattern_ServerExits(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "exit_test.sh")
	scriptContent := `#!/bin/sh
echo "starting..."
echo "done"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for a pattern that won't appear (server exits first)
	_, err := s.WaitForPattern(ctx, "NEVER_APPEARS")
	if err != ErrServerExited {
		t.Errorf("Expected ErrServerExited, got: %v", err)
	}
}

// TestServer_WaitForRegex tests the WaitForRegex method directly.
func TestServer_WaitForRegex(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "wait_regex_test.sh")
	scriptContent := `#!/bin/sh
echo "Log: Application version 2.5.3"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	re := regexp.MustCompile(`version \d+\.\d+\.\d+`)
	line, err := s.WaitForRegex(ctx, re)
	if err != nil {
		t.Fatalf("WaitForRegex failed: %v", err)
	}

	if !strings.Contains(line, "2.5.3") {
		t.Errorf("Expected line with version, got: %q", line)
	}
}

// TestServer_WaitForPattern_InvalidRegex tests that invalid regex returns error.
func TestServer_WaitForPattern_InvalidRegex(t *testing.T) {
	s := &Server{
		ServerPath: "echo",
		Args:       []string{"hello"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Invalid regex
	_, err := s.WaitForPattern(ctx, "[invalid")
	if err == nil {
		t.Error("Expected error for invalid regex")
	}
}

// TestServer_ContextCancellation tests that context cancellation stops the server.
func TestServer_ContextCancellation(t *testing.T) {
	// Use sleep which responds immediately to SIGINT
	s := &Server{
		ServerPath: "sleep",
		Args:       []string{"300"},
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the process a moment to start
	time.Sleep(50 * time.Millisecond)

	// Verify server is running
	if !s.Running() {
		t.Error("Expected server to be running")
	}

	// Cancel context
	cancel()

	// Wait for server to stop - should be quick since sleep responds to signals
	select {
	case <-s.Done():
		// Good
	case <-time.After(5 * time.Second):
		t.Error("Server did not stop after context cancellation")
	}

	if s.Running() {
		t.Error("Expected server to not be running after cancellation")
	}
}

// TestServer_Done tests the Done channel.
func TestServer_Done(t *testing.T) {
	s := &Server{
		ServerPath: "echo",
		Args:       []string{"quick"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Done on unstarted server should return closed channel
	select {
	case <-s.Done():
		// Expected - channel should be closed
	default:
		t.Error("Expected Done to return closed channel for unstarted server")
	}

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	select {
	case <-s.Done():
		// Good, process exited
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for Done")
	}
}

// TestServer_Running tests the Running method.
func TestServer_Running(t *testing.T) {
	s := &Server{
		ServerPath: "sleep",
		Args:       []string{"300"},
	}

	// Not running before start
	if s.Running() {
		t.Error("Expected Running to be false before Start")
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Running after start
	if !s.Running() {
		t.Error("Expected Running to be true after Start")
	}

	// Cancel and wait
	cancel()
	<-s.Done()

	// Not running after done
	if s.Running() {
		t.Error("Expected Running to be false after Done")
	}
}

// TestServer_PID tests the PID method.
func TestServer_PID(t *testing.T) {
	s := &Server{
		ServerPath: "sleep",
		Args:       []string{"300"},
	}

	// PID is 0 before start
	if s.PID() != 0 {
		t.Errorf("Expected PID 0 before start, got %d", s.PID())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// PID is non-zero after start
	pid := s.PID()
	if pid == 0 {
		t.Error("Expected non-zero PID after start")
	}

	// Verify it's a real process
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Errorf("FindProcess failed: %v", err)
	}
	if process == nil {
		t.Error("Process not found")
	}
}

// TestServer_OnOutput tests the OnOutput callback.
func TestServer_OnOutput(t *testing.T) {
	var lines []string
	var mu sync.Mutex

	s := &Server{
		ServerPath: "echo",
		Args:       []string{"-e", "line1\nline2\nline3"},
		OnOutput: func(line string) bool {
			mu.Lock()
			lines = append(lines, line)
			mu.Unlock()
			return true
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for process to complete
	<-s.Done()

	mu.Lock()
	defer mu.Unlock()

	if len(lines) == 0 {
		t.Error("Expected to receive output lines")
	}
}

// TestServer_Wait tests the Wait method.
func TestServer_Wait(t *testing.T) {
	s := &Server{
		ServerPath: "echo",
		Args:       []string{"hello"},
	}

	// Wait before start should return error
	if err := s.Wait(); err != ErrServerNotRunning {
		t.Errorf("Expected ErrServerNotRunning before start, got: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait should block until process exits
	if err := s.Wait(); err != nil {
		t.Errorf("Wait returned unexpected error: %v", err)
	}
}

// TestServer_Wait_NonZeroExit tests Wait with a non-zero exit code.
func TestServer_Wait_NonZeroExit(t *testing.T) {
	s := &Server{
		ServerPath: "sh",
		Args:       []string{"-c", "exit 42"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err := s.Wait()
	if err == nil {
		t.Error("Expected error for non-zero exit")
	}

	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Errorf("Expected *exec.ExitError, got %T: %v", err, err)
	} else if exitErr.ExitCode() != 42 {
		t.Errorf("Expected exit code 42, got %d", exitErr.ExitCode())
	}
}

// TestServer_ExitError tests the ExitError method.
func TestServer_ExitError(t *testing.T) {
	s := &Server{
		ServerPath: "sh",
		Args:       []string{"-c", "exit 1"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// ExitError should be nil while running
	if err := s.ExitError(); err != nil {
		t.Errorf("Expected nil ExitError while running, got: %v", err)
	}

	<-s.Done()

	// ExitError should be set after exit
	if err := s.ExitError(); err == nil {
		t.Error("Expected non-nil ExitError after non-zero exit")
	}
}

// TestServer_Stderr tests that stderr is captured.
func TestServer_Stderr(t *testing.T) {
	var lines []string
	var mu sync.Mutex

	s := &Server{
		ServerPath: "sh",
		Args:       []string{"-c", "echo 'to stderr' >&2"},
		OnOutput: func(line string) bool {
			mu.Lock()
			lines = append(lines, line)
			mu.Unlock()
			return true
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-s.Done()

	mu.Lock()
	defer mu.Unlock()

	found := false
	for _, line := range lines {
		if strings.Contains(line, "to stderr") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to capture stderr output")
	}
}

// TestServer_WorkingDir tests the WorkingDir option.
func TestServer_WorkingDir(t *testing.T) {
	tmpDir := t.TempDir()

	var output string
	var mu sync.Mutex

	s := &Server{
		ServerPath: "pwd",
		WorkingDir: tmpDir,
		OnOutput: func(line string) bool {
			mu.Lock()
			output = line
			mu.Unlock()
			return true
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-s.Done()

	mu.Lock()
	defer mu.Unlock()

	// The output should be the temp directory (resolving symlinks)
	expectedDir, _ := filepath.EvalSymlinks(tmpDir)
	actualDir, _ := filepath.EvalSymlinks(output)
	if actualDir != expectedDir {
		t.Errorf("Expected working dir %q, got %q", expectedDir, actualDir)
	}
}

// TestServer_Env tests the Env option.
func TestServer_Env(t *testing.T) {
	var output string
	var mu sync.Mutex

	s := &Server{
		ServerPath: "sh",
		Args:       []string{"-c", "echo $TEST_VAR"},
		Env:        []string{"TEST_VAR=hello_world"},
		OnOutput: func(line string) bool {
			mu.Lock()
			output = line
			mu.Unlock()
			return true
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-s.Done()

	mu.Lock()
	defer mu.Unlock()

	if output != "hello_world" {
		t.Errorf("Expected 'hello_world', got %q", output)
	}
}

// TestServer_MultiplePatternWaiters tests multiple goroutines waiting for patterns.
func TestServer_MultiplePatternWaiters(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "multi_pattern.sh")
	scriptContent := `#!/bin/sh
echo "EVENT_A"
sleep 0.1
echo "EVENT_B"
sleep 0.1
echo "EVENT_C"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	var wg sync.WaitGroup
	results := make(map[string]string)
	var mu sync.Mutex

	patterns := []string{"EVENT_A", "EVENT_B", "EVENT_C"}
	for _, p := range patterns {
		wg.Add(1)
		go func(pattern string) {
			defer wg.Done()
			line, err := s.WaitForPattern(ctx, pattern)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[pattern] = "error: " + err.Error()
			} else {
				results[pattern] = line
			}
		}(p)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	for _, p := range patterns {
		if results[p] != p {
			t.Errorf("Pattern %q: expected %q, got %q", p, p, results[p])
		}
	}
}

// TestServer_Start_InvalidPath tests starting with an invalid server path.
func TestServer_Start_InvalidPath(t *testing.T) {
	s := &Server{
		ServerPath: "/nonexistent/path/to/server",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Start(ctx)
	if err == nil {
		t.Error("Expected error for invalid server path")
	}
}

// TestServer_SendCommand_AfterExit tests SendCommand after server exits.
func TestServer_SendCommand_AfterExit(t *testing.T) {
	s := &Server{
		ServerPath: "echo",
		Args:       []string{"quick exit"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-s.Done()

	// Try to send command after exit
	err := s.SendCommand("test")
	if err != ErrServerNotRunning {
		t.Errorf("Expected ErrServerNotRunning, got: %v", err)
	}
}

// TestServer_WaitForPattern_NotRunning tests WaitForPattern when not running.
func TestServer_WaitForPattern_NotRunning(t *testing.T) {
	s := &Server{
		ServerPath: "echo",
		Args:       []string{"quick"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for exit
	<-s.Done()

	// Try to wait for pattern after exit
	_, err := s.WaitForPattern(ctx, "anything")
	if err != ErrServerNotRunning {
		t.Errorf("Expected ErrServerNotRunning, got: %v", err)
	}
}

// TestServer_Stop tests the Stop method gracefully stops the server.
func TestServer_Stop(t *testing.T) {
	s := &Server{
		ServerPath: "sleep",
		Args:       []string{"300"},
	}

	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the process a moment to start
	time.Sleep(50 * time.Millisecond)

	// Verify server is running
	if !s.Running() {
		t.Error("Expected server to be running")
	}

	// Stop the server
	s.Stop()

	// Wait for server to stop (sleep responds to SIGINT quickly)
	select {
	case <-s.Done():
		// Good
	case <-time.After(5 * time.Second):
		t.Error("Server did not stop after Stop()")
	}

	if s.Running() {
		t.Error("Expected server to not be running after Stop()")
	}
}

// TestServer_Kill tests the Kill method forcefully terminates the server.
func TestServer_Kill(t *testing.T) {
	// Create a script that ignores SIGINT to test that Kill works
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "ignore_sigint.sh")
	scriptContent := `#!/bin/sh
trap '' INT  # Ignore SIGINT
sleep 300
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the process a moment to start
	time.Sleep(50 * time.Millisecond)

	// Verify server is running
	if !s.Running() {
		t.Error("Expected server to be running")
	}

	// Kill the server (should work even though SIGINT is ignored)
	s.Kill()

	// Wait for server to stop
	select {
	case <-s.Done():
		// Good
	case <-time.After(5 * time.Second):
		t.Error("Server did not stop after Kill()")
	}

	if s.Running() {
		t.Error("Expected server to not be running after Kill()")
	}
}

// TestServer_StopThenKill tests Stop followed by Kill for stubborn processes.
func TestServer_StopThenKill(t *testing.T) {
	// Create a script that ignores SIGINT
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "ignore_sigint.sh")
	scriptContent := `#!/bin/sh
trap '' INT  # Ignore SIGINT
sleep 300
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the process a moment to start
	time.Sleep(50 * time.Millisecond)

	// Try Stop first (won't work because SIGINT is ignored)
	s.Stop()

	// Process should still be running after a brief wait
	time.Sleep(100 * time.Millisecond)
	if !s.Running() {
		t.Skip("Process stopped on SIGINT, can't test escalation to Kill")
	}

	// Now use Kill
	s.Kill()

	// Wait for server to stop
	select {
	case <-s.Done():
		// Good
	case <-time.After(5 * time.Second):
		t.Error("Server did not stop after Kill()")
	}
}

// TestServer_Stop_NotStarted tests that Stop on an unstarted server doesn't panic.
func TestServer_Stop_NotStarted(t *testing.T) {
	s := &Server{
		ServerPath: "sleep",
		Args:       []string{"1"},
	}

	// Should not panic
	s.Stop()
}

// TestServer_Kill_NotStarted tests that Kill on an unstarted server doesn't panic.
func TestServer_Kill_NotStarted(t *testing.T) {
	s := &Server{
		ServerPath: "sleep",
		Args:       []string{"1"},
	}

	// Should not panic
	s.Kill()
}

// TestServer_HasBooted tests the HasBooted method and boot detection.
func TestServer_HasBooted(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "boot_test.sh")
	scriptContent := `#!/bin/sh
echo "starting up..."
sleep 0.1
echo "14.12.2025 19:56:10 [Server Event] Dedicated Server now running"
sleep 0.1
echo "accepting connections"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	// Should be false before start
	if s.HasBooted() {
		t.Error("Expected HasBooted() to be false before start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the boot pattern to be detected
	_, err := s.WaitForPattern(ctx, "Dedicated Server now running")
	if err != nil {
		t.Fatalf("WaitForPattern failed: %v", err)
	}

	// Should be true after boot pattern detected
	if !s.HasBooted() {
		t.Error("Expected HasBooted() to be true after boot pattern detected")
	}
}

// TestServer_HasBooted_NotDetected tests HasBooted when boot pattern is not present.
func TestServer_HasBooted_NotDetected(t *testing.T) {
	s := &Server{
		ServerPath: "echo",
		Args:       []string{"just some output without boot pattern"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-s.Done()

	// Should be false because boot pattern was never detected
	if s.HasBooted() {
		t.Error("Expected HasBooted() to be false when boot pattern not detected")
	}
}

// TestServer_OnBoot tests the OnBoot callback.
func TestServer_OnBoot(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "onboot_test.sh")
	scriptContent := `#!/bin/sh
echo "starting..."
echo "Dedicated Server now running"
echo "done"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	var onBootCalled bool
	var mu sync.Mutex

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
		OnBoot: func() {
			mu.Lock()
			onBootCalled = true
			mu.Unlock()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-s.Done()

	mu.Lock()
	defer mu.Unlock()

	if !onBootCalled {
		t.Error("Expected OnBoot callback to be called")
	}
}

// TestServer_OnBoot_CalledOnlyOnce tests that OnBoot is only called once even with multiple boot patterns.
func TestServer_OnBoot_CalledOnlyOnce(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "onboot_once_test.sh")
	scriptContent := `#!/bin/sh
echo "Dedicated Server now running"
echo "Dedicated Server now running"
echo "Dedicated Server now running"
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	var onBootCount int
	var mu sync.Mutex

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
		OnBoot: func() {
			mu.Lock()
			onBootCount++
			mu.Unlock()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-s.Done()

	mu.Lock()
	defer mu.Unlock()

	if onBootCount != 1 {
		t.Errorf("Expected OnBoot to be called exactly once, but was called %d times", onBootCount)
	}
}

// TestServer_HasBooted_CannotUnset tests that HasBooted cannot be unset once set.
func TestServer_HasBooted_CannotUnset(t *testing.T) {
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "boot_unset_test.sh")
	scriptContent := `#!/bin/sh
echo "Dedicated Server now running"
sleep 0.2
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("Failed to write script: %v", err)
	}

	s := &Server{
		ServerPath: "/bin/sh",
		Args:       []string{scriptPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for boot pattern
	_, err := s.WaitForPattern(ctx, "Dedicated Server now running")
	if err != nil {
		t.Fatalf("WaitForPattern failed: %v", err)
	}

	// Verify booted
	if !s.HasBooted() {
		t.Fatal("Expected HasBooted() to be true")
	}

	// Wait for process to exit
	<-s.Done()

	// Should still be true after exit
	if !s.HasBooted() {
		t.Error("Expected HasBooted() to remain true after server exit")
	}
}
