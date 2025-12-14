package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockCommandSender records commands and optionally returns errors.
type mockCommandSender struct {
	mu       sync.Mutex
	commands []commandRecord
	err      error
}

type commandRecord struct {
	cmd  string
	time time.Time
}

func (m *mockCommandSender) SendCommand(cmd string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = append(m.commands, commandRecord{cmd: cmd, time: time.Now()})
	return m.err
}

func (m *mockCommandSender) getCommands() []commandRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]commandRecord, len(m.commands))
	copy(result, m.commands)
	return result
}

func TestCommandQueue_BasicSubmission(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond, // Use short delay for tests
	}

	cq.Start()
	defer cq.Stop()

	cq.Submit("test command")

	// Wait for command to be processed
	time.Sleep(50 * time.Millisecond)

	commands := sender.getCommands()
	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}
	if commands[0].cmd != "test command" {
		t.Errorf("expected 'test command', got %q", commands[0].cmd)
	}
}

func TestCommandQueue_RateLimiting(t *testing.T) {
	sender := &mockCommandSender{}
	minDelay := 50 * time.Millisecond
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: minDelay,
	}

	cq.Start()
	defer cq.Stop()

	// Submit 3 commands rapidly
	cq.Submit("cmd1")
	cq.Submit("cmd2")
	cq.Submit("cmd3")

	// Wait for all commands to be processed
	time.Sleep(200 * time.Millisecond)

	commands := sender.getCommands()
	if len(commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(commands))
	}

	// Verify commands are in order
	if commands[0].cmd != "cmd1" || commands[1].cmd != "cmd2" || commands[2].cmd != "cmd3" {
		t.Errorf("commands out of order: %v", commands)
	}

	// Verify minimum delay between commands
	for i := 1; i < len(commands); i++ {
		delay := commands[i].time.Sub(commands[i-1].time)
		// Allow some tolerance for timing
		if delay < minDelay-5*time.Millisecond {
			t.Errorf("delay between command %d and %d was %v, expected at least %v",
				i-1, i, delay, minDelay)
		}
	}
}

func TestCommandQueue_NoDelayWhenIdle(t *testing.T) {
	sender := &mockCommandSender{}
	minDelay := 50 * time.Millisecond
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: minDelay,
	}

	cq.Start()
	defer cq.Stop()

	// Submit first command
	cq.Submit("cmd1")

	// Wait longer than minDelay for it to process
	time.Sleep(100 * time.Millisecond)

	// Now submit second command - should not be delayed
	start := time.Now()
	cq.Submit("cmd2")

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	commands := sender.getCommands()
	if len(commands) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(commands))
	}

	// Second command should be processed quickly since we waited
	elapsed := commands[1].time.Sub(start)
	if elapsed > minDelay {
		t.Errorf("second command took %v to process, expected less than %v since queue was idle",
			elapsed, minDelay)
	}
}

func TestCommandQueue_OnErrorCallback(t *testing.T) {
	expectedErr := errors.New("send failed")
	sender := &mockCommandSender{err: expectedErr}

	var errorCallbackCalled atomic.Bool
	var capturedCmd string
	var capturedErr error
	var mu sync.Mutex

	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond,
		OnError: func(cmd string, err error) {
			mu.Lock()
			capturedCmd = cmd
			capturedErr = err
			mu.Unlock()
			errorCallbackCalled.Store(true)
		},
	}

	cq.Start()
	defer cq.Stop()

	cq.Submit("failing command")

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	if !errorCallbackCalled.Load() {
		t.Error("expected OnError callback to be called")
	}

	mu.Lock()
	if capturedCmd != "failing command" {
		t.Errorf("expected command 'failing command', got %q", capturedCmd)
	}
	if capturedErr != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, capturedErr)
	}
	mu.Unlock()
}

func TestCommandQueue_StopDrainsQueue(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond,
	}

	cq.Start()

	// Submit several commands
	cq.Submit("cmd1")
	cq.Submit("cmd2")
	cq.Submit("cmd3")

	// Stop should wait for all commands to be processed
	cq.Stop()

	commands := sender.getCommands()
	if len(commands) != 3 {
		t.Errorf("expected 3 commands after Stop(), got %d", len(commands))
	}
}

func TestCommandQueue_SubmitBeforeStart(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond,
	}

	// Submit before Start should be ignored
	cq.Submit("ignored")

	cq.Start()
	defer cq.Stop()

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	commands := sender.getCommands()
	if len(commands) != 0 {
		t.Errorf("expected 0 commands, got %d", len(commands))
	}
}

func TestCommandQueue_DefaultMinDelay(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender: sender,
		// MinDelay not set, should default to DefaultMinCommandDelay
	}

	cq.Start()

	// Check that MinDelay was set to default
	if cq.MinDelay != DefaultMinCommandDelay {
		t.Errorf("expected MinDelay to default to %v, got %v",
			DefaultMinCommandDelay, cq.MinDelay)
	}

	cq.Stop()
}

func TestCommandQueue_SendCommandInterface(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond,
	}

	cq.Start()
	defer cq.Stop()

	// Use the SendCommand interface (for compatibility with ServerCommander)
	err := cq.SendCommand("test via interface")
	if err != nil {
		t.Errorf("SendCommand returned error: %v", err)
	}

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	commands := sender.getCommands()
	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}
	if commands[0].cmd != "test via interface" {
		t.Errorf("expected 'test via interface', got %q", commands[0].cmd)
	}
}

func TestCommandQueue_ConcurrentSubmissions(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 5 * time.Millisecond,
	}

	cq.Start()
	defer cq.Stop()

	// Submit commands from multiple goroutines
	var wg sync.WaitGroup
	numGoroutines := 10
	commandsPerGoroutine := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < commandsPerGoroutine; j++ {
				cq.Submit("cmd")
			}
		}(i)
	}

	wg.Wait()

	// Wait for all commands to be processed
	time.Sleep(time.Duration(numGoroutines*commandsPerGoroutine*10) * time.Millisecond)

	commands := sender.getCommands()
	expectedTotal := numGoroutines * commandsPerGoroutine
	if len(commands) != expectedTotal {
		t.Errorf("expected %d commands, got %d", expectedTotal, len(commands))
	}
}

func TestCommandQueue_DoubleStart(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond,
	}

	cq.Start()
	cq.Start() // Should be a no-op
	defer cq.Stop()

	cq.Submit("cmd")
	time.Sleep(50 * time.Millisecond)

	commands := sender.getCommands()
	if len(commands) != 1 {
		t.Errorf("expected 1 command, got %d", len(commands))
	}
}

func TestCommandQueue_DoubleStop(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond,
	}

	cq.Start()
	cq.Stop()
	cq.Stop() // Should be a no-op, not panic
}

func TestCommandQueue_StopBeforeStart(t *testing.T) {
	sender := &mockCommandSender{}
	cq := &CommandQueue{
		Sender:   sender,
		MinDelay: 10 * time.Millisecond,
	}

	// Stop before Start should be a no-op, not panic
	cq.Stop()
}
