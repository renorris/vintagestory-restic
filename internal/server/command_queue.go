package server

import (
	"sync"
	"time"
)

const (
	// DefaultMinCommandDelay is the minimum time between commands sent to the server.
	DefaultMinCommandDelay = 100 * time.Millisecond
)

// CommandSender is an interface for sending commands to the server.
// This is satisfied by *Server.
type CommandSender interface {
	SendCommand(cmd string) error
}

// CommandQueue provides rate-limited command submission to the server.
// It ensures a minimum delay between commands to prevent overwhelming the server.
// All commands are queued and processed in order with the configured delay.
type CommandQueue struct {
	// Sender is the underlying command sender (usually *Server).
	Sender CommandSender

	// MinDelay is the minimum time between commands.
	// Defaults to DefaultMinCommandDelay (100ms) if not set.
	MinDelay time.Duration

	// OnError is called when a command fails to send. Optional.
	// If nil, errors are silently dropped.
	OnError func(cmd string, err error)

	mu           sync.Mutex
	lastSentTime time.Time
	started      bool
	queue        chan string
	done         chan struct{}
	wg           sync.WaitGroup
}

// Start begins processing the command queue.
// Must be called before submitting commands.
func (cq *CommandQueue) Start() {
	cq.mu.Lock()
	defer cq.mu.Unlock()

	if cq.started {
		return
	}

	if cq.MinDelay <= 0 {
		cq.MinDelay = DefaultMinCommandDelay
	}

	// Buffer allows commands to be submitted without blocking
	cq.queue = make(chan string, 100)
	cq.done = make(chan struct{})
	cq.started = true

	cq.wg.Add(1)
	go cq.processLoop()
}

// Stop stops the command queue and waits for pending commands to be processed.
func (cq *CommandQueue) Stop() {
	cq.mu.Lock()
	if !cq.started {
		cq.mu.Unlock()
		return
	}
	// Mark as stopped to prevent double-close
	cq.started = false
	cq.mu.Unlock()

	close(cq.done)
	cq.wg.Wait()
}

// Submit adds a command to the queue for processing.
// Commands are processed in order with the configured minimum delay.
// Returns immediately without blocking (unless the queue buffer is full).
func (cq *CommandQueue) Submit(cmd string) {
	cq.mu.Lock()
	if !cq.started {
		cq.mu.Unlock()
		return
	}
	queue := cq.queue
	cq.mu.Unlock()

	select {
	case queue <- cmd:
	default:
		// Queue full, drop the command (shouldn't happen with reasonable usage)
		if cq.OnError != nil {
			cq.OnError(cmd, nil)
		}
	}
}

// processLoop is the main loop that processes commands from the queue.
func (cq *CommandQueue) processLoop() {
	defer cq.wg.Done()

	for {
		select {
		case <-cq.done:
			// Drain remaining commands before exiting
			cq.drainQueue()
			return
		case cmd := <-cq.queue:
			cq.sendWithDelay(cmd)
		}
	}
}

// drainQueue processes any remaining commands in the queue.
func (cq *CommandQueue) drainQueue() {
	for {
		select {
		case cmd := <-cq.queue:
			cq.sendWithDelay(cmd)
		default:
			return
		}
	}
}

// sendWithDelay sends a command after ensuring the minimum delay has elapsed.
func (cq *CommandQueue) sendWithDelay(cmd string) {
	cq.mu.Lock()
	lastSent := cq.lastSentTime
	minDelay := cq.MinDelay
	cq.mu.Unlock()

	// Calculate how long to wait
	elapsed := time.Since(lastSent)
	if elapsed < minDelay {
		time.Sleep(minDelay - elapsed)
	}

	// Send the command
	err := cq.Sender.SendCommand(cmd)

	// Update last sent time
	cq.mu.Lock()
	cq.lastSentTime = time.Now()
	cq.mu.Unlock()

	if err != nil && cq.OnError != nil {
		cq.OnError(cmd, err)
	}
}

// SendCommand implements the CommandSender interface, allowing CommandQueue
// to be used as a drop-in replacement for Server in code that sends commands.
// This method submits the command to the queue and returns immediately.
// Note: Unlike Server.SendCommand, this always returns nil since errors
// are handled asynchronously via the OnError callback.
func (cq *CommandQueue) SendCommand(cmd string) error {
	cq.Submit(cmd)
	return nil
}

// Ensure CommandQueue implements CommandSender at compile time.
var _ CommandSender = (*CommandQueue)(nil)
