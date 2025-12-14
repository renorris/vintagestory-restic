package backup

import (
	"regexp"
	"strings"
	"sync"
)

// playerJoinPattern matches when a player joins the server.
// Format: [Server Event] playername joins.
// The playername can contain any characters including whitespace.
var playerJoinPattern = regexp.MustCompile(`\[Server Event\].*joins\.$`)

// playerLeavePattern matches when a player leaves the server.
// Format: [Server Event] playername left.
// The playername can contain any characters including whitespace.
var playerLeavePattern = regexp.MustCompile(`\[Server Event\].*left\.$`)

// serverEventMarker is the exact string we count to ensure only one instance exists.
const serverEventMarker = "[Server Event]"

// serverChatPrefix is the prefix for chat messages, which should be ignored
// to prevent players from injecting fake join/leave events via chat.
const serverChatPrefix = "[Server Chat]"

// PlayerChecker tracks the number of online players by watching server output
// for join/leave events. It maintains a counter that increments when players
// join and decrements when players leave.
//
// It also tracks whether players were online at the previous backup check,
// allowing a "final backup" to be triggered when all players log off.
type PlayerChecker struct {
	mu          sync.Mutex
	playerCount int

	// playersOnlineAtLastCheck tracks whether any players were online
	// when ShouldBackup() was last called. This is used to trigger
	// a final backup when all players log off.
	playersOnlineAtLastCheck bool
}

// HandleOutput should be called for each line of server output.
// It detects player join/leave events and updates the player count.
// For security, it only counts lines that contain exactly one [Server Event] marker
// and ignores chat messages.
func (p *PlayerChecker) HandleOutput(line string) {
	// Security check: ignore chat messages to prevent injection attacks.
	// Players could type "[Server Event] fakeplayer joins." in chat.
	if strings.Contains(line, serverChatPrefix) {
		return
	}

	// Security check: ensure exactly one [Server Event] marker exists.
	// This prevents attack vectors where someone could inject fake events
	// through messages containing multiple [Server Event] strings.
	if strings.Count(line, serverEventMarker) != 1 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if playerJoinPattern.MatchString(line) {
		p.playerCount++
		return
	}

	if playerLeavePattern.MatchString(line) {
		p.playerCount--
		// Ensure we don't go negative (shouldn't happen, but be safe)
		if p.playerCount < 0 {
			p.playerCount = 0
		}
	}
}

// PlayersOnline returns true if there are any players currently online.
func (p *PlayerChecker) PlayersOnline() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.playerCount > 0
}

// PlayerCount returns the current number of online players.
func (p *PlayerChecker) PlayerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.playerCount
}

// ShouldBackup checks if a backup should run based on player status.
// It returns true if:
//   - Players are currently online, OR
//   - Players were online at the last check but are now all offline
//     (triggers a "final backup" to save changes before players left)
//
// This method updates the internal state tracking, so it should only
// be called once per backup attempt.
func (p *PlayerChecker) ShouldBackup() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	playersOnlineNow := p.playerCount > 0
	werePlayersOnlineBefore := p.playersOnlineAtLastCheck

	// Update state for next check
	p.playersOnlineAtLastCheck = playersOnlineNow

	// Backup if players are online now
	if playersOnlineNow {
		return true
	}

	// Also backup if players WERE online before but are now gone
	// This ensures we capture any changes made before they logged off
	if werePlayersOnlineBefore && !playersOnlineNow {
		return true
	}

	return false
}
